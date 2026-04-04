"""Tests for deid_rs.py -- Rust engine wrapper."""

import os

import pytest
from unittest.mock import patch, MagicMock

from deid_rs import _parse_report, DeidRsPipeline


def _make_readable_pipe(content: str):
    """Create a real pipe with content, returning the read-end as a file object."""
    r, w = os.pipe()
    os.write(w, content.encode())
    os.close(w)
    return os.fdopen(r, "r")


def _mock_popen(returncode=0, stdout="", stderr=""):
    """Create a mock Popen object with real file descriptor stdout/stderr."""
    mock_proc = MagicMock()
    mock_proc.stdout = _make_readable_pipe(stdout)
    mock_proc.stderr = _make_readable_pipe(stderr)
    mock_proc.returncode = returncode
    mock_proc.wait.return_value = returncode
    return mock_proc


def _mock_translate_run(returncode=0, stderr=""):
    """Create a mock subprocess.run result for translate-ctp."""
    mock_result = MagicMock()
    mock_result.returncode = returncode
    mock_result.stdout = ""
    mock_result.stderr = stderr
    return mock_result


# Default translate-ctp stderr output (variables + config)
TRANSLATE_STDERR = """\
Recipe written to /tmp/recipe.txt

Variables:
  DATEINC = -100

Config:
  remove_private_tags: true
  remove_unspecified_elements: false
"""


class TestParseReport:
    def test_basic_output(self):
        stdout = """De-identification complete:
  Files processed:  42
  Files blacklisted: 3
  Files skipped:    1"""
        report = _parse_report(stdout)
        assert report["files_processed"] == 42
        assert report["files_blacklisted"] == 3
        assert report["files_skipped"] == 1

    def test_empty_output(self):
        report = _parse_report("")
        assert report == {}

    def test_partial_output(self):
        stdout = "  Files processed:  10"
        report = _parse_report(stdout)
        assert report["files_processed"] == 10
        assert "files_blacklisted" not in report


class TestDeidRsPipeline:
    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_basic_invocation(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            stdout="De-identification complete:\n  Files processed:  5\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        result = pipeline.run()
        assert result["num_images_saved"] == 5
        assert result["num_images_quarantined"] == 0

        # Verify translate-ctp was called first
        assert mock_run.called
        translate_cmd = mock_run.call_args[0][0]
        assert translate_cmd[1] == "translate-ctp"

        # Verify pipeline was called
        assert mock_popen_cls.called
        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert pipeline_cmd[0] == "/usr/bin/dicom-deid-rs"
        assert pipeline_cmd[1] == "/tmp/in"
        assert pipeline_cmd[2] == "/tmp/out"

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_variables_passed(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            anonymizer_script='<script><p t="DATEINC">-100</p></script>',
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert "--var" in pipeline_cmd
        var_idx = pipeline_cmd.index("--var")
        assert pipeline_cmd[var_idx + 1] == "DATEINC"
        assert pipeline_cmd[var_idx + 2] == "-100"

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_error_handling(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            returncode=1,
            stderr="Error: recipe parse failed\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        with pytest.raises(RuntimeError, match="exited with code 1"):
            pipeline.run()

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_translate_error(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(
            returncode=1, stderr="Error: XML parse error"
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        with pytest.raises(RuntimeError, match="translate-ctp failed"):
            pipeline.run()

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_lookup_table_passed(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            lookup_table="PatientID/12345 = ANON001",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert "--lookup-table" in pipeline_cmd
