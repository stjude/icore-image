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
    def test_basic_invocation(self, mock_popen_cls):
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

        # Verify subprocess was called
        assert mock_popen_cls.called
        cmd = mock_popen_cls.call_args[0][0]
        assert cmd[0] == "/usr/bin/dicom-deid-rs"
        assert cmd[1] == "/tmp/in"
        assert cmd[2] == "/tmp/out"

    @patch("deid_rs.subprocess.Popen")
    def test_variables_passed(self, mock_popen_cls):
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            anonymizer_script='<script><p t="DATEINC">-100</p><e en="T" t="00080020" n="StudyDate">@incrementdate(this,@DATEINC)</e></script>',
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        cmd = mock_popen_cls.call_args[0][0]
        # Should have --var DATEINC -100 in the command
        assert "--var" in cmd
        var_idx = cmd.index("--var")
        assert cmd[var_idx + 1] == "DATEINC"
        assert cmd[var_idx + 2] == "-100"

    @patch("deid_rs.subprocess.Popen")
    def test_error_handling(self, mock_popen_cls):
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
    def test_lookup_table_passed(self, mock_popen_cls):
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

        cmd = mock_popen_cls.call_args[0][0]
        assert "--lookup-table" in cmd
