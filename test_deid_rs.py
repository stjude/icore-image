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

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_remove_unspecified_elements_passed(self, mock_run, mock_popen_cls):
        stderr_with_unspecified = TRANSLATE_STDERR.replace(
            "remove_unspecified_elements: false",
            "remove_unspecified_elements: true",
        )
        mock_run.return_value = _mock_translate_run(stderr=stderr_with_unspecified)
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert "--remove-unspecified-elements" in pipeline_cmd

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_remove_unspecified_not_passed_when_false(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert "--remove-unspecified-elements" not in pipeline_cmd

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_quarantine_dir_passed(self, mock_run, mock_popen_cls, tmp_path):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        qdir = tmp_path / "quarantine"
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            quarantine_dir=str(qdir),
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert "--quarantine-dir" in pipeline_cmd
        idx = pipeline_cmd.index("--quarantine-dir")
        assert pipeline_cmd[idx + 1] == str(qdir)
        assert qdir.is_dir(), "quarantine_dir should be created before invocation"

    @patch("deid_rs.subprocess.Popen")
    @patch("deid_rs.subprocess.run")
    def test_quarantine_dir_not_passed_when_none(self, mock_run, mock_popen_cls):
        mock_run.return_value = _mock_translate_run(stderr=TRANSLATE_STDERR)
        mock_popen_cls.return_value = _mock_popen(
            stdout="  Files processed:  1\n  Files blacklisted: 0\n  Files skipped:    0\n",
        )
        pipeline = DeidRsPipeline(
            input_dir="/tmp/in",
            output_dir="/tmp/out",
            binary_path="/usr/bin/dicom-deid-rs",
        )
        pipeline.run()

        pipeline_cmd = mock_popen_cls.call_args[0][0]
        assert "--quarantine-dir" not in pipeline_cmd


class TestDeidRsQuarantineE2E:
    """End-to-end: real binary, real inputs, real quarantine directory.

    Skipped when the release binary isn't built. Build with
    ``cd dicom-deid-rs && cargo build --release`` before running.
    """

    def _binary_path(self) -> str:
        from pathlib import Path

        return str(
            Path(__file__).parent
            / "dicom-deid-rs"
            / "target"
            / "release"
            / "dicom-deid-rs"
        )

    def test_blacklisted_files_land_in_quarantine_dir(self, tmp_path):
        if not os.path.exists(self._binary_path()):
            pytest.skip("dicom-deid-rs release binary not built")

        from pydicom.dataset import FileDataset, FileMetaDataset

        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        quarantine_dir = tmp_path / "quarantine"
        input_dir.mkdir()

        # Write a minimal CT DICOM so the blacklist matches.
        meta = FileMetaDataset()
        meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
        meta.MediaStorageSOPInstanceUID = "1.2.3"
        meta.TransferSyntaxUID = "1.2.840.10008.1.2.1"
        ds = FileDataset(
            str(input_dir / "test.dcm"), {}, file_meta=meta, preamble=b"\0" * 128
        )
        ds.PatientName = "Test^Patient"
        ds.PatientID = "PID123"
        ds.Modality = "CT"
        ds.StudyDate = "20250101"
        ds.SeriesNumber = "1"
        ds.SOPInstanceUID = "1.2.3"
        ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
        ds.StudyInstanceUID = "1.2"
        ds.SeriesInstanceUID = "1.2.9"
        ds.save_as(str(input_dir / "test.dcm"), write_like_original=False)

        # Recipe with a blacklist that rejects CT files.
        recipe_path = tmp_path / "recipe.txt"
        recipe_path.write_text(
            "FORMAT dicom\n%filter blacklist\nLABEL reject_ct\n  equals Modality CT\n"
        )

        pipeline = DeidRsPipeline(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            quarantine_dir=str(quarantine_dir),
            binary_path=self._binary_path(),
        )
        # Bypass translate-ctp by providing the recipe directly — we still
        # need a minimal anonymizer for the translator step, so use `run()`
        # after mocking the translator to return our prewritten recipe.
        import subprocess

        real_run = subprocess.run

        def fake_translate(cmd, *args, **kwargs):
            if len(cmd) >= 2 and cmd[1] == "translate-ctp":
                # Copy our prewritten recipe to the requested output path.
                if "-o" in cmd:
                    out_idx = cmd.index("-o")
                    out_path = cmd[out_idx + 1]
                    import shutil

                    shutil.copy(recipe_path, out_path)
                result = MagicMock()
                result.returncode = 0
                result.stdout = ""
                result.stderr = (
                    "Recipe written\n\nConfig:\n"
                    "  remove_private_tags: true\n"
                    "  remove_unspecified_elements: false\n"
                )
                return result
            return real_run(cmd, *args, **kwargs)

        with patch("deid_rs.subprocess.run", side_effect=fake_translate):
            result = pipeline.run()

        assert result["num_images_quarantined"] >= 1
        quarantined = list(quarantine_dir.rglob("*.dcm"))
        assert len(quarantined) == 1, (
            f"expected one quarantined .dcm, got {quarantined}"
        )
        assert (quarantine_dir / "test.dcm").exists()
        assert (quarantine_dir / "blacklisted_files.txt").exists()
        # Sanity: the output directory does NOT contain the blacklist report.
        assert not (output_dir / "blacklisted_files.txt").exists()
