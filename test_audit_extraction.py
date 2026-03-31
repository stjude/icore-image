"""Tests for audit_extraction.py."""

import os
import tempfile

import pandas as pd
import pydicom
import pydicom.uid
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian

from audit_extraction import (
    AUDIT_TAGS,
    build_linker_table,
    extract_audit_log,
)


def _create_test_dicom(filepath: str, **tag_values) -> None:
    """Create a minimal valid DICOM file with given tag values."""
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = pydicom.uid.UID("1.2.840.10008.5.1.4.1.1.2")
    file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset(filepath, {}, file_meta=file_meta, preamble=b"\x00" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    for tag_name, value in tag_values.items():
        setattr(ds, tag_name, value)

    ds.save_as(filepath)


class TestExtractAuditLog:
    def test_extract_from_single_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_test_dicom(
                os.path.join(tmpdir, "test.dcm"),
                PatientID="PAT001",
                PatientName="John^Doe",
                StudyInstanceUID="1.2.3.4.5",
                Modality="CT",
                Manufacturer="GE MEDICAL",
            )
            df = extract_audit_log(tmpdir)
            assert len(df) == 1
            assert df.iloc[0]["PatientID"] == "PAT001"
            assert df.iloc[0]["PatientName"] == "John^Doe"
            assert df.iloc[0]["Modality"] == "CT"

    def test_extract_deduplicates_by_study(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Two files from the same study
            _create_test_dicom(
                os.path.join(tmpdir, "series1.dcm"),
                PatientID="PAT001",
                StudyInstanceUID="1.2.3.4.5",
                SeriesInstanceUID="1.2.3.4.5.1",
            )
            _create_test_dicom(
                os.path.join(tmpdir, "series2.dcm"),
                PatientID="PAT001",
                StudyInstanceUID="1.2.3.4.5",
                SeriesInstanceUID="1.2.3.4.5.2",
            )
            df = extract_audit_log(tmpdir)
            assert len(df) == 1

    def test_extract_multiple_studies(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_test_dicom(
                os.path.join(tmpdir, "study1.dcm"),
                PatientID="PAT001",
                StudyInstanceUID="1.2.3.1",
            )
            _create_test_dicom(
                os.path.join(tmpdir, "study2.dcm"),
                PatientID="PAT002",
                StudyInstanceUID="1.2.3.2",
            )
            df = extract_audit_log(tmpdir)
            assert len(df) == 2

    def test_extract_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = extract_audit_log(tmpdir)
            assert len(df) == 0
            assert list(df.columns) == AUDIT_TAGS

    def test_extract_missing_tags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_test_dicom(
                os.path.join(tmpdir, "minimal.dcm"),
                PatientID="PAT001",
                StudyInstanceUID="1.2.3.4.5",
            )
            df = extract_audit_log(tmpdir)
            assert len(df) == 1
            # Missing tags should be empty strings
            assert df.iloc[0]["Manufacturer"] == ""


class TestBuildLinkerTable:
    def test_basic_linker(self):
        pre = pd.DataFrame(
            {
                "PatientID": ["PAT001"],
                "PatientName": ["John^Doe"],
                "AccessionNumber": ["ACC001"],
                "StudyInstanceUID": ["1.2.3.4.5"],
            }
        )
        post = pd.DataFrame(
            {
                "PatientID": ["ANON001"],
                "PatientName": ["A1B2C3^D4"],
                "AccessionNumber": ["abc123"],
                "StudyInstanceUID": ["2.25.12345"],
            }
        )
        linker = build_linker_table(pre, post)
        assert len(linker) == 1
        assert linker.iloc[0]["Original PatientID"] == "PAT001"
        assert linker.iloc[0]["Deidentified PatientID"] == "ANON001"

    def test_empty_dataframes(self):
        pre = pd.DataFrame()
        post = pd.DataFrame()
        linker = build_linker_table(pre, post)
        assert len(linker) == 0

    def test_mismatched_sizes(self):
        pre = pd.DataFrame(
            {
                "PatientID": ["PAT001", "PAT002"],
                "PatientName": ["A", "B"],
                "AccessionNumber": ["ACC1", "ACC2"],
                "StudyInstanceUID": ["1.1", "1.2"],
            }
        )
        post = pd.DataFrame(
            {
                "PatientID": ["ANON001"],
                "PatientName": ["X"],
                "AccessionNumber": ["anon1"],
                "StudyInstanceUID": ["2.1"],
            }
        )
        linker = build_linker_table(pre, post)
        # Should match only the first entry
        assert len(linker) == 1
