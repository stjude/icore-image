import logging
import os
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
import pydicom

from module_imageqr import imageqr
from test_utils import OrthancServer, _create_test_dicom, _upload_dicom_to_orthanc, Fixtures
from utils import Spreadsheet, PacsConfiguration
from dcmtk import get_study


logging.basicConfig(level=logging.INFO)


def test_imageqr_pacs_with_accession_filter(tmp_path):
    """Test imageqr with filter script that filters by modality and slice thickness."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i in range(3):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "CT", "0.5")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)

        for i in range(3, 6):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)

        for i in range(6, 9):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "MR", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": [f"ACC{i:03d}" for i in range(9)]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        filter_script = 'Modality.contains("CT") * SliceThickness.isGreaterThan("1") * SliceThickness.isLessThan("5")'

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            filter_script=filter_script
        )

        images_dir = output_dir / "images"
        output_files = list(images_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 .dcm files, found {len(output_files)}"

        for file in output_files:
            ds = pydicom.dcmread(file)
            assert ds.Modality == "CT", "Modality should be CT"
            assert float(ds.SliceThickness) > 1 and float(ds.SliceThickness) < 5, "SliceThickness should be between 1 and 5"

        metadata_path = appdata_dir / "metadata.xlsx"
        assert metadata_path.exists(), "metadata.xlsx should exist"

        metadata_df = pd.read_excel(metadata_path)
        assert len(metadata_df) >= 3, "metadata.xlsx should have at least 3 rows"

        assert result["num_studies_found"] >= 3
        assert result["num_images_saved"] == 3

        quarantine_dir = appdata_dir / "quarantine"
        assert quarantine_dir.exists(), "Quarantine directory should exist in appdata"

        quarantined_files = list(quarantine_dir.rglob("*.dcm"))
        assert len(quarantined_files) == 6, f"Expected 6 quarantined files, found {len(quarantined_files)}"

        for file in quarantined_files:
            ds = pydicom.dcmread(file)
            assert ds.Modality in ["CT", "MR"], "Quarantined file should be either CT or MR"
            if ds.Modality == "CT":
                slice_thickness = float(ds.SliceThickness)
                assert slice_thickness <= 1.0 or slice_thickness >= 5.0, f"Quarantined CT should have slice thickness <=1 or >=5, got {slice_thickness}"
            else:
                assert ds.Modality == "MR", "Non-CT quarantined file should be MR"
    
    finally:
        orthanc.stop()


def test_continuous_audit_log_saving(tmp_path):
    """Test that metadata file is saved continuously during processing."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i in range(5):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": [f"ACC{i:03d}" for i in range(5)]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        file_write_times = {}
        lock = threading.Lock()
        stop_monitoring = threading.Event()

        def monitor_files():
            while not stop_monitoring.is_set():
                filepath = appdata_dir / "metadata.xlsx"
                if filepath.exists():
                    mtime = os.path.getmtime(filepath)
                    with lock:
                        if "metadata.xlsx" not in file_write_times:
                            file_write_times["metadata.xlsx"] = []
                        if not file_write_times["metadata.xlsx"] or mtime != file_write_times["metadata.xlsx"][-1]:
                            file_write_times["metadata.xlsx"].append(mtime)
                time.sleep(1)

        monitor_thread = threading.Thread(target=monitor_files, daemon=True)
        monitor_thread.start()

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        stop_monitoring.set()
        monitor_thread.join(timeout=2)

        with lock:
            write_count = len(file_write_times.get("metadata.xlsx", []))
            assert write_count >= 2, f"metadata.xlsx should have been written multiple times (found {write_count} writes), indicating continuous saving"

        assert (appdata_dir / "metadata.xlsx").exists()
    
    finally:
        orthanc.stop()


def test_imageqr_failures_reported(tmp_path):
    """Test that failures are properly reported when PACS is unreachable."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()

    query_file = appdata_dir / "query.xlsx"
    query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002", "ACC003"]})
    query_df.to_excel(query_file, index=False)
    
    query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

    invalid_pacs_config = PacsConfiguration(
        host="invalid-host-that-does-not-exist.local",
        port=99999,
        aet="INVALID_AET"
    )

    result = imageqr(
        pacs_list=[invalid_pacs_config],
        query_spreadsheet=query_spreadsheet,
        application_aet="TEST_AET",
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir)
    )

    assert len(result["failed_query_indices"]) == 3, "All 3 queries should have failed"
    assert result["failed_query_indices"] == [0, 1, 2], "Failed indices should be [0, 1, 2]"
    assert result["num_studies_found"] == 0, "No studies should be found"
    assert result["num_images_saved"] == 0, "No images should be saved"


def test_imageqr_filter_script_generation(tmp_path):
    """Test that filter scripts are correctly generated from query spreadsheets."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()

    pacs_config = PacsConfiguration(host="localhost", port=4242, aet="TEST_PACS")

    with patch('module_imageqr.find_studies_from_pacs_list') as mock_find_studies, \
         patch('module_imageqr.get_studies_from_study_pacs_map') as mock_get, \
         patch('module_imageqr.CTPPipeline') as mock_pipeline_class:

        mock_find_studies.return_value = ({}, [], {})
        mock_get.return_value = (0, [], {})
        mock_pipeline_instance = MagicMock()
        mock_pipeline_class.return_value.__enter__.return_value = mock_pipeline_instance
        mock_pipeline_instance.is_complete.return_value = True
        mock_pipeline_instance.metrics = MagicMock(files_saved=0, files_quarantined=0)
        mock_pipeline_instance.get_audit_log_csv.return_value = None

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        call_kwargs = mock_pipeline_class.call_args[1]
        expected_filter = 'AccessionNumber.contains("ACC001") + AccessionNumber.contains("ACC002")'
        assert call_kwargs['filter_script'] == expected_filter, f"Expected filter: {expected_filter}, got: {call_kwargs['filter_script']}"

        mock_pipeline_class.reset_mock()
        mock_find_studies.reset_mock()
        mock_get.reset_mock()

        query_file = appdata_dir / "query2.xlsx"
        query_df = pd.DataFrame({
            "PatientID": ["MRN001", "MRN002"],
            "StudyDate": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-15")]
        })
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), mrn_col="PatientID", date_col="StudyDate")

        imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        call_kwargs = mock_pipeline_class.call_args[1]
        expected_filter = '(PatientID.contains("MRN001") * StudyDate.isGreaterThan("20241231") * StudyDate.isLessThan("20250102")) + (PatientID.contains("MRN002") * StudyDate.isGreaterThan("20250114") * StudyDate.isLessThan("20250116"))'
        assert call_kwargs['filter_script'] == expected_filter, f"Expected filter: {expected_filter}, got: {call_kwargs['filter_script']}"

        mock_pipeline_class.reset_mock()
        mock_find_studies.reset_mock()
        mock_get.reset_mock()

        user_filter = 'Modality.contains("CT")'

        imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            filter_script=user_filter
        )

        call_kwargs = mock_pipeline_class.call_args[1]
        expected_combined = '(Modality.contains("CT")) * ((PatientID.contains("MRN001") * StudyDate.isGreaterThan("20241231") * StudyDate.isLessThan("20250102")) + (PatientID.contains("MRN002") * StudyDate.isGreaterThan("20250114") * StudyDate.isLessThan("20250116")))'
        assert call_kwargs['filter_script'] == expected_combined, f"Expected filter: {expected_combined}, got: {call_kwargs['filter_script']}"


def test_imageqr_multiple_pacs(tmp_path):
    """Test querying from multiple PACS servers simultaneously."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc1 = OrthancServer(aet="ORTHANC1")
    orthanc1.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc1.start()

    orthanc2 = OrthancServer(aet="ORTHANC2")
    orthanc2.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc2.start()

    try:
        for i in range(2):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc1)

        for i in range(2, 4):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc2)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": [f"ACC{i:03d}" for i in range(4)]})
        query_df.to_excel(query_file, index=False)
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_configs = [
            orthanc1.get_pacs_config(),
            orthanc2.get_pacs_config()
        ]

        result = imageqr(
            pacs_list=pacs_configs,
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        assert result["num_studies_found"] == 4, f"Should find 4 studies (2 from each PACS), found {result['num_studies_found']}"
        assert result["num_images_saved"] == 4, f"Should save 4 images, saved {result['num_images_saved']}"

        images_dir = output_dir / "images"
        output_files = list(images_dir.rglob("*.dcm"))
        assert len(output_files) == 4, f"Expected 4 .dcm files, found {len(output_files)}"

    finally:
        orthanc1.stop()
        orthanc2.stop()


def test_imageqr_pacs_mrn_study_date_fallback(tmp_path):
    """Test fallback to MRN/StudyDate query when accession is missing."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        ds = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        ds.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds, orthanc)

        ds = _create_test_dicom("", "MRN002", "Patient2", "CT", "3.0")
        ds.InstanceNumber = 2
        _upload_dicom_to_orthanc(ds, orthanc)

        ds = _create_test_dicom("", "MRN003", "Patient3", "CT", "3.0")
        ds.InstanceNumber = 3
        _upload_dicom_to_orthanc(ds, orthanc)

        query_file = appdata_dir / "query_valid.xlsx"
        query_data = {
            "AccessionNumber": ["ACC001", None, None],
            "PatientID": ["MRN001", "MRN002", "MRN003"],
            "StudyDate": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01")]
        }
        query_df = pd.DataFrame(query_data)
        query_df.to_excel(query_file, index=False)

        query_spreadsheet = Spreadsheet.from_file(
            str(query_file),
            acc_col="AccessionNumber",
            mrn_col="PatientID",
            date_col="StudyDate"
        )

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        assert result["num_studies_found"] == 3, f"Should find 3 studies, found {result['num_studies_found']}"
        assert result["num_images_saved"] == 3, f"Should save 3 images, saved {result['num_images_saved']}"

        images_dir = output_dir / "images"
        output_files = list(images_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 .dcm files, found {len(output_files)}"

        query_file_invalid = appdata_dir / "query_invalid.xlsx"
        query_data_invalid = {
            "AccessionNumber": ["ACC001", None, None, None],
            "PatientID": ["MRN001", "MRN002", "MRN003", "MRN004"],
            "StudyDate": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01"), "invalid-date"]
        }
        query_df_invalid = pd.DataFrame(query_data_invalid)
        query_df_invalid.to_excel(query_file_invalid, index=False)

        query_spreadsheet_invalid = Spreadsheet.from_file(
            str(query_file_invalid),
            acc_col="AccessionNumber",
            mrn_col="PatientID",
            date_col="StudyDate"
        )

        with pytest.raises(ValueError, match="StudyDate must be in Excel date format"):
            imageqr(
                pacs_list=[pacs_config],
                query_spreadsheet=query_spreadsheet_invalid,
                application_aet="TEST_AET",
                output_dir=str(output_dir),
                appdata_dir=str(appdata_dir)
            )
    
    finally:
        orthanc.stop()


def test_imageqr_pacs_date_window(tmp_path):
    """Test date window functionality for MRN/date queries."""
    import numpy as np
    
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        dicom1 = Fixtures.create_minimal_dicom(
            patient_id="MRN001",
            patient_name="Patient1",
            accession="",
            study_date="20250101",
            modality="CT",
            SliceThickness="3.0"
        )
        dicom1.InstanceNumber = 1
        dicom1.SamplesPerPixel = 1
        dicom1.PhotometricInterpretation = "MONOCHROME2"
        dicom1.Rows = 64
        dicom1.Columns = 64
        dicom1.BitsAllocated = 16
        dicom1.BitsStored = 16
        dicom1.HighBit = 15
        dicom1.PixelRepresentation = 0
        dicom1.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(dicom1, orthanc)

        dicom2 = Fixtures.create_minimal_dicom(
            patient_id="MRN001",
            patient_name="Patient1",
            accession="",
            study_date="20250103",
            modality="CT",
            SliceThickness="3.0"
        )
        dicom2.InstanceNumber = 2
        dicom2.SamplesPerPixel = 1
        dicom2.PhotometricInterpretation = "MONOCHROME2"
        dicom2.Rows = 64
        dicom2.Columns = 64
        dicom2.BitsAllocated = 16
        dicom2.BitsStored = 16
        dicom2.HighBit = 15
        dicom2.PixelRepresentation = 0
        dicom2.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(dicom2, orthanc)

        dicom3 = Fixtures.create_minimal_dicom(
            patient_id="MRN001",
            patient_name="Patient1",
            accession="",
            study_date="20250110",
            modality="CT",
            SliceThickness="3.0"
        )
        dicom3.InstanceNumber = 3
        dicom3.SamplesPerPixel = 1
        dicom3.PhotometricInterpretation = "MONOCHROME2"
        dicom3.Rows = 64
        dicom3.Columns = 64
        dicom3.BitsAllocated = 16
        dicom3.BitsStored = 16
        dicom3.HighBit = 15
        dicom3.PixelRepresentation = 0
        dicom3.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(dicom3, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": [None],
            "PatientID": ["MRN001"],
            "StudyDate": [pd.Timestamp("2025-01-03")]
        })
        query_df.to_excel(query_file, index=False)

        query_spreadsheet = Spreadsheet.from_file(
            str(query_file),
            acc_col="AccessionNumber",
            mrn_col="PatientID",
            date_col="StudyDate"
        )

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            date_window_days=2
        )

        images_dir = output_dir / "images"
        output_files = list(images_dir.rglob("*.dcm"))

        study_dates_found = set()
        for file in output_files:
            ds = pydicom.dcmread(file)
            study_dates_found.add(ds.StudyDate)

        assert result["num_studies_found"] >= 2, f"Should find at least 2 studies within 2-day window, found {result['num_studies_found']}"
        assert result["num_images_saved"] >= 2, f"Should save at least 2 images, saved {result['num_images_saved']}"
        assert "20250101" in study_dates_found and "20250103" in study_dates_found, f"Should find both studies within 2-day window. Found dates: {study_dates_found}"
        assert "20250110" not in study_dates_found, f"Should not find study outside window. Found dates: {study_dates_found}"
    
    finally:
        orthanc.stop()


def test_imageqr_accession_wildcard_filtering(tmp_path):
    """Test that accession number matching uses wildcards correctly."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        dicom1 = _create_test_dicom("  ABC001  ", "MRN001", "Patient1", "CT", "3.0")
        dicom1.InstanceNumber = 1
        _upload_dicom_to_orthanc(dicom1, orthanc)

        dicom2 = _create_test_dicom("ABC001", "MRN002", "Patient2", "CT", "3.0")
        dicom2.InstanceNumber = 2
        _upload_dicom_to_orthanc(dicom2, orthanc)

        dicom3 = _create_test_dicom("12ABC0011", "MRN003", "Patient3", "CT", "3.0")
        dicom3.InstanceNumber = 3
        _upload_dicom_to_orthanc(dicom3, orthanc)

        dicom4 = _create_test_dicom("ABC001ABC", "MRN004", "Patient4", "CT", "3.0")
        dicom4.InstanceNumber = 4
        _upload_dicom_to_orthanc(dicom4, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ABC001"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        images_dir = output_dir / "images"
        output_files = list(images_dir.rglob("*.dcm"))

        assert result["num_studies_found"] == 2, f"Should find exactly 2 studies (with exact match and whitespace), found {result['num_studies_found']}"
        assert len(output_files) == 2, f"Expected 2 .dcm files, found {len(output_files)}"

        found_accessions = set()
        for file in output_files:
            ds = pydicom.dcmread(file)
            found_accessions.add(ds.AccessionNumber.strip())

        assert found_accessions == {"ABC001"}, f"Should only find studies with exact AccessionNumber 'ABC001' (after trimming), found: {found_accessions}"
    
    finally:
        orthanc.stop()


def test_imageqr_saves_failed_queries_csv_on_find_failure(tmp_path):
    """Test that failed queries are saved to CSV with appropriate failure reasons."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        dicom = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        dicom.InstanceNumber = 1
        _upload_dicom_to_orthanc(dicom, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC999", "ACC998"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        assert result["num_studies_found"] == 1
        assert len(result["failed_query_indices"]) == 2

        csv_path = appdata_dir / "failed_queries.csv"
        assert csv_path.exists(), "failed_queries.csv should exist"

        df = pd.read_csv(csv_path)
        assert len(df) == 2
        assert list(df.columns) == ["Accession Number", "Failure Reason"]
        assert df.loc[0, "Accession Number"] == "ACC999"
        assert df.loc[0, "Failure Reason"] == "Failed to find images"
        assert df.loc[1, "Accession Number"] == "ACC998"
        assert df.loc[1, "Failure Reason"] == "Failed to find images"
    
    finally:
        orthanc.stop()


def test_imageqr_saves_failed_queries_csv_with_mrn_date(tmp_path):
    """Test that failed queries CSV works with MRN/Date columns."""
    import numpy as np
    
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        dicom = Fixtures.create_minimal_dicom(
            accession="",
            patient_id="MRN001",
            patient_name="Patient1",
            study_date="20250115",
            modality="CT",
            SliceThickness="3.0"
        )
        dicom.InstanceNumber = 1
        dicom.SamplesPerPixel = 1
        dicom.PhotometricInterpretation = "MONOCHROME2"
        dicom.Rows = 64
        dicom.Columns = 64
        dicom.BitsAllocated = 16
        dicom.BitsStored = 16
        dicom.HighBit = 15
        dicom.PixelRepresentation = 0
        dicom.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(dicom, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({
            "PatientID": ["MRN001", "MRN999"],
            "StudyDate": [pd.Timestamp("2025-01-15"), pd.Timestamp("2025-02-20")]
        })
        query_df.to_excel(query_file, index=False)

        query_spreadsheet = Spreadsheet.from_file(
            str(query_file),
            mrn_col="PatientID",
            date_col="StudyDate"
        )

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        result = imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        assert result["num_studies_found"] == 1
        assert len(result["failed_query_indices"]) == 1

        csv_path = appdata_dir / "failed_queries.csv"
        assert csv_path.exists(), "failed_queries.csv should exist"

        df = pd.read_csv(csv_path)
        assert len(df) == 1
        assert list(df.columns) == ["MRN", "Date", "Failure Reason"]
        assert df.loc[0, "MRN"] == "MRN999"
        assert df.loc[0, "Date"] == "2025-02-20"
        assert df.loc[0, "Failure Reason"] == "Failed to find images"

    finally:
        orthanc.stop()


def test_imageqr_cleans_up_getscu_temp(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")

    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    output_dir.mkdir()
    appdata_dir.mkdir()

    pacs_config = PacsConfiguration(host="localhost", port=4242, aet="TEST_PACS")

    with patch('module_imageqr.find_studies_from_pacs_list') as mock_find_studies, \
         patch('module_imageqr.get_studies_from_study_pacs_map') as mock_get, \
         patch('module_imageqr.CTPPipeline') as mock_pipeline_class:

        mock_find_studies.return_value = ({}, [], {})
        mock_get.return_value = (0, [], {})
        mock_pipeline_instance = MagicMock()
        mock_pipeline_class.return_value.__enter__.return_value = mock_pipeline_instance
        mock_pipeline_instance.is_complete.return_value = True
        mock_pipeline_instance.metrics = MagicMock(files_saved=0, files_quarantined=0)
        mock_pipeline_instance.get_audit_log_csv.return_value = None

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001"]})
        query_df.to_excel(query_file, index=False)
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        imageqr(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir)
        )

        getscu_temp = appdata_dir / "getscu_temp"
        assert not getscu_temp.exists(), "getscu_temp should be removed after successful completion"


def test_imageqr_cleans_up_getscu_temp_on_error(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")

    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    output_dir.mkdir()
    appdata_dir.mkdir()

    pacs_config = PacsConfiguration(host="localhost", port=4242, aet="TEST_PACS")

    with patch('module_imageqr.find_studies_from_pacs_list') as mock_find_studies, \
         patch('module_imageqr.get_studies_from_study_pacs_map') as mock_get, \
         patch('module_imageqr.CTPPipeline') as mock_pipeline_class:

        mock_find_studies.return_value = ({}, [], {})
        mock_get.return_value = (0, [], {})
        mock_pipeline_class.return_value.__enter__.return_value = MagicMock()
        mock_pipeline_class.return_value.__exit__.side_effect = RuntimeError("Pipeline failure")

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001"]})
        query_df.to_excel(query_file, index=False)
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        with pytest.raises(RuntimeError, match="Pipeline failure"):
            imageqr(
                pacs_list=[pacs_config],
                query_spreadsheet=query_spreadsheet,
                application_aet="TEST_AET",
                output_dir=str(output_dir),
                appdata_dir=str(appdata_dir)
            )

        getscu_temp = appdata_dir / "getscu_temp"
        assert not getscu_temp.exists(), "getscu_temp should be removed even when pipeline raises"


def test_imageqr_continues_despite_get_failures(tmp_path, capsys):
    """Test that imageqr job continues despite C-GET failures and zero file retrievals."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")

    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    output_dir.mkdir()
    appdata_dir.mkdir()

    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()

    try:
        # Upload 3 studies
        for i in range(3):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "CT", "2.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": [f"ACC{i:03d}" for i in range(3)]})
        query_df.to_excel(query_file, index=False)

        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        call_count = {"count": 0}

        def mock_get_study(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                # First call: simulate zero files retrieved
                return {
                    "success": True,
                    "num_completed": 0,
                    "num_failed": 0,
                    "num_warning": 0,
                    "message": "Get completed with no sub-operations"
                }
            elif call_count["count"] == 2:
                # Second call: simulate exception
                raise Exception("Network timeout during C-GET")
            else:
                # Third call: actually retrieve files
                return get_study(*args, **kwargs)

        with patch('utils.get_study', side_effect=mock_get_study):
            result = imageqr(
                pacs_list=[pacs_config],
                query_spreadsheet=query_spreadsheet,
                application_aet="TEST_AET",
                output_dir=str(output_dir),
                appdata_dir=str(appdata_dir)
            )

        assert result is not None, "imageqr should return a result"

        assert result["num_studies_found"] == 3, "Should have found 3 studies"

        assert len(result["failed_query_indices"]) == 2, "Should have 2 failed queries"
        assert 0 in result["failed_query_indices"], "First query should have failed (zero files)"
        assert 1 in result["failed_query_indices"], "Second query should have failed (exception)"

        captured = capsys.readouterr()
        assert "0 files" in captured.out or "Exception while retrieving" in captured.out, "Should log failures"

        csv_path = appdata_dir / "failed_queries.csv"
        assert csv_path.exists(), "failed_queries.csv should exist"

        df = pd.read_csv(csv_path)
        assert len(df) == 2, "Should have 2 failed queries in CSV"

    finally:
        orthanc.stop()
