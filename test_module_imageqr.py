import logging
import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest
import pydicom

from module_imageqr import imageqr
from test_utils import cleanup_docker_containers, _create_test_dicom, _upload_dicom_to_orthanc, Fixtures, OrthancServer
from utils import PacsConfiguration, Spreadsheet


logging.basicConfig(level=logging.INFO)


def test_imageqr_pacs_with_accession_filter(tmp_path):
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
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": [f"ACC{i:03d}" for i in range(9)]
        })
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
        
        metadata_path = appdata_dir / "metadata.csv"
        assert metadata_path.exists(), "metadata.csv should exist"
        
        metadata_df = pd.read_csv(metadata_path)
        assert len(metadata_df) >= 3, "metadata.csv should have at least 3 rows"
        
        assert result["num_studies_found"] >= 3
        assert result["num_images_saved"] == 3
    
    finally:
        orthanc.stop()


def test_continuous_audit_log_saving(tmp_path):
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
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": [f"ACC{i:03d}" for i in range(5)]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        pacs_config = PacsConfiguration(host="localhost", port=orthanc.dicom_port, aet=orthanc.aet)
        
        import threading
        file_write_times = {}
        lock = threading.Lock()
        stop_monitoring = threading.Event()
        
        def monitor_files():
            while not stop_monitoring.is_set():
                filepath = appdata_dir / "metadata.csv"
                if filepath.exists():
                    mtime = os.path.getmtime(filepath)
                    with lock:
                        if "metadata.csv" not in file_write_times:
                            file_write_times["metadata.csv"] = []
                        if not file_write_times["metadata.csv"] or mtime != file_write_times["metadata.csv"][-1]:
                            file_write_times["metadata.csv"].append(mtime)
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
            write_count = len(file_write_times.get("metadata.csv", []))
            assert write_count >= 2, f"metadata.csv should have been written multiple times (found {write_count} writes), indicating continuous saving"
        
        assert (appdata_dir / "metadata.csv").exists()
    
    finally:
        orthanc.stop()


def test_imageqr_failures_reported(tmp_path):
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
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    pacs_config = PacsConfiguration(host="localhost", port=4242, aet="TEST_PACS")
    
    with patch('module_imageqr.find_studies_from_pacs_list') as mock_find_studies, \
         patch('module_imageqr.move_studies_from_study_pacs_map') as mock_move, \
         patch('module_imageqr.CTPPipeline') as mock_pipeline_class:
        
        mock_find_studies.return_value = ({}, [])
        mock_move.return_value = (0, [])
        mock_pipeline_instance = MagicMock()
        mock_pipeline_class.return_value.__enter__.return_value = mock_pipeline_instance
        mock_pipeline_instance.is_complete.return_value = True
        mock_pipeline_instance.metrics = MagicMock(files_saved=0, files_quarantined=0)
        mock_pipeline_instance.get_audit_log_csv.return_value = None
        
        query_file = appdata_dir / "query_acc.xlsx"
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
        mock_move.reset_mock()
        
        query_file = appdata_dir / "query_mrn.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": [None, None],
            "PatientID": ["MRN001", "MRN002"],
            "StudyDate": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-15")]
        })
        query_df.to_excel(query_file, index=False)
        query_spreadsheet = Spreadsheet.from_file(
            str(query_file), 
            acc_col="AccessionNumber",
            mrn_col="PatientID",
            date_col="StudyDate"
        )
        
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
        mock_move.reset_mock()
        
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
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": [f"ACC{i:03d}" for i in range(4)]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_configs = [
            PacsConfiguration(host="localhost", port=orthanc1.dicom_port, aet=orthanc1.aet),
            PacsConfiguration(host="localhost", port=orthanc2.dicom_port, aet=orthanc2.aet)
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
        ds1 = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        ds1.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds1, orthanc)
        
        ds2 = _create_test_dicom("", "MRN002", "Patient2", "CT", "3.0")
        ds2.InstanceNumber = 2
        _upload_dicom_to_orthanc(ds2, orthanc)
        
        ds3 = _create_test_dicom("", "MRN003", "Patient3", "CT", "3.0")
        ds3.InstanceNumber = 3
        _upload_dicom_to_orthanc(ds3, orthanc)
        
        time.sleep(2)
        
        query_file_valid = appdata_dir / "query_valid.xlsx"
        query_data_valid = {
            "AccessionNumber": ["ACC001", None, None],
            "PatientID": ["MRN001", "MRN002", "MRN003"],
            "StudyDate": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01")]
        }
        query_df_valid = pd.DataFrame(query_data_valid)
        query_df_valid.to_excel(query_file_valid, index=False)
        
        query_spreadsheet_valid = Spreadsheet.from_file(
            str(query_file_valid), 
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
            query_spreadsheet=query_spreadsheet_valid,
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
        ds1 = Fixtures.create_minimal_dicom(
            patient_id="MRN001",
            patient_name="Patient1",
            accession="",
            study_date="20250101",
            modality="CT",
            SliceThickness="3.0"
        )
        ds1.InstitutionName = "Test Hospital"
        ds1.Manufacturer = "TestManufacturer"
        ds1.ManufacturerModelName = "TestModel"
        ds1.SeriesNumber = 1
        ds1.InstanceNumber = 1
        ds1.SamplesPerPixel = 1
        ds1.PhotometricInterpretation = "MONOCHROME2"
        ds1.Rows = 64
        ds1.Columns = 64
        ds1.BitsAllocated = 16
        ds1.BitsStored = 16
        ds1.HighBit = 15
        ds1.PixelRepresentation = 0
        ds1.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(ds1, orthanc)
        
        ds2 = Fixtures.create_minimal_dicom(
            patient_id="MRN001",
            patient_name="Patient1",
            accession="",
            study_date="20250103",
            modality="CT",
            SliceThickness="3.0"
        )
        ds2.InstitutionName = "Test Hospital"
        ds2.Manufacturer = "TestManufacturer"
        ds2.ManufacturerModelName = "TestModel"
        ds2.SeriesNumber = 1
        ds2.InstanceNumber = 2
        ds2.SamplesPerPixel = 1
        ds2.PhotometricInterpretation = "MONOCHROME2"
        ds2.Rows = 64
        ds2.Columns = 64
        ds2.BitsAllocated = 16
        ds2.BitsStored = 16
        ds2.HighBit = 15
        ds2.PixelRepresentation = 0
        ds2.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(ds2, orthanc)
        
        ds3 = Fixtures.create_minimal_dicom(
            patient_id="MRN001",
            patient_name="Patient1",
            accession="",
            study_date="20250110",
            modality="CT",
            SliceThickness="3.0"
        )
        ds3.InstitutionName = "Test Hospital"
        ds3.Manufacturer = "TestManufacturer"
        ds3.ManufacturerModelName = "TestModel"
        ds3.SeriesNumber = 1
        ds3.InstanceNumber = 3
        ds3.SamplesPerPixel = 1
        ds3.PhotometricInterpretation = "MONOCHROME2"
        ds3.Rows = 64
        ds3.Columns = 64
        ds3.BitsAllocated = 16
        ds3.BitsStored = 16
        ds3.HighBit = 15
        ds3.PixelRepresentation = 0
        ds3.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(ds3, orthanc)
        
        time.sleep(2)
        
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

