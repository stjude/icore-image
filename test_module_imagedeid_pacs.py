import logging
import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest
import pydicom

from module_imagedeid_local import _generate_lookup_table_content
from module_imagedeid_pacs import imagedeid_pacs
from test_utils import cleanup_docker_containers, _create_test_dicom, _upload_dicom_to_orthanc, Fixtures, OrthancServer
from utils import PacsConfiguration, Spreadsheet


logging.basicConfig(level=logging.INFO)


def test_imagedeid_pacs_with_accession_filter(tmp_path):
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
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080080" n="InstitutionName">@remove()</e>
<e en="T" t="00080090" n="ReferringPhysicianName">@remove()</e>
<e en="T" t="00080070" n="Manufacturer">@keep()</e>
<e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
        
        filter_script = 'Modality.contains("CT") * SliceThickness.isGreaterThan("1") * SliceThickness.isLessThan("5")'
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            filter_script=filter_script,
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 .dcm files, found {len(output_files)}"
        
        for file in output_files:
            ds = pydicom.dcmread(file)
            
            assert ds.PatientName == "", "PatientName should be empty"
            assert ds.PatientID == "", "PatientID should be empty"
            assert not hasattr(ds, 'InstitutionName') or ds.InstitutionName == "", "InstitutionName should be removed"
            assert not hasattr(ds, 'ReferringPhysicianName') or ds.ReferringPhysicianName == "", "ReferringPhysicianName should be removed"
            assert ds.Manufacturer == "TestManufacturer", "Manufacturer should be kept"
            assert ds.ManufacturerModelName == "TestModel", "ManufacturerModelName should be kept"
            assert ds.Modality == "CT", "Modality should be kept"
        
        deid_metadata_path = appdata_dir / "deid_metadata.xlsx"
        assert deid_metadata_path.exists(), "deid_metadata.xlsx should exist"
        
        deid_df = pd.read_excel(deid_metadata_path)
        assert len(deid_df) == 3, "deid_metadata.xlsx should have 3 rows"
        
        if "PatientName" in deid_df.columns:
            patient_names = deid_df["PatientName"].astype(str)
            assert all(name in ["", "nan", '=""'] for name in patient_names), "PatientName should be empty"
        if "PatientID" in deid_df.columns:
            patient_ids = deid_df["PatientID"].astype(str)
            assert all(pid in ["", "nan", '=""'] for pid in patient_ids), "PatientID should be empty"
        
        for _, row in deid_df.iterrows():
            for col in row.index:
                value = str(row[col])
                assert "Smith" not in value, f"PHI (Smith) found in column {col}"
                assert "John" not in value, f"PHI (John) found in column {col}"
        
        linker_path = appdata_dir / "linker.xlsx"
        assert linker_path.exists(), "linker.xlsx should exist"
        
        linker_df = pd.read_excel(linker_path)
        assert len(linker_df) > 0, "linker.xlsx should have content"
        assert "Original AccessionNumber" in linker_df.columns, "Linker should have Original AccessionNumber column"
        assert "Trial AccessionNumber" in linker_df.columns, "Linker should have Trial AccessionNumber column"
        
        original_accessions_in_linker = set()
        for _, row in linker_df.iterrows():
            orig_acc = str(row["Original AccessionNumber"])
            orig_acc = orig_acc.strip().strip('="').strip('(').strip(')').strip('"')
            if orig_acc and orig_acc != "nan":
                original_accessions_in_linker.add(orig_acc)
        
        expected_accessions = {f"ACC{i:03d}" for i in range(3, 6)}
        found_accessions = original_accessions_in_linker & expected_accessions
        assert len(found_accessions) >= 1, f"At least one of the expected accession numbers should be in linker. Found: {original_accessions_in_linker}, Expected: {expected_accessions}"
        
        for _, row in linker_df.iterrows():
            trial_acc = str(row["Trial AccessionNumber"]).strip().strip('="').strip('(').strip(')').strip('"')
            orig_acc = str(row["Original AccessionNumber"]).strip().strip('="').strip('(').strip(')').strip('"')
            if trial_acc and orig_acc and trial_acc != "nan" and orig_acc != "nan":
                assert trial_acc != orig_acc, f"Trial accession should be different from original: {trial_acc} vs {orig_acc}"
        
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
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
        
        import threading
        file_write_times = {}
        lock = threading.Lock()
        stop_monitoring = threading.Event()
        
        def monitor_files():
            while not stop_monitoring.is_set():
                for filename in ["metadata.xlsx", "deid_metadata.xlsx", "linker.xlsx"]:
                    filepath = appdata_dir / filename
                    if filepath.exists():
                        mtime = os.path.getmtime(filepath)
                        with lock:
                            if filename not in file_write_times:
                                file_write_times[filename] = []
                            if not file_write_times[filename] or mtime != file_write_times[filename][-1]:
                                file_write_times[filename].append(mtime)
                time.sleep(1)
        
        monitor_thread = threading.Thread(target=monitor_files, daemon=True)
        monitor_thread.start()
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        stop_monitoring.set()
        monitor_thread.join(timeout=2)
        
        with lock:
            for filename in ["metadata.xlsx", "deid_metadata.xlsx", "linker.xlsx"]:
                write_count = len(file_write_times.get(filename, []))
                assert write_count >= 2, f"{filename} should have been written multiple times (found {write_count} writes), indicating continuous saving"
        
        assert (appdata_dir / "metadata.xlsx").exists()
        assert (appdata_dir / "deid_metadata.xlsx").exists()
        assert (appdata_dir / "linker.xlsx").exists()
    
    finally:
        orthanc.stop()


def test_imagedeid_failures_reported(tmp_path):
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
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
    
    result = imagedeid_pacs(
        pacs_list=[invalid_pacs_config],
        query_spreadsheet=query_spreadsheet,
        application_aet="TEST_AET",
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )
    
    assert len(result["failed_query_indices"]) == 3, "All 3 queries should have failed"
    assert result["failed_query_indices"] == [0, 1, 2], "Failed indices should be [0, 1, 2]"
    assert result["num_studies_found"] == 0, "No studies should be found"
    assert result["num_images_saved"] == 0, "No images should be saved"


def test_imagedeid_filter_script_generation(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    pacs_config = PacsConfiguration(host="localhost", port=4242, aet="TEST_PACS")
    
    with patch('module_imagedeid_pacs.find_studies_from_pacs_list') as mock_find_studies, \
         patch('module_imagedeid_pacs.get_studies_from_study_pacs_map') as mock_get, \
         patch('module_imagedeid_pacs.CTPPipeline') as mock_pipeline_class:
        
        mock_find_studies.return_value = ({}, [], {})
        mock_get.return_value = (0, [], {})
        mock_pipeline_instance = MagicMock()
        mock_pipeline_class.return_value.__enter__.return_value = mock_pipeline_instance
        mock_pipeline_instance.is_complete.return_value = True
        mock_pipeline_instance.metrics = MagicMock(files_saved=0, files_quarantined=0)
        mock_pipeline_instance.get_audit_log_csv.return_value = None
        mock_pipeline_instance.get_idmap_csv.return_value = None
        
        query_file = appdata_dir / "query_acc.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            apply_default_filter_script=False
        )
        
        call_kwargs = mock_pipeline_class.call_args[1]
        expected_filter = 'AccessionNumber.contains("ACC001") + AccessionNumber.contains("ACC002")'
        assert call_kwargs['filter_script'] == expected_filter, f"Expected filter: {expected_filter}, got: {call_kwargs['filter_script']}"
        
        mock_pipeline_class.reset_mock()
        mock_find_studies.reset_mock()
        mock_get.reset_mock()
        
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
        
        imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            apply_default_filter_script=False
        )
        
        call_kwargs = mock_pipeline_class.call_args[1]
        expected_filter = '(PatientID.contains("MRN001") * StudyDate.isGreaterThan("20241231") * StudyDate.isLessThan("20250102")) + (PatientID.contains("MRN002") * StudyDate.isGreaterThan("20250114") * StudyDate.isLessThan("20250116"))'
        assert call_kwargs['filter_script'] == expected_filter, f"Expected filter: {expected_filter}, got: {call_kwargs['filter_script']}"
        
        mock_pipeline_class.reset_mock()
        mock_find_studies.reset_mock()
        mock_get.reset_mock()
        
        user_filter = 'Modality.contains("CT")'
        
        imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            filter_script=user_filter,
            apply_default_filter_script=False
        )
        
        call_kwargs = mock_pipeline_class.call_args[1]
        expected_combined = '(Modality.contains("CT")) * ((PatientID.contains("MRN001") * StudyDate.isGreaterThan("20241231") * StudyDate.isLessThan("20250102")) + (PatientID.contains("MRN002") * StudyDate.isGreaterThan("20250114") * StudyDate.isLessThan("20250116")))'
        assert call_kwargs['filter_script'] == expected_combined, f"Expected filter: {expected_combined}, got: {call_kwargs['filter_script']}"


def test_imagedeid_multiple_pacs(tmp_path):
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
        
        result = imagedeid_pacs(
            pacs_list=pacs_configs,
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 4, f"Should find 4 studies (2 from each PACS), found {result['num_studies_found']}"
        assert result["num_images_saved"] == 4, f"Should save 4 images, saved {result['num_images_saved']}"
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 4, f"Expected 4 .dcm files, found {len(output_files)}"
    
    finally:
        orthanc1.stop()
        orthanc2.stop()


def test_imagedeid_pacs_mrn_study_date_fallback(tmp_path):
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
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet_valid,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 3, f"Should find 3 studies, found {result['num_studies_found']}"
        assert result["num_images_saved"] == 3, f"Should save 3 images, saved {result['num_images_saved']}"
        
        output_files = list(output_dir.rglob("*.dcm"))
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
            imagedeid_pacs(
                pacs_list=[pacs_config],
                query_spreadsheet=query_spreadsheet_invalid,
                application_aet="TEST_AET",
                output_dir=str(output_dir),
                appdata_dir=str(appdata_dir)
            )
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_date_window(tmp_path):
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
        
        anonymizer_script = """<script>
<e en="T" t="00080020" n="StudyDate">@keep()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            date_window_days=2,
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        
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


def test_imagedeid_pacs_deid_pixels_parameter(tmp_path):
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
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            deid_pixels=True,
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 1, f"Should find 1 study, found {result['num_studies_found']}"
        assert result["num_images_saved"] == 1, f"Should save 1 image, saved {result['num_images_saved']}"
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 1, f"Expected 1 .dcm file, found {len(output_files)}"
        
        linker_path = appdata_dir / "linker.xlsx"
        assert linker_path.exists(), "linker.xlsx should exist"
        
        deid_metadata_path = appdata_dir / "deid_metadata.xlsx"
        assert deid_metadata_path.exists(), "deid_metadata.xlsx should exist"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_apply_default_filter_script(tmp_path):
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
            accession="ACC001",
            study_date="20250101",
            modality="CT"
        )
        ds1.Manufacturer = "UNKNOWN VENDOR"
        ds1.ImageType = ["DERIVED", "SECONDARY"]
        ds1.Rows = 512
        ds1.Columns = 512
        ds1.SamplesPerPixel = 1
        ds1.PhotometricInterpretation = "MONOCHROME2"
        ds1.BitsAllocated = 16
        ds1.BitsStored = 16
        ds1.HighBit = 15
        ds1.PixelRepresentation = 0
        ds1.PixelData = np.random.randint(0, 4096, (512, 512), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(ds1, orthanc)

        ds2 = Fixtures.create_minimal_dicom(
            patient_id="MRN002",
            patient_name="Patient2",
            accession="ACC002",
            study_date="20250101",
            modality="CT"
        )
        ds2.Manufacturer = "GE MEDICAL SYSTEMS"
        ds2.ManufacturerModelName = "REVOLUTION CT"
        ds2.SoftwareVersions = "REVO_CT_22BC.50"
        ds2.ImageType = ["ORIGINAL", "PRIMARY"]
        ds2.Rows = 512
        ds2.Columns = 512
        ds2.SamplesPerPixel = 1
        ds2.PhotometricInterpretation = "MONOCHROME2"
        ds2.BitsAllocated = 16
        ds2.BitsStored = 16
        ds2.HighBit = 15
        ds2.PixelRepresentation = 0
        ds2.PixelData = np.random.randint(0, 4096, (512, 512), dtype=np.uint16).tobytes()
        _upload_dicom_to_orthanc(ds2, orthanc)

        time.sleep(2)

        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)

        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")

        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )

        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""

        result_without_filter = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False,
        )

        assert result_without_filter["num_images_saved"] == 2, "Without default filter, both images should be saved"
        assert result_without_filter["num_images_quarantined"] == 0, "Without default filter, no images should be quarantined"

        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 2, "Both DICOM files should be in output"

        for file in output_dir.rglob("*.dcm"):
            file.unlink()
        quarantine_dir = appdata_dir / "quarantine"
        if quarantine_dir.exists():
            for file in quarantine_dir.rglob("*.dcm"):
                file.unlink()

        result_with_filter = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=True
        )

        assert result_with_filter["num_images_saved"] == 1, "With default filter, only primary CT should be saved"
        assert result_with_filter["num_images_quarantined"] == 1, "With default filter, derived CT should be quarantined"

        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 1, "Only one DICOM file should be in output"

        output_ds = pydicom.dcmread(output_files[0])
        assert "ORIGINAL" in output_ds.ImageType, "Output file should be the primary CT"

        quarantine_files = list(quarantine_dir.rglob("*.dcm"))
        assert len(quarantine_files) == 1, "One DICOM file should be quarantined"

        quarantine_ds = pydicom.dcmread(quarantine_files[0])
        assert "DERIVED" in quarantine_ds.ImageType, "Quarantined file should be the derived CT"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_with_mapping_file_basic(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["MAPPED001", "MAPPED002"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i, acc in enumerate(["ACC001", "ACC002", "ACC003"]):
            ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002", "ACC003"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@keep()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 output files, got {len(output_files)}"
        
        accession_numbers = []
        for file in output_files:
            ds = pydicom.dcmread(file)
            accession_numbers.append(ds.AccessionNumber)
        
        assert "MAPPED001" in accession_numbers, "ACC001 should be mapped to MAPPED001"
        assert "MAPPED002" in accession_numbers, "ACC002 should be mapped to MAPPED002"
        assert "ACC003" in accession_numbers, "ACC003 should be kept (fallback to @keep())"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_with_mapping_file_multiple_tags(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["NEWACC001", "NEWACC002"],
        "PatientID": ["MRN0000", "MRN0001"],
        "New-PatientID": ["NEWMRN0000", "NEWMRN0001"],
        "StudyDate": [pd.Timestamp("2023-01-15"), pd.Timestamp("2023-02-20")],
        "New-StudyDate": [pd.Timestamp("2024-06-15"), pd.Timestamp("2024-07-20")]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i, (acc, mrn, study_date) in enumerate([
            ("ACC001", "MRN0000", "20230115"),
            ("ACC002", "MRN0001", "20230220"),
            ("ACC999", "MRN0999", "20231231")
        ]):
            ds = _create_test_dicom(acc, mrn, f"Patient{i}", "CT", "3.0")
            ds.StudyDate = study_date
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002", "ACC999"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@keep()</e>
<e en="T" t="00080050" n="AccessionNumber">@empty()</e>
<e en="T" t="00080020" n="StudyDate">@keep()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 output files, got {len(output_files)}"
        
        mapped_files = []
        unmapped_file = None
        
        for file in output_files:
            ds = pydicom.dcmread(file)
            if ds.AccessionNumber in ["NEWACC001", "NEWACC002"]:
                mapped_files.append(ds)
            else:
                unmapped_file = ds
        
        assert len(mapped_files) == 2, "Should have 2 mapped files"
        assert unmapped_file is not None, "Should have 1 unmapped file"
        
        for ds in mapped_files:
            if ds.AccessionNumber == "NEWACC001":
                assert ds.PatientID == "NEWMRN0000", "ACC001 PatientID should be mapped"
                assert ds.StudyDate == "20240615", "ACC001 StudyDate should be mapped"
            elif ds.AccessionNumber == "NEWACC002":
                assert ds.PatientID == "NEWMRN0001", "ACC002 PatientID should be mapped"
                assert ds.StudyDate == "20240720", "ACC002 StudyDate should be mapped"
        
        assert unmapped_file.AccessionNumber == "", "ACC999 should be empty (fallback to @empty())"
        assert unmapped_file.PatientID == "MRN0999", "MRN0999 should be kept (fallback to @keep())"
        assert unmapped_file.StudyDate == "20231231", "Unmapped study date should be kept (fallback to @keep())"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_date_format_conversion_with_mapping(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "StudyDate": [pd.Timestamp("2023-01-15"), pd.Timestamp("2023-12-31")],
        "New-StudyDate": [pd.Timestamp("2024-03-20"), pd.Timestamp("2024-11-05")]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i, study_date in enumerate(["20230115", "20231231"]):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.StudyDate = study_date
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC000", "ACC001"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080020" n="StudyDate">@keep()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 2, f"Expected 2 output files, got {len(output_files)}"
        
        study_dates = set()
        for file in output_files:
            ds = pydicom.dcmread(file)
            study_dates.add(ds.StudyDate)
        
        assert "20240320" in study_dates, "20230115 should be mapped to 20240320"
        assert "20241105" in study_dates, "20231231 should be mapped to 20241105"
        assert "20230115" not in study_dates, "Original date 20230115 should not appear"
        assert "20231231" not in study_dates, "Original date 20231231 should not appear"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_fallback_to_simple_action(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001"],
        "New-AccessionNumber": ["MAPPED001"],
        "PatientID": ["MRN0000"],
        "New-PatientID": ["NEWMRN0000"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i, (acc, mrn) in enumerate([("ACC001", "MRN0000"), ("ACC002", "MRN0001"), ("ACC003", "MRN0002")]):
            ds = _create_test_dicom(acc, mrn, f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002", "ACC003"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@keep()</e>
<e en="T" t="00080050" n="AccessionNumber">@empty()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 output files, got {len(output_files)}"
        
        for file in output_files:
            ds = pydicom.dcmread(file)
            
            if ds.AccessionNumber == "MAPPED001":
                assert ds.PatientID == "NEWMRN0000", "Mapped file should have mapped PatientID"
            else:
                assert ds.AccessionNumber == "", "Unmapped accession should be empty (fallback to @empty())"
                assert ds.PatientID in ["MRN0001", "MRN0002"], "Unmapped PatientID should be kept (fallback to @keep())"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_complex_function_quarantines(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001"],
        "New-AccessionNumber": ["MAPPED001"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i, acc in enumerate(["ACC001", "ACC002"]):
            ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 1, f"Expected 1 output file (mapped), got {len(output_files)}"
        
        ds = pydicom.dcmread(output_files[0])
        assert ds.AccessionNumber == "MAPPED001", "Only mapped file should be in output"
        
        quarantine_dir = appdata_dir / "quarantine"
        quarantined_files = list(quarantine_dir.rglob("*.dcm"))
        assert len(quarantined_files) == 1, "Unmapped file with complex function should be quarantined"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_tag_not_in_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["NEWACC001", "NEWACC002"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i, acc in enumerate(["ACC001", "ACC002", "ACC003"]):
            ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002", "ACC003"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 output files, got {len(output_files)}"
        
        accession_numbers = set()
        for file in output_files:
            ds = pydicom.dcmread(file)
            accession_numbers.add(ds.AccessionNumber)
        
        assert "NEWACC001" in accession_numbers, "ACC001 should be mapped"
        assert "NEWACC002" in accession_numbers, "ACC002 should be mapped"
        assert "ACC003" in accession_numbers, "ACC003 should be kept (not in mapping, fallback to @keep() for new tags)"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_explicit_lookup_table_overrides_mapping_file(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001"],
        "New-AccessionNumber": ["FROM_MAPPING"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    explicit_lookup_table = """AccessionNumber/ACC001 = FROM_EXPLICIT"""
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        ds = _create_test_dicom("ACC001", "MRN0000", "Patient", "CT", "3.0")
        ds.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@lookup(this,AccessionNumber,keep)</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            lookup_table=explicit_lookup_table,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 1, f"Expected 1 output file, got {len(output_files)}"
        
        ds = pydicom.dcmread(output_files[0])
        assert ds.AccessionNumber == "FROM_EXPLICIT", "Explicit lookup_table should override mapping_file_path"
        assert ds.AccessionNumber != "FROM_MAPPING", "Mapping file should be ignored when explicit lookup_table is provided"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_saves_failed_queries_csv(tmp_path):
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
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001", "ACC999"],
            "PatientID": ["MRN001", "MRN999"]
        })
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(
            str(query_file),
            acc_col="AccessionNumber",
            mrn_col="PatientID"
        )
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""
        
        result = imagedeid_pacs(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 1
        assert len(result["failed_query_indices"]) == 1
        
        csv_path = appdata_dir / "failed_queries.csv"
        assert csv_path.exists(), "failed_queries.csv should exist"
        
        df = pd.read_csv(csv_path)
        assert len(df) == 1
        assert list(df.columns) == ["Accession Number", "MRN", "Failure Reason"]
        assert df.loc[0, "Accession Number"] == "ACC999"
        assert df.loc[0, "MRN"] == "MRN999"
        assert df.loc[0, "Failure Reason"] == "Failed to find images"
    
    finally:
        orthanc.stop()


