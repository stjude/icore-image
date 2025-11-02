import logging
import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pydicom

from module_imagedeid_pacs import PacsConfiguration, Spreadsheet, imagedeid_pacs
from test_ctp import Fixtures, OrthancServer


logging.basicConfig(level=logging.INFO)


def _create_test_dicom(accession, patient_id, patient_name, modality, slice_thickness):
    ds = Fixtures.create_minimal_dicom(
        patient_id=patient_id,
        patient_name=patient_name,
        accession=accession,
        study_date="20250101",
        modality=modality,
        SliceThickness=slice_thickness
    )
    ds.InstitutionName = "Test Hospital"
    ds.ReferringPhysicianName = "Dr. Referring"
    ds.Manufacturer = "TestManufacturer"
    ds.ManufacturerModelName = "TestModel"
    ds.SeriesNumber = 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.Rows = 64
    ds.Columns = 64
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
    return ds


def _upload_dicom_to_orthanc(ds, orthanc):
    temp_file = tempfile.mktemp(suffix=".dcm")
    ds.save_as(temp_file)
    orthanc.upload_dicom(temp_file)
    os.remove(temp_file)


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
            anonymizer_script=anonymizer_script
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
        
        deid_metadata_path = appdata_dir / "deid_metadata.csv"
        assert deid_metadata_path.exists(), "deid_metadata.csv should exist"
        
        deid_df = pd.read_csv(deid_metadata_path)
        assert len(deid_df) == 3, "deid_metadata.csv should have 3 rows"
        
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
        
        linker_path = appdata_dir / "linker.csv"
        assert linker_path.exists(), "linker.csv should exist"
        
        with open(linker_path, 'r') as f:
            linker_content = f.read()
            assert len(linker_content.strip()) > 0, "linker.csv should have content"
        
        linker_df = pd.read_csv(linker_path)
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
                for filename in ["metadata.csv", "deid_metadata.csv", "linker.csv"]:
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
            anonymizer_script=anonymizer_script
        )
        
        stop_monitoring.set()
        monitor_thread.join(timeout=2)
        
        with lock:
            for filename in ["metadata.csv", "deid_metadata.csv", "linker.csv"]:
                write_count = len(file_write_times.get(filename, []))
                assert write_count >= 2, f"{filename} should have been written multiple times (found {write_count} writes), indicating continuous saving"
        
        assert (appdata_dir / "metadata.csv").exists()
        assert (appdata_dir / "deid_metadata.csv").exists()
        assert (appdata_dir / "linker.csv").exists()
    
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
        anonymizer_script=anonymizer_script
    )
    
    assert len(result["failed_query_indices"]) == 3, "All 3 queries should have failed"
    assert result["failed_query_indices"] == [0, 1, 2], "Failed indices should be [0, 1, 2]"
    assert result["num_studies_found"] == 0, "No studies should be found"
    assert result["num_images_saved"] == 0, "No images should be saved"


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
            appdata_dir=str(appdata_dir)
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
            appdata_dir=str(appdata_dir)
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

