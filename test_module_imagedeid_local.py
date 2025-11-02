import logging
import os
import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pydicom

from module_imagedeid_local import imagedeid_local
from test_utils import _create_test_dicom, Fixtures


logging.basicConfig(level=logging.INFO)


def test_imagedeid_local(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    for i in range(3):
        ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "CT", "0.5")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    for i in range(3, 6):
        ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "CT", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    for i in range(6, 9):
        ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Smith^John{i}", "MR", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
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
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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
    
    assert result["num_images_saved"] == 3


def test_imagedeid_local_pixel(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    for i in range(10):
        ds = Fixtures.create_minimal_dicom(
            patient_id=f"MRN{i:04d}",
            patient_name=f"Smith^John{i}",
            accession=f"ACC{i:03d}",
            study_date="20250101",
            modality="CT"
        )
        ds.InstitutionName = "Test Hospital"
        ds.ReferringPhysicianName = "Dr. Referring"
        ds.Manufacturer = "TestManufacturer"
        ds.ManufacturerModelName = "TestModel"
        ds.SeriesNumber = (i % 2) + 1
        ds.InstanceNumber = i + 1
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.Rows = 512
        ds.Columns = 512
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelData = np.random.randint(0, 4096, (512, 512), dtype=np.uint16).tobytes()
        
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080080" n="InstitutionName">@remove()</e>
<e en="T" t="00080090" n="ReferringPhysicianName">@remove()</e>
<e en="T" t="00080070" n="Manufacturer">@keep()</e>
<e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        deid_pixels=True
    )
    
    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 10, f"Expected 10 output files, got {len(output_files)}"
    
    for file in output_files:
        ds = pydicom.dcmread(file)
        
        assert ds.PatientName == "", "PatientName should be empty"
        assert ds.PatientID == "", "PatientID should be empty"
        assert not hasattr(ds, 'InstitutionName') or ds.InstitutionName == "", "InstitutionName should be removed"
        assert not hasattr(ds, 'ReferringPhysicianName') or ds.ReferringPhysicianName == "", "ReferringPhysicianName should be removed"
        assert ds.Manufacturer == "TestManufacturer", "Manufacturer should be kept"
        assert ds.ManufacturerModelName == "TestModel", "ManufacturerModelName should be kept"
        assert ds.Modality == "CT", "Modality should be kept"
    
    metadata_path = appdata_dir / "metadata.csv"
    assert metadata_path.exists(), "metadata.csv should exist"
    
    deid_metadata_path = appdata_dir / "deid_metadata.csv"
    assert deid_metadata_path.exists(), "deid_metadata.csv should exist"
    
    linker_path = appdata_dir / "linker.csv"
    assert linker_path.exists(), "linker.csv should exist"
    
    assert result["num_images_saved"] == 10


def test_continuous_audit_log_saving(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    for i in range(5):
        ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
    
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
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_imagedeid_failures_reported(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script
    )
    
    assert result["num_images_saved"] == 0, "No images should be saved from empty directory"
    assert result["num_images_quarantined"] == 0, "No images should be quarantined from empty directory"

