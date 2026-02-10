import logging
import os
import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pydicom

from module_imagedeid_local import imagedeid_local, _generate_lookup_table_content
from test_utils import _create_test_dicom, _create_secondary_capture_dicom, _create_encapsulated_pdf_dicom, Fixtures


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
        deid_pixels=True,
        apply_default_filter_script=False
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
    
    metadata_path = appdata_dir / "metadata.xlsx"
    assert metadata_path.exists(), "metadata.xlsx should exist"
    
    deid_metadata_path = appdata_dir / "deid_metadata.xlsx"
    assert deid_metadata_path.exists(), "deid_metadata.xlsx should exist"
    
    linker_path = appdata_dir / "linker.xlsx"
    assert linker_path.exists(), "linker.xlsx should exist"
    
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
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )
    
    assert result["num_images_saved"] == 0, "No images should be saved from empty directory"
    assert result["num_images_quarantined"] == 0, "No images should be quarantined from empty directory"


def test_imagedeid_local_apply_default_filter_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
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
    ds1.save_as(str(input_dir / "derived_ct_image.dcm"), write_like_original=False)

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
    ds2.save_as(str(input_dir / "ct_image.dcm"), write_like_original=False)

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""

    result_without_filter = imagedeid_local(
        input_dir=str(input_dir),
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
    for file in (appdata_dir / "quarantine").rglob("*.dcm"):
        file.unlink()

    result_with_filter = imagedeid_local(
        input_dir=str(input_dir),
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

    quarantine_dir = appdata_dir / "quarantine"
    quarantine_files = list(quarantine_dir.rglob("*.dcm"))
    assert len(quarantine_files) == 1, "One DICOM file should be quarantined"

    quarantine_ds = pydicom.dcmread(quarantine_files[0])
    assert "DERIVED" in quarantine_ds.ImageType, "Quarantined file should be the derived CT"


def test_generate_lookup_table_content_single_tag(tmp_path):
    mapping_file = tmp_path / "mapping.xlsx"
    
    df = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002", "ACC003"],
        "New-AccessionNumber": ["NEWACC001", "NEWACC002", "NEWACC003"]
    })
    df.to_excel(mapping_file, index=False)
    
    lookup_content = _generate_lookup_table_content(str(mapping_file))
    
    assert "AccessionNumber/ACC001 = NEWACC001" in lookup_content
    assert "AccessionNumber/ACC002 = NEWACC002" in lookup_content
    assert "AccessionNumber/ACC003 = NEWACC003" in lookup_content


def test_generate_lookup_table_content_multiple_tags(tmp_path):
    mapping_file = tmp_path / "mapping.xlsx"
    
    df = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["NEWACC001", "NEWACC002"],
        "PatientID": ["MRN001", "MRN002"],
        "New-PatientID": ["NEWMRN001", "NEWMRN002"]
    })
    df.to_excel(mapping_file, index=False)
    
    lookup_content = _generate_lookup_table_content(str(mapping_file))
    
    assert "AccessionNumber/ACC001 = NEWACC001" in lookup_content
    assert "AccessionNumber/ACC002 = NEWACC002" in lookup_content
    assert "PatientID/MRN001 = NEWMRN001" in lookup_content
    assert "PatientID/MRN002 = NEWMRN002" in lookup_content


def test_generate_lookup_table_content_with_dates(tmp_path):
    mapping_file = tmp_path / "mapping.xlsx"
    
    df = pd.DataFrame({
        "StudyDate": [pd.Timestamp("2023-01-15"), pd.Timestamp("2023-02-20")],
        "New-StudyDate": [pd.Timestamp("2024-01-15"), pd.Timestamp("2024-02-20")]
    })
    df.to_excel(mapping_file, index=False)
    
    lookup_content = _generate_lookup_table_content(str(mapping_file))
    
    assert "StudyDate/20230115 = 20240115" in lookup_content
    assert "StudyDate/20230220 = 20240220" in lookup_content


def test_imagedeid_local_with_mapping_file_basic(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["MAPPED001", "MAPPED002"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    for i, acc in enumerate(["ACC001", "ACC002", "ACC003"]):
        ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@keep()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_imagedeid_local_with_mapping_file_multiple_tags(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
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
    
    for i, (acc, mrn, study_date) in enumerate([
        ("ACC001", "MRN0000", "20230115"),
        ("ACC002", "MRN0001", "20230220"),
        ("ACC999", "MRN0999", "20231231")
    ]):
        ds = _create_test_dicom(acc, mrn, f"Patient{i}", "CT", "3.0")
        ds.StudyDate = study_date
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@keep()</e>
<e en="T" t="00080050" n="AccessionNumber">@empty()</e>
<e en="T" t="00080020" n="StudyDate">@keep()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_imagedeid_local_date_format_conversion(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "StudyDate": [pd.Timestamp("2023-01-15"), pd.Timestamp("2023-12-31")],
        "New-StudyDate": [pd.Timestamp("2024-03-20"), pd.Timestamp("2024-11-05")]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    for i, study_date in enumerate(["20230115", "20231231"]):
        ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
        ds.StudyDate = study_date
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080020" n="StudyDate">@keep()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_mapping_file_invalid_dicom_tag(tmp_path):
    mapping_file = tmp_path / "mapping.xlsx"
    
    df = pd.DataFrame({
        "InvalidTagName": ["value1"],
        "New-InvalidTagName": ["newvalue1"]
    })
    df.to_excel(mapping_file, index=False)
    
    with pytest.raises(ValueError, match="Invalid DICOM tag names"):
        _generate_lookup_table_content(str(mapping_file))


def test_mapping_file_missing_new_column(tmp_path):
    mapping_file = tmp_path / "mapping.xlsx"
    
    df = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"]
    })
    df.to_excel(mapping_file, index=False)
    
    with pytest.raises(ValueError, match="at least one New-"):
        _generate_lookup_table_content(str(mapping_file))


def test_mapping_file_inconsistent_date_types(tmp_path):
    from utils import detect_and_validate_dates
    
    df = pd.DataFrame({
        "StudyDate": [pd.Timestamp("2023-01-15"), "not a date", pd.Timestamp("2023-03-20")]
    })
    
    with pytest.raises(ValueError, match="inconsistent date types"):
        detect_and_validate_dates(df, "StudyDate")


def test_imagedeid_local_fallback_to_simple_action(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001"],
        "New-AccessionNumber": ["MAPPED001"],
        "PatientID": ["MRN0000"],
        "New-PatientID": ["NEWMRN0000"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    for i, (acc, mrn) in enumerate([("ACC001", "MRN0000"), ("ACC002", "MRN0001"), ("ACC003", "MRN0002")]):
        ds = _create_test_dicom(acc, mrn, f"Patient{i}", "CT", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@keep()</e>
<e en="T" t="00080050" n="AccessionNumber">@empty()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_imagedeid_local_complex_function_quarantines(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001"],
        "New-AccessionNumber": ["MAPPED001"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    for i, acc in enumerate(["ACC001", "ACC002"]):
        ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_imagedeid_local_tag_not_in_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["NEWACC001", "NEWACC002"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    for i, acc in enumerate(["ACC001", "ACC002", "ACC003"]):
        ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
        ds.InstanceNumber = i + 1
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def test_explicit_lookup_table_overrides_mapping_file(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    mapping_file = tmp_path / "mapping.xlsx"
    
    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001"],
        "New-AccessionNumber": ["FROM_MAPPING"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    explicit_lookup_table = """AccessionNumber/ACC001 = FROM_EXPLICIT"""
    
    ds = _create_test_dicom("ACC001", "MRN0000", "Patient", "CT", "3.0")
    ds.InstanceNumber = 1
    filepath = input_dir / "f001.dcm"
    ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@lookup(this,AccessionNumber,keep)</e>
</script>"""
    
    result = imagedeid_local(
        input_dir=str(input_dir),
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


def _create_sc_pdf_ct_dicom(accession, patient_id, patient_name):
    ds = Fixtures.create_minimal_dicom(
        patient_id=patient_id,
        patient_name=patient_name,
        accession=accession,
        study_date="20250101",
        modality="CT"
    )
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    ds.file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    ds.Manufacturer = "TestManufacturer"
    ds.ManufacturerModelName = "TestModel"
    ds.SliceThickness = "3.0"
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


def test_sc_pdf_routed_to_quarantine_by_default(tmp_path):
    """SC and PDF files go to quarantine when no custom path provided."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    ct_ds = _create_sc_pdf_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    sc_ds = _create_secondary_capture_dicom(patient_id="MRN002", patient_name="Doe^Jane", accession="ACC002")
    sc_ds.save_as(str(input_dir / "sc.dcm"))

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""

    imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )

    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    for f in output_files:
        ds = pydicom.dcmread(f)
        assert not ds.SOPClassUID.startswith("1.2.840.10008.5.1.4.1.1.7"), "SC file should not be in output"

    quarantine_dir = appdata_dir / "quarantine"
    quarantine_files = list(quarantine_dir.rglob("*.dcm"))
    assert len(quarantine_files) >= 1, f"Expected SC file in quarantine, found {len(quarantine_files)}"


def test_sc_pdf_routed_to_custom_path(tmp_path):
    """SC and PDF files go to custom path when sc_pdf_output_dir is provided."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"
    custom_sc_dir = tmp_path / "custom_sc_pdf"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()
    custom_sc_dir.mkdir()

    ct_ds = _create_sc_pdf_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    sc_ds = _create_secondary_capture_dicom(patient_id="MRN002", patient_name="Doe^Jane", accession="ACC002")
    sc_ds.save_as(str(input_dir / "sc.dcm"))

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""

    imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False,
        sc_pdf_output_dir=str(custom_sc_dir)
    )

    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    custom_files = list(custom_sc_dir.rglob("*.dcm"))
    assert len(custom_files) >= 1, f"Expected SC file in custom dir, found {len(custom_files)}"

    quarantine_dir = appdata_dir / "quarantine"
    if quarantine_dir.exists():
        quarantine_files = list(quarantine_dir.rglob("*.dcm"))
        for f in quarantine_files:
            ds = pydicom.dcmread(f)
            assert not ds.SOPClassUID.startswith("1.2.840.10008.5.1.4.1.1.7"), \
                "SC file should be in custom dir, not regular quarantine"


def test_sc_pdf_normal_images_not_affected(tmp_path):
    """CT/MR images pass through de-identification normally."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    for i in range(3):
        ct_ds = _create_sc_pdf_ct_dicom(f"ACC00{i}", f"MRN00{i}", f"Smith^John{i}")
        ct_ds.InstanceNumber = i + 1
        ct_ds.save_as(str(input_dir / f"ct{i}.dcm"), write_like_original=False)

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""

    imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )

    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 3, f"Expected 3 CT files in output, found {len(output_files)}"

    for f in output_files:
        ds = pydicom.dcmread(f)
        assert ds.PatientName == "", "PatientName should be empty"
        assert ds.PatientID == "", "PatientID should be empty"
        assert ds.Modality == "CT", "Modality should be kept as CT"


def test_sc_pdf_encapsulated_pdf_quarantined(tmp_path):
    """Encapsulated PDF files are quarantined."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    ct_ds = _create_sc_pdf_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    pdf_ds = _create_encapsulated_pdf_dicom(patient_id="MRN002", patient_name="Doe^Jane", accession="ACC002")
    pdf_ds.save_as(str(input_dir / "pdf.dcm"))

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""

    imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )

    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    quarantine_dir = appdata_dir / "quarantine"
    quarantine_files = list(quarantine_dir.rglob("*.dcm"))
    assert len(quarantine_files) >= 1, f"Expected PDF in quarantine, found {len(quarantine_files)}"


def test_sc_pdf_burned_in_annotation_quarantined(tmp_path):
    """Files with BurnedInAnnotation=YES are quarantined."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    ct_ds = _create_sc_pdf_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    burned_ds = _create_sc_pdf_ct_dicom("ACC002", "MRN002", "Doe^Jane")
    burned_ds.BurnedInAnnotation = "YES"
    burned_ds.save_as(str(input_dir / "burned.dcm"), write_like_original=False)

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""

    imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )

    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    quarantine_dir = appdata_dir / "quarantine"
    quarantine_files = list(quarantine_dir.rglob("*.dcm"))
    assert len(quarantine_files) >= 1, f"Expected burned-in file in quarantine, found {len(quarantine_files)}"


