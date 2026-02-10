"""Tests for SC/PDF filter functionality."""
import logging
import os
import time
from pathlib import Path

import numpy as np
import pydicom
import pytest
from pydicom.uid import generate_uid

from module_imagedeid_local import imagedeid_local
from test_utils import Fixtures

logging.basicConfig(level=logging.INFO)


def _create_secondary_capture_dicom(accession, patient_id, patient_name):
    """Create a Secondary Capture DICOM file."""
    ds = Fixtures.create_minimal_dicom(
        patient_id=patient_id,
        patient_name=patient_name,
        accession=accession,
        study_date="20250101",
        modality="OT"
    )
    # Secondary Capture SOP Class UID
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    ds.file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    ds.Manufacturer = "TestManufacturer"
    ds.ManufacturerModelName = "TestModel"
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


def _create_encapsulated_pdf_dicom(accession, patient_id, patient_name):
    """Create an Encapsulated PDF DICOM file."""
    ds = Fixtures.create_minimal_dicom(
        patient_id=patient_id,
        patient_name=patient_name,
        accession=accession,
        study_date="20250101",
        modality="DOC"
    )
    # Encapsulated PDF SOP Class UID
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.104.1"
    ds.file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.104.1"
    ds.Manufacturer = "TestManufacturer"
    ds.ManufacturerModelName = "TestModel"
    # PDF content would go in EncapsulatedDocument tag (0042,0011)
    ds.add_new((0x0042, 0x0011), 'OB', b'%PDF-1.4 fake pdf content')
    ds.MIMETypeOfEncapsulatedDocument = "application/pdf"
    return ds


def _create_ct_dicom(accession, patient_id, patient_name):
    """Create a normal CT DICOM file."""
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


def _create_burned_in_annotation_dicom(accession, patient_id, patient_name):
    """Create a DICOM file with BurnedInAnnotation set to YES."""
    ds = _create_ct_dicom(accession, patient_id, patient_name)
    ds.BurnedInAnnotation = "YES"
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

    # Create 1 CT (should pass), 1 SC (should be quarantined)
    ct_ds = _create_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    sc_ds = _create_secondary_capture_dicom("ACC002", "MRN002", "Doe^Jane")
    sc_ds.save_as(str(input_dir / "sc.dcm"), write_like_original=False)

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
        apply_default_filter_script=False
    )

    # CT should be in output, SC should be quarantined
    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    # Verify the output file is the CT
    for f in output_files:
        ds = pydicom.dcmread(f)
        # SC files have SOPClassUID starting with 1.2.840.10008.5.1.4.1.1.7
        assert not ds.SOPClassUID.startswith("1.2.840.10008.5.1.4.1.1.7"), "SC file should not be in output"

    # SC should be in quarantine
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

    # Create 1 CT (should pass), 1 SC (should go to custom dir)
    ct_ds = _create_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    sc_ds = _create_secondary_capture_dicom("ACC002", "MRN002", "Doe^Jane")
    sc_ds.save_as(str(input_dir / "sc.dcm"), write_like_original=False)

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
        apply_default_filter_script=False,
        sc_pdf_output_dir=str(custom_sc_dir)
    )

    # CT should be in output
    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    # SC should be in custom directory, NOT in regular quarantine
    custom_files = list(custom_sc_dir.rglob("*.dcm"))
    assert len(custom_files) >= 1, f"Expected SC file in custom dir, found {len(custom_files)}"

    # Regular quarantine should not have the SC file
    quarantine_dir = appdata_dir / "quarantine"
    if quarantine_dir.exists():
        quarantine_files = list(quarantine_dir.rglob("*.dcm"))
        # SC should NOT be in regular quarantine - it should be in custom_sc_dir
        for f in quarantine_files:
            ds = pydicom.dcmread(f)
            assert not ds.SOPClassUID.startswith("1.2.840.10008.5.1.4.1.1.7"), \
                "SC file should be in custom dir, not regular quarantine"


def test_normal_images_not_affected(tmp_path):
    """CT/MR images pass through de-identification normally."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    # Create multiple CT files
    for i in range(3):
        ct_ds = _create_ct_dicom(f"ACC00{i}", f"MRN00{i}", f"Smith^John{i}")
        ct_ds.InstanceNumber = i + 1
        ct_ds.save_as(str(input_dir / f"ct{i}.dcm"), write_like_original=False)

    time.sleep(2)

    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""

    result = imagedeid_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
        anonymizer_script=anonymizer_script,
        apply_default_filter_script=False
    )

    # All 3 CT files should be in output
    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 3, f"Expected 3 CT files in output, found {len(output_files)}"

    # All should be de-identified
    for f in output_files:
        ds = pydicom.dcmread(f)
        assert ds.PatientName == "", "PatientName should be empty"
        assert ds.PatientID == "", "PatientID should be empty"
        assert ds.Modality == "CT", "Modality should be kept as CT"


def test_encapsulated_pdf_quarantined(tmp_path):
    """Encapsulated PDF files are quarantined."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    # Create 1 CT and 1 PDF
    ct_ds = _create_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    pdf_ds = _create_encapsulated_pdf_dicom("ACC002", "MRN002", "Doe^Jane")
    pdf_ds.save_as(str(input_dir / "pdf.dcm"), write_like_original=False)

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
        apply_default_filter_script=False
    )

    # Only CT should be in output
    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    # PDF should be quarantined
    quarantine_dir = appdata_dir / "quarantine"
    quarantine_files = list(quarantine_dir.rglob("*.dcm"))
    assert len(quarantine_files) >= 1, f"Expected PDF in quarantine, found {len(quarantine_files)}"


def test_burned_in_annotation_quarantined(tmp_path):
    """Files with BurnedInAnnotation=YES are quarantined."""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    appdata_dir = tmp_path / "appdata"

    input_dir.mkdir()
    output_dir.mkdir()
    appdata_dir.mkdir()

    # Create 1 normal CT and 1 CT with BurnedInAnnotation
    ct_ds = _create_ct_dicom("ACC001", "MRN001", "Smith^John")
    ct_ds.save_as(str(input_dir / "ct.dcm"), write_like_original=False)

    burned_ds = _create_burned_in_annotation_dicom("ACC002", "MRN002", "Doe^Jane")
    burned_ds.save_as(str(input_dir / "burned.dcm"), write_like_original=False)

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
        apply_default_filter_script=False
    )

    # Only normal CT should be in output
    output_files = list(output_dir.rglob("*.dcm"))
    assert len(output_files) == 1, f"Expected 1 CT file in output, found {len(output_files)}"

    # File with BurnedInAnnotation should be quarantined
    quarantine_dir = appdata_dir / "quarantine"
    quarantine_files = list(quarantine_dir.rglob("*.dcm"))
    assert len(quarantine_files) >= 1, f"Expected burned-in file in quarantine, found {len(quarantine_files)}"
