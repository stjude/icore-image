import os
import shutil
import socket
import tempfile
import time
from pathlib import Path

import numpy as np
import pytest
import pydicom
import requests
from pydicom.dataset import Dataset
from pydicom.uid import generate_uid

from ctp import CTPServer, CTPPipeline, PIPELINE_TEMPLATES
from dcmtk import get_study
from test_utils import cleanup_docker_containers, Fixtures, OrthancServer


def create_test_dicoms(input_dir, num_files=10):
    for i in range(num_files):
        study_uid = f"1.2.{(i % 3) + 3}"
        series_uid = f"{study_uid}.{(i % 2) + 1}"
        instance_uid = f"{series_uid}.{i+1}"
        
        ds = Dataset()
        ds.PatientName = "Test^Patient"
        ds.PatientID = "TEST001"
        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = series_uid
        ds.SOPInstanceUID = instance_uid
        ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
        ds.Modality = "CT"
        ds.StudyDate = "20250101"
        ds.StudyTime = "120000"
        ds.SeriesNumber = "1"
        ds.InstanceNumber = str(i + 1)
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.Rows = 64
        ds.Columns = 64
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        
        ds.file_meta = pydicom.dataset.FileMetaDataset()
        ds.file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
        ds.is_little_endian = True
        ds.is_implicit_VR = True
        
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)


def test_ctp_port_selection_local_pipeline(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    pipeline = CTPPipeline(
        pipeline_type="imagecopy_local",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        source_ctp_dir=str(source_ctp)
    )
    assert pipeline.port == 50000, "Should pick port 50000 when available"
    
    blocked_sockets = []
    try:
        for attempt in range(3):
            port = 50000 + (attempt * 10)
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('localhost', port))
            sock.listen(1)
            blocked_sockets.append(sock)
        
        pipeline = CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imagecopy_local",
            output_dir=str(output_dir),
            input_dir=str(input_dir)
        )
        assert pipeline.port == 50030, "Should pick port 50030 when 50000, 50010, 50020 are blocked"
        
    finally:
        for sock in blocked_sockets:
            sock.close()
    
    blocked_sockets = []
    try:
        for attempt in range(10):
            port = 50000 + (attempt * 10)
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('localhost', port))
            sock.listen(1)
            blocked_sockets.append(sock)
        
        with pytest.raises(RuntimeError, match="Could not find available port after 10 attempts"):
            CTPPipeline(
                source_ctp_dir=str(source_ctp),
                pipeline_type="imagecopy_local",
                output_dir=str(output_dir),
                input_dir=str(input_dir)
            )
    
    finally:
        for sock in blocked_sockets:
            sock.close()


def test_ctp_port_avoids_dicom_port(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    pipeline = CTPPipeline(
        pipeline_type="imagedeid_pacs",
        output_dir=str(output_dir),
        application_aet="TEST",
        dicom_port=50005,
        source_ctp_dir=str(source_ctp)
    )
    assert pipeline._dicom_port == 50005, "DICOM port should be set to 50005"
    assert pipeline.port != 50005, "CTP port should not conflict with DICOM port"
    assert pipeline.port in [50000, 50010, 50020, 50030, 50040, 50050, 50060, 50070, 50080, 50090], \
        f"CTP port should be one of the standard ports, got {pipeline.port}"


def test_parallel_local_pipelines(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir1 = tmp_path / "input1"
    input_dir2 = tmp_path / "input2"
    input_dir3 = tmp_path / "input3"
    output_dir1 = tmp_path / "output1"
    output_dir2 = tmp_path / "output2"
    output_dir3 = tmp_path / "output3"
    
    for d in [input_dir1, input_dir2, input_dir3, output_dir1, output_dir2, output_dir3]:
        d.mkdir()
    
    create_test_dicoms(input_dir1, num_files=3)
    create_test_dicoms(input_dir2, num_files=3)
    create_test_dicoms(input_dir3, num_files=3)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    with CTPPipeline(
        pipeline_type="imagecopy_local",
        output_dir=str(output_dir1),
        input_dir=str(input_dir1),
        source_ctp_dir=str(source_ctp)
    ) as pipeline1:
        with CTPPipeline(
            pipeline_type="imagecopy_local",
            output_dir=str(output_dir2),
            input_dir=str(input_dir2),
            source_ctp_dir=str(source_ctp)
        ) as pipeline2:
            with CTPPipeline(
                pipeline_type="imagecopy_local",
                output_dir=str(output_dir3),
                input_dir=str(input_dir3),
                source_ctp_dir=str(source_ctp)
            ) as pipeline3:
                ports = [pipeline1.port, pipeline2.port, pipeline3.port]
                assert len(set(ports)) == 3, f"All pipelines should have different ports, got {ports}"
                
                start_time = time.time()
                timeout = 60
                
                while not (pipeline1.is_complete() and pipeline2.is_complete() and pipeline3.is_complete()):
                    if time.time() - start_time > timeout:
                        raise TimeoutError("Pipelines did not complete")
                    time.sleep(1)
                
                assert pipeline1.metrics.files_saved == 3
                assert pipeline2.metrics.files_saved == 3
                assert pipeline3.metrics.files_saved == 3


@pytest.mark.skip(reason="No longer applicable: imagedeid_pacs now uses ArchiveImportService (file-based) instead of DicomImportService (network-based) after C-MOVE to C-GET migration")
def test_pacs_pipeline_with_custom_dicom_port(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    pipeline = CTPPipeline(
        pipeline_type="imagedeid_pacs",
        output_dir=str(output_dir),
        application_aet="TEST",
        dicom_port=11112,
        source_ctp_dir=str(source_ctp)
    )

    assert pipeline._dicom_port == 11112, "DICOM port should be set to 11112"
    assert pipeline.port != 11112, "CTP port should not equal DICOM port"
    assert pipeline.port >= 50000, "CTP port should be in expected range"


@pytest.mark.skip(reason="No longer applicable: imagedeid_pacs now uses ArchiveImportService (file-based) instead of DicomImportService (network-based) after C-MOVE to C-GET migration")
def test_pacs_pipeline_dicom_port_conflict(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir1 = tmp_path / "output1"
    output_dir2 = tmp_path / "output2"
    
    input_dir.mkdir()
    output_dir1.mkdir()
    output_dir2.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    from ctp import DicomPortInUseError
    
    with CTPPipeline(
        pipeline_type="imagedeid_pacs",
        output_dir=str(output_dir1),
        application_aet="TEST",
        dicom_port=11112,
        source_ctp_dir=str(source_ctp)
    ) as pipeline1:
        time.sleep(3)

        with pytest.raises(DicomPortInUseError, match="DICOM port 11112"):
            with CTPPipeline(
                pipeline_type="imagedeid_pacs",
                output_dir=str(output_dir2),
                application_aet="TEST",
                dicom_port=11112,
                source_ctp_dir=str(source_ctp)
            ) as pipeline2:
                pass


@pytest.mark.skip(reason="No longer applicable: imagedeid_pacs now uses ArchiveImportService (file-based) instead of DicomImportService (network-based) after C-MOVE to C-GET migration")
def test_pacs_pipeline_force_kill(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir1 = tmp_path / "output1"
    output_dir2 = tmp_path / "output2"
    
    input_dir.mkdir()
    output_dir1.mkdir()
    output_dir2.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    pipeline1 = CTPPipeline(
        pipeline_type="imagedeid_pacs",
        output_dir=str(output_dir1),
        application_aet="TEST",
        dicom_port=11112,
        source_ctp_dir=str(source_ctp)
    )
    pipeline1.__enter__()

    try:
        time.sleep(3)
        assert pipeline1.server.process.poll() is None, "First pipeline should be running"

        pipeline2 = CTPPipeline(
            pipeline_type="imagedeid_pacs",
            output_dir=str(output_dir2),
            application_aet="TEST",
            dicom_port=11112,
            force_kill_dicom_pipeline=True,
            source_ctp_dir=str(source_ctp)
        )
        pipeline2.__enter__()
        
        try:
            time.sleep(3)
            
            assert pipeline1.server.process.poll() is not None, "First pipeline should be killed"
            assert pipeline2.server.process.poll() is None, "Second pipeline should be running"
        finally:
            pipeline2.__exit__(None, None, None)
    finally:
        if pipeline1.server.process and pipeline1.server.process.poll() is None:
            pipeline1.__exit__(None, None, None)


def test_archive_import_to_directory_storage(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    tempdir = tmp_path / "temp"
    ctp_dir = tmp_path / "ctp"
    
    input_dir.mkdir()
    output_dir.mkdir()
    tempdir.mkdir()
    ctp_dir.mkdir()
    
    (tempdir / "roots").mkdir()
    (tempdir / "quarantine").mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    for item in source_ctp.iterdir():
        if item.is_dir():
            shutil.copytree(item, ctp_dir / item.name)
        else:
            shutil.copy(item, ctp_dir / item.name)
    
    create_test_dicoms(input_dir, num_files=100)
    
    time.sleep(2)
    
    config_xml = PIPELINE_TEMPLATES["imagecopy_local"].format(
        input_dir=str(input_dir.absolute()),
        output_dir=str(output_dir.absolute()),
        tempdir=str(tempdir.absolute()),
        port=50000
    )
    
    config_path = ctp_dir / "config.xml"
    config_path.write_text(config_xml)
    
    server = CTPServer(str(ctp_dir))
    
    try:
        server.start()
        
        start_time = time.time()
        timeout = 60
        
        while not server.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError(f"CTP pipeline did not complete within {timeout} seconds")
            time.sleep(1)
        
        total_files = server.metrics.files_saved + server.metrics.files_quarantined
        assert total_files == 100, f"Expected 100 files, got {total_files}"
        
        assert server.metrics.files_received == 100, f"Expected 100 files received, got {server.metrics.files_received}"
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 100, f"Expected 100 output files, found {len(output_files)}"
    
    finally:
        server.stop()


def test_kills_existing_ctp_instance(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    tempdir = tmp_path / "temp"
    ctp_dir = tmp_path / "ctp"
    
    input_dir.mkdir()
    output_dir.mkdir()
    tempdir.mkdir()
    ctp_dir.mkdir()
    
    (tempdir / "roots").mkdir()
    (tempdir / "quarantine").mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    for item in source_ctp.iterdir():
        if item.is_dir():
            shutil.copytree(item, ctp_dir / item.name)
        else:
            shutil.copy(item, ctp_dir / item.name)
    
    create_test_dicoms(input_dir, num_files=100)
    
    time.sleep(2)
    
    config_xml = PIPELINE_TEMPLATES["imagecopy_local"].format(
        input_dir=str(input_dir.absolute()),
        output_dir=str(output_dir.absolute()),
        tempdir=str(tempdir.absolute()),
        port=50000
    )
    
    config_path = ctp_dir / "config.xml"
    config_path.write_text(config_xml)
    
    server1 = CTPServer(str(ctp_dir))
    server1.start()
    
    time.sleep(2)
    
    assert server1.process.poll() is None, "Server1 should be running"
    
    response1 = requests.get("http://localhost:50000/status", timeout=2)
    assert response1.status_code == 200, "Server1 should be responding on port 50000"
    
    shutil.rmtree(tempdir / "roots", ignore_errors=True)
    shutil.rmtree(tempdir / "quarantine", ignore_errors=True)
    (tempdir / "roots").mkdir()
    (tempdir / "quarantine").mkdir()
    
    server2 = CTPServer(str(ctp_dir))
    
    try:
        server2.start()
        
        time.sleep(2)
        
        assert server1.process.poll() is not None, "Server1 should have been terminated"
        
        assert server2.process.poll() is None, "Server2 should be running"
        
        response2 = requests.get("http://localhost:50000/status", timeout=2)
        assert response2.status_code == 200, "Server2 should be responding on port 50000"
    
    finally:
        server2.stop()
        if server1.process and server1.process.poll() is None:
            server1.stop()


def test_imagecopy_local_pipeline(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    log_path = tmp_path / "ctp.log"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    create_test_dicoms(input_dir, num_files=10)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    with CTPPipeline(
        pipeline_type="imagecopy_local",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        source_ctp_dir=str(source_ctp),
        log_path=str(log_path),
        log_level="DEBUG"
    ) as pipeline:
        start_time = time.time()
        timeout = 60
        
        while not pipeline.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError("Pipeline did not complete")
            time.sleep(1)
        
        assert pipeline.metrics.files_saved + pipeline.metrics.files_quarantined == 10
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 10
        
        for file in output_files:
            assert file.suffix == ".dcm"
            
            parts = file.relative_to(output_dir).parts
            assert len(parts) >= 2
            assert "-CT-" in parts[0]
            assert parts[1].startswith("S")
    
    assert log_path.exists(), "CTP log file should exist"
    assert log_path.stat().st_size > 0, "CTP log file should contain content"
    
    log_content = log_path.read_text()
    assert "DEBUG" in log_content, "Log file should contain DEBUG level messages"


def test_imagedeid_local_pipeline(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    for i in range(10):
        ds = Fixtures.create_minimal_dicom(
            patient_id=f"P{i:03d}",
            patient_name=f"Patient{i}^Test",
            accession=f"ACC{i:03d}",
            study_date="20250101",
            modality="CT"
        )
        ds.SeriesNumber = (i % 2) + 1
        ds.InstanceNumber = i + 1
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.Rows = 64
        ds.Columns = 64
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    lookup_table = """acc/ACC000=100
acc/ACC001=101
acc/ACC002=102
acc/ACC003=103
acc/ACC004=104
acc/ACC005=105
acc/ACC006=106
acc/ACC007=107
acc/ACC008=108
acc/ACC009=109
ptid/P000=P100
ptid/P001=P101
ptid/P002=P102
ptid/P003=P103
ptid/P004=P104
ptid/P005=P105
ptid/P006=P106
"""
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@lookup(this, ptid, default, "test")</e>
<e en="T" t="00080050" n="AccessionNumber">@lookup(this, acc, default, "test")</e>
</script>"""
    
    with CTPPipeline(
        pipeline_type="imagedeid_local",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        anonymizer_script=anonymizer_script,
        lookup_table=lookup_table,
        source_ctp_dir=str(source_ctp)
    ) as pipeline:
        start_time = time.time()
        timeout = 60
        
        while not pipeline.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError("Pipeline did not complete")
            time.sleep(1)
        
        assert pipeline.metrics.files_saved + pipeline.metrics.files_quarantined == 10, \
            f"Expected 10 total files, got {pipeline.metrics.files_saved} saved + {pipeline.metrics.files_quarantined} quarantined"
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 10, f"Expected 10 output files, got {len(output_files)}"
        
        found_patient_ids = set()
        found_accessions = set()
        
        for file in output_files:
            ds = pydicom.dcmread(file)
            
            assert ds.PatientName == "", f"PatientName should be empty"
            
            found_patient_ids.add(ds.PatientID)
            found_accessions.add(ds.AccessionNumber)
            
            if ds.PatientID.startswith("P1"):
                original_index = int(ds.PatientID[1:]) - 100
                expected_accession = str(100 + original_index)
                assert ds.AccessionNumber == expected_accession, \
                    f"PatientID {ds.PatientID}: AccessionNumber should be {expected_accession}, got {ds.AccessionNumber}"
            elif ds.PatientID == "test":
                assert ds.AccessionNumber in ["107", "108", "109"], \
                    f"PatientID 'test': AccessionNumber should be 107, 108, or 109, got {ds.AccessionNumber}"
            else:
                raise AssertionError(f"Unexpected PatientID: {ds.PatientID}")
        
        expected_patient_ids = {f"P{100+i}" for i in range(7)} | {"test"}
        assert found_patient_ids == expected_patient_ids, \
            f"Expected PatientIDs {expected_patient_ids}, found {found_patient_ids}"
        
        expected_accessions = {str(100+i) for i in range(10)}
        assert found_accessions == expected_accessions, \
            f"Expected AccessionNumbers {expected_accessions}, found {found_accessions}"


def test_imagedeid_local_with_filter(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    custom_quarantine_dir = tmp_path / "custom_quarantine"
    
    input_dir.mkdir()
    output_dir.mkdir()
    custom_quarantine_dir.mkdir()
    
    for i in range(6):
        modality = "CT" if i < 3 else "MR"
        ds = Fixtures.create_minimal_dicom(
            patient_id=f"P{i:03d}",
            patient_name=f"Patient{i}^Test",
            accession=f"ACC{i:03d}",
            study_date="20250101",
            modality=modality
        )
        ds.SeriesNumber = 1
        ds.InstanceNumber = i + 1
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.Rows = 64
        ds.Columns = 64
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
    
    with CTPPipeline(
        pipeline_type="imagedeid_local",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        anonymizer_script=anonymizer_script,
        filter_script='Modality.contains("CT")',
        source_ctp_dir=str(source_ctp),
        quarantine_dir=str(custom_quarantine_dir)
    ) as pipeline:
        start_time = time.time()
        timeout = 60
        
        while not pipeline.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError("Pipeline did not complete")
            time.sleep(1)
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 3, f"Expected 3 CT files in output, got {len(output_files)}"
        
        for file in output_files:
            ds = pydicom.dcmread(file)
            assert ds.Modality == "CT", f"Only CT files should be in output, found {ds.Modality}"
            assert ds.PatientName == "", f"PatientName should be anonymized"
        
        assert pipeline.metrics.files_saved == 3, f"Expected 3 CT files saved"
        assert pipeline.metrics.files_quarantined == 3, f"Expected 3 MR files quarantined"
        
        quarantined_files = list(custom_quarantine_dir.rglob("*.dcm"))
        assert len(quarantined_files) == 3, f"Expected 3 quarantined files in custom directory, got {len(quarantined_files)}"
        
        for file in quarantined_files:
            ds = pydicom.dcmread(file)
            assert ds.Modality == "MR", f"Quarantined file should be MR, found {ds.Modality}"


def test_imagedeid_local_with_anonymizer_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
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
        ds.Rows = 64
        ds.Columns = 64
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080080" n="InstitutionName">@remove()</e>
<e en="T" t="00080090" n="ReferringPhysicianName">@remove()</e>
<e en="T" t="00080070" n="Manufacturer">@keep()</e>
<e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""
    
    with CTPPipeline(
        pipeline_type="imagedeid_local",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        anonymizer_script=anonymizer_script,
        source_ctp_dir=str(source_ctp)
    ) as pipeline:
        start_time = time.time()
        timeout = 60
        
        while not pipeline.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError("Pipeline did not complete")
            time.sleep(1)
        
        output_files = list(output_dir.rglob("*.dcm"))
        assert len(output_files) == 10
        
        for file in output_files:
            ds = pydicom.dcmread(file)
            
            assert ds.PatientName == "", "PatientName should be empty"
            assert ds.PatientID == "", "PatientID should be empty"
            assert not hasattr(ds, 'InstitutionName') or ds.InstitutionName == "", "InstitutionName should be removed"
            assert not hasattr(ds, 'ReferringPhysicianName') or ds.ReferringPhysicianName == "", "ReferringPhysicianName should be removed"
            assert ds.Manufacturer == "TestManufacturer", "Manufacturer should be kept"
            assert ds.ManufacturerModelName == "TestModel", "ManufacturerModelName should be kept"
            assert ds.Modality == "CT", "Modality should be kept"


def test_pipeline_auto_cleanup(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    for i in range(10):
        ds = Fixtures.create_minimal_dicom(
            patient_id="TEST001",
            modality="CT"
        )
        ds.SeriesNumber = 1
        ds.InstanceNumber = i + 1
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.Rows = 64
        ds.Columns = 64
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelData = np.random.randint(0, 1000, (64, 64), dtype=np.uint16).tobytes()
        
        filepath = input_dir / f"f{i+1:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    tempdir_path = None
    
    with CTPPipeline(
        pipeline_type="imagecopy_local",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        source_ctp_dir=str(source_ctp)
    ) as pipeline:
        tempdir_path = pipeline._tempdir
        assert os.path.exists(tempdir_path)
        
        start_time = time.time()
        while not pipeline.is_complete():
            if time.time() - start_time > 60:
                break
            time.sleep(1)
    
    assert not os.path.exists(tempdir_path)


def test_imageqr_pipeline(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"

    input_dir.mkdir()
    output_dir.mkdir()

    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()

    try:
        for i in range(10):
            ds = Fixtures.create_minimal_dicom(
                patient_id=f"P{i:03d}",
                accession=f"ACC{i:03d}",
                study_date="20250101",
                modality="CT"
            )
            ds.SeriesNumber = (i % 2) + 1
            ds.InstanceNumber = i + 1

            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)

        source_ctp = Path(__file__).parent / "ctp"

        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()

        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']

            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")

        input_files = list(Path(input_dir).glob("*.dcm"))
        print(f"Files retrieved to input_dir (*.dcm): {len(input_files)}")

        time.sleep(2)

        with CTPPipeline(
            pipeline_type="imageqr",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 60

            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)

            images_dir = Path(output_dir) / "images"
            output_files = list(images_dir.rglob("*.dcm"))
            print(f"Files in output images_dir: {len(output_files)}")
            assert len(output_files) >= 9
    
    finally:
        orthanc.stop()


def test_imageqr_with_filter(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i in range(6):
            modality = "CT" if i < 3 else "MR"
            ds = Fixtures.create_minimal_dicom(
                patient_id=f"P{i:03d}",
                accession=f"ACC{i:03d}",
                study_date="20250101",
                modality=modality
            )
            ds.SeriesNumber = 1
            ds.InstanceNumber = i + 1
            
            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)
        
        source_ctp = Path(__file__).parent / "ctp"

        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()

        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']

            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")

        time.sleep(2)

        with CTPPipeline(
            pipeline_type="imageqr",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            filter_script='Modality.contains("CT")',
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 60

            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            images_dir = Path(output_dir) / "images"
            output_files = list(images_dir.rglob("*.dcm"))
            assert len(output_files) == 3, f"Expected 3 CT files in output, got {len(output_files)}"
            
            for file in output_files:
                ds = pydicom.dcmread(file)
                assert ds.Modality == "CT", f"Expected CT modality, got {ds.Modality}"
            
            assert pipeline.metrics.files_quarantined == 3, f"Expected 3 MR files quarantined, got {pipeline.metrics.files_quarantined}"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_pipeline(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i in range(10):
            ds = Fixtures.create_minimal_dicom(
                patient_id=f"P{i:03d}",
                patient_name=f"Patient{i}^Test",
                accession=f"ACC{i:03d}",
                study_date="20250101",
                modality="CT"
            )
            ds.SeriesNumber = (i % 2) + 1
            ds.InstanceNumber = i + 1
            
            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)
        
        source_ctp = Path(__file__).parent / "ctp"

        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""

        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()

        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']

            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")

        time.sleep(2)

        with CTPPipeline(
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script,
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 60

            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            output_files = list(output_dir.rglob("*.dcm"))
            assert len(output_files) >= 9
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_with_filter(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i in range(6):
            modality = "CT" if i < 3 else "MR"
            ds = Fixtures.create_minimal_dicom(
                patient_id=f"P{i:03d}",
                patient_name=f"Patient{i}^Test",
                accession=f"ACC{i:03d}",
                study_date="20250101",
                modality=modality
            )
            ds.SeriesNumber = 1
            ds.InstanceNumber = i + 1
            
            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)
        
        source_ctp = Path(__file__).parent / "ctp"

        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""

        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()

        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']

            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")

        time.sleep(2)

        with CTPPipeline(
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script,
            filter_script='Modality.contains("CT")',
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 120

            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            output_files = list(output_dir.rglob("*.dcm"))
            assert len(output_files) == 3, f"Expected 3 CT files in output, got {len(output_files)}"
            
            for file in output_files:
                ds = pydicom.dcmread(file)
                assert ds.Modality == "CT", f"Only CT files should be in output, found {ds.Modality}"
                assert ds.PatientName == "", f"PatientName should be anonymized, got '{ds.PatientName}'"
            
            assert pipeline.metrics.files_saved == 3, f"Expected 3 CT files saved"
            assert pipeline.metrics.files_quarantined == 3, f"Expected 3 MR files quarantined"
            assert pipeline.metrics.files_received == 6, f"Expected 6 files received"
    
    finally:
        orthanc.stop()


def test_imagedeid_pacs_with_anonymizer_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
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
            
            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)
        
        source_ctp = Path(__file__).parent / "ctp"

        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080080" n="InstitutionName">@remove()</e>
<e en="T" t="00080090" n="ReferringPhysicianName">@remove()</e>
<e en="T" t="00080070" n="Manufacturer">@keep()</e>
<e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""

        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()

        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']

            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")

        time.sleep(2)

        with CTPPipeline(
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script,
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 60

            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            output_files = list(output_dir.rglob("*.dcm"))
            assert len(output_files) >= 9
            
            for file in output_files:
                ds = pydicom.dcmread(file)
                
                assert ds.PatientName == "", "PatientName should be empty"
                assert ds.PatientID == "", "PatientID should be empty"
                assert not hasattr(ds, 'InstitutionName') or ds.InstitutionName == "", "InstitutionName should be removed"
                assert not hasattr(ds, 'ReferringPhysicianName') or ds.ReferringPhysicianName == "", "ReferringPhysicianName should be removed"
                assert ds.Manufacturer == "TestManufacturer", "Manufacturer should be kept"
                assert ds.ManufacturerModelName == "TestModel", "ManufacturerModelName should be kept"
                assert ds.Modality == "CT", "Modality should be kept"
    
    finally:
        orthanc.stop()


def test_id_map_audit_log_extraction(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        for i in range(5):
            ds = Fixtures.create_minimal_dicom(
                patient_id=f"MRN{i:04d}",
                patient_name=f"Patient{i}^Test",
                accession=f"ACC{i:03d}",
                study_date="20250101",
                modality="CT"
            )
            ds.SeriesNumber = 1
            ds.InstanceNumber = i + 1
            
            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)
        
        source_ctp = Path(__file__).parent / "ctp"

        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""

        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()

        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']

            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")

        time.sleep(2)

        with CTPPipeline(
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script,
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 60

            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
            assert audit_log_csv is not None, "AuditLog CSV should be retrieved"
            assert len(audit_log_csv.split('\n')) > 1, "AuditLog should have data rows"
            
            deid_audit_log_csv = pipeline.get_audit_log_csv("DeidAuditLog")
            assert deid_audit_log_csv is not None, "DeidAuditLog CSV should be retrieved"
            assert len(deid_audit_log_csv.split('\n')) > 1, "DeidAuditLog should have data rows"
            
            linker_csv = pipeline.get_idmap_csv()
            assert linker_csv is not None, "IDMap linker CSV should be retrieved"
            assert len(linker_csv.split('\n')) > 1, "Linker CSV should have data rows"
            
            audit_lines = [line for line in audit_log_csv.split('\n') if line.strip()]
            deid_audit_lines = [line for line in deid_audit_log_csv.split('\n') if line.strip()]
            linker_lines = [line for line in linker_csv.split('\n') if line.strip()]
            
            assert len(audit_lines) >= 2, "AuditLog should have header + data rows"
            assert len(deid_audit_lines) >= 2, "DeidAuditLog should have header + data rows"
            assert len(linker_lines) >= 2, "Linker should have header + data rows"
            
            assert 'ACC' in audit_log_csv, "Original accession numbers should be in AuditLog"
    
    finally:
        orthanc.stop()


def test_imagedeid_local_pixel_with_anonymizer_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
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
    
    source_ctp = Path(__file__).parent / "ctp"
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080080" n="InstitutionName">@remove()</e>
<e en="T" t="00080090" n="ReferringPhysicianName">@remove()</e>
<e en="T" t="00080070" n="Manufacturer">@keep()</e>
<e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""
    
    with CTPPipeline(
        pipeline_type="imagedeid_local_pixel",
        output_dir=str(output_dir),
        input_dir=str(input_dir),
        anonymizer_script=anonymizer_script,
        source_ctp_dir=str(source_ctp)
    ) as pipeline:
        start_time = time.time()
        timeout = 60
        
        while not pipeline.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError("Pipeline did not complete")
            time.sleep(1)
        
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


def test_imagedeid_pacs_pixel_with_anonymizer_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
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
            
            temp_file = tempfile.mktemp(suffix=".dcm")
            ds.save_as(temp_file, write_like_original=False)
            orthanc.upload_dicom(temp_file)
            os.remove(temp_file)
        
        source_ctp = Path(__file__).parent / "ctp"
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
<e en="T" t="00080080" n="InstitutionName">@remove()</e>
<e en="T" t="00080090" n="ReferringPhysicianName">@remove()</e>
<e en="T" t="00080070" n="Manufacturer">@keep()</e>
<e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>
<e en="T" t="00080060" n="Modality">@keep()</e>
</script>"""
        
        studies_response = requests.get(f"{orthanc.base_url}/studies")
        studies = studies_response.json()
        
        for study_id in studies:
            study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
            study_uid = study_info['MainDicomTags']['StudyInstanceUID']
            
            result = get_study(
                host="localhost",
                port=orthanc.dicom_port,
                calling_aet="TEST_AET",
                called_aet=orthanc.aet,
                output_dir=str(input_dir),
                study_uid=study_uid
            )
            if not result["success"]:
                print(f"Warning: Failed to retrieve study {study_uid}: {result['message']}")
        
        time.sleep(2)
        
        with CTPPipeline(
            pipeline_type="imagedeid_pacs_pixel",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script,
            source_ctp_dir=str(source_ctp)
        ) as pipeline:
            start_time = time.time()
            timeout = 60
            
            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            output_files = list(output_dir.rglob("*.dcm"))
            
            assert pipeline.metrics.files_received >= 9, f"Expected at least 9 files received, got {pipeline.metrics.files_received}"
            assert pipeline.metrics.files_saved + pipeline.metrics.files_quarantined >= 9, f"Expected at least 9 files processed"
            assert len(output_files) >= 9, f"Expected at least 9 output files, got {len(output_files)}"
            
            for file in output_files:
                ds = pydicom.dcmread(file)
                
                assert ds.PatientName == "", "PatientName should be empty"
                assert ds.PatientID == "", "PatientID should be empty"
                assert not hasattr(ds, 'InstitutionName') or ds.InstitutionName == "", "InstitutionName should be removed"
                assert not hasattr(ds, 'ReferringPhysicianName') or ds.ReferringPhysicianName == "", "ReferringPhysicianName should be removed"
                assert ds.Manufacturer == "TestManufacturer", "Manufacturer should be kept"
                assert ds.ManufacturerModelName == "TestModel", "ManufacturerModelName should be kept"
                assert ds.Modality == "CT", "Modality should be kept"
    
    finally:
        orthanc.stop()


def test_ctp_server_stall_timeout(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    with pytest.raises(TimeoutError, match="CTP metrics have not changed for 10 seconds"):
        with CTPPipeline(
            pipeline_type="imagecopy_local",
            output_dir=str(output_dir),
            input_dir=str(input_dir),
            source_ctp_dir=str(source_ctp),
            stall_timeout=10
        ) as pipeline:
            start_time = time.time()
            safety_timeout = 30
            while not pipeline.is_complete():
                if time.time() - start_time > safety_timeout:
                    raise AssertionError(f"Stall timeout did not trigger within {safety_timeout} seconds")
                time.sleep(1)

