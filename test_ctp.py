import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import numpy as np
import pytest
import pydicom
import requests
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import generate_uid

from ctp import CTPServer, CTPPipeline, PIPELINE_TEMPLATES
from dcmtk import move_study


@pytest.fixture(scope="session", autouse=True)
def cleanup_docker_containers():
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", "name=orthanc_test_", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    
    container_names = result.stdout.strip().split('\n')
    container_names = [name for name in container_names if name]
    
    for container_name in container_names:
        subprocess.run(["docker", "stop", container_name], capture_output=True)
        subprocess.run(["docker", "rm", container_name], capture_output=True)
    
    yield


class Fixtures:
    
    @staticmethod
    def create_minimal_dicom(patient_id="TEST001", patient_name="DOE^JOHN", 
                            accession="ACC001", study_date="20240101", 
                            modality="CT", **extra_tags):
        file_meta = FileMetaDataset()
        file_meta.MediaStorageSOPClassUID = '1.2.840.10008.5.1.4.1.1.2'
        file_meta.MediaStorageSOPInstanceUID = generate_uid()
        file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.1'
        file_meta.ImplementationClassUID = generate_uid()
        
        ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)
        
        ds.PatientName = patient_name
        ds.PatientID = patient_id
        ds.AccessionNumber = accession
        ds.StudyDate = study_date
        ds.StudyTime = "120000"
        ds.Modality = modality
        ds.StudyInstanceUID = generate_uid()
        ds.SeriesInstanceUID = generate_uid()
        ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
        ds.SOPClassUID = file_meta.MediaStorageSOPClassUID
        ds.SeriesNumber = 1
        ds.InstanceNumber = 1
        
        for key, value in extra_tags.items():
            setattr(ds, key, value)
        
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        
        return ds


class OrthancServer:
    
    def __init__(self, aet="ORTHANC_TEST", http_port=None, dicom_port=None):
        self.aet = aet
        self.http_port = http_port or self._get_free_port()
        self.dicom_port = dicom_port or self._get_free_port()
        self.container = None
        self.base_url = f"http://localhost:{self.http_port}"
        self.modalities = {}
        
    def _get_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port
    
    def add_modality(self, name, aet, ip, port):
        self.modalities[name] = [aet, ip, port]
    
    def start(self):
        container_name = f"orthanc_test_{uuid.uuid4().hex[:8]}"
        
        config_dir = tempfile.mkdtemp()
        config_path = os.path.join(config_dir, "orthanc.json")
        
        config = {
            "Name": "OrthancTest",
            "DicomAet": self.aet,
            "DicomPort": 4242,
            "HttpPort": 8042,
            "RemoteAccessEnabled": True,
            "AuthenticationEnabled": False,
            "DicomAlwaysAllowFind": True,
            "DicomAlwaysAllowGet": True,
            "DicomAlwaysAllowMove": True,
            "DicomCheckCalledAet": False,
            "DicomCheckModalityHost": False,
            "DicomModalities": self.modalities
        }
        
        with open(config_path, 'w') as f:
            json.dump(config, f)
        
        subprocess.run([
            "docker", "run", "-d",
            "--name", container_name,
            "-p", f"{self.http_port}:8042",
            "-p", f"{self.dicom_port}:4242",
            "-v", f"{config_dir}:/etc/orthanc:ro",
            "orthancteam/orthanc:latest"
        ], check=True, capture_output=True)
        
        self.container = container_name
        
        for _ in range(30):
            try:
                response = requests.get(f"{self.base_url}/system", timeout=1)
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(1)
    
    def upload_dicom(self, file_path):
        with open(file_path, 'rb') as f:
            response = requests.post(f"{self.base_url}/instances", files={'file': f})
        return response.status_code == 200
    
    def upload_study(self, patient_id, accession, study_date="20240101", series=None):
        if series is None:
            series = [{'modality': 'CT', 'series_description': 'SERIES1'}]
        
        study_uid = generate_uid()
        
        for series_num, series_info in enumerate(series):
            series_uid = generate_uid()
            modality = series_info.get('modality', 'CT')
            series_desc = series_info.get('series_description', f'SERIES{series_num+1}')
            series_date = series_info.get('series_date')
            
            for instance_num in range(3):
                ds = Fixtures.create_minimal_dicom(
                    patient_id=patient_id,
                    accession=accession,
                    study_date=study_date,
                    modality=modality
                )
                ds.StudyInstanceUID = study_uid
                ds.SeriesInstanceUID = series_uid
                ds.SeriesNumber = series_num + 1
                ds.InstanceNumber = instance_num + 1
                ds.SeriesDescription = series_desc
                if series_date:
                    ds.SeriesDate = series_date
                
                temp_file = tempfile.mktemp(suffix=".dcm")
                ds.save_as(temp_file)
                self.upload_dicom(temp_file)
                os.remove(temp_file)
    
    def get_study_count(self):
        response = requests.get(f"{self.base_url}/studies")
        return len(response.json())
    
    def stop(self):
        if self.container:
            subprocess.run(["docker", "stop", self.container], capture_output=True)
            subprocess.run(["docker", "rm", self.container], capture_output=True)


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


def test_ctp_pipeline_port_selection(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    source_ctp = Path(__file__).parent / "ctp"
    
    pipeline = CTPPipeline(
        source_ctp_dir=str(source_ctp),
        pipeline_type="imagecopy_local",
        input_dir=str(input_dir),
        output_dir=str(output_dir)
    )
    assert pipeline.port == 50000, "Should pick port 50000 when available"
    
    blocked_sockets = []
    try:
        for attempt in range(3):
            port = 50000 + (attempt * 10)
            dicom_port = port + 1
            
            sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock1.bind(('localhost', port))
            sock1.listen(1)
            blocked_sockets.append(sock1)
            
            sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock2.bind(('localhost', dicom_port))
            sock2.listen(1)
            blocked_sockets.append(sock2)
        
        pipeline = CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imagecopy_local",
            input_dir=str(input_dir),
            output_dir=str(output_dir)
        )
        assert pipeline.port == 50030, "Should pick port 50030 when 50000, 50010, 50020 are blocked"
        
    finally:
        for sock in blocked_sockets:
            sock.close()
    
    blocked_sockets = []
    try:
        for attempt in range(10):
            port = 50000 + (attempt * 10)
            dicom_port = port + 1
            
            sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock1.bind(('localhost', port))
            sock1.listen(1)
            blocked_sockets.append(sock1)
            
            sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock2.bind(('localhost', dicom_port))
            sock2.listen(1)
            blocked_sockets.append(sock2)
        
        with pytest.raises(RuntimeError, match="Could not find available port after 10 attempts"):
            CTPPipeline(
                source_ctp_dir=str(source_ctp),
                pipeline_type="imagecopy_local",
                input_dir=str(input_dir),
                output_dir=str(output_dir)
            )
    
    finally:
        for sock in blocked_sockets:
            sock.close()


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
        
        for file in output_files:
            assert file.suffix == ".dcm", f"File {file.name} does not have .dcm extension"
            
            parts = file.relative_to(output_dir).parts
            assert len(parts) >= 2
            assert "-CT-TEST001" in parts[0]
            assert parts[1].startswith("S")
    
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
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    create_test_dicoms(input_dir, num_files=10)
    
    time.sleep(2)
    
    source_ctp = Path(__file__).parent / "ctp"
    
    with CTPPipeline(
        source_ctp_dir=str(source_ctp),
        pipeline_type="imagecopy_local",
        input_dir=str(input_dir),
        output_dir=str(output_dir)
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
    
    anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
    
    with CTPPipeline(
        source_ctp_dir=str(source_ctp),
        pipeline_type="imagedeid_local",
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        anonymizer_script=anonymizer_script
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


def test_imagedeid_local_with_filter(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
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
        source_ctp_dir=str(source_ctp),
        pipeline_type="imagedeid_local",
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        anonymizer_script=anonymizer_script,
        filter_script='Modality.contains("CT")'
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
        source_ctp_dir=str(source_ctp),
        pipeline_type="imagedeid_local",
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        anonymizer_script=anonymizer_script
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
        source_ctp_dir=str(source_ctp),
        pipeline_type="imagecopy_local",
        input_dir=str(input_dir),
        output_dir=str(output_dir)
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
        
        with CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imageqr",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET"
        ) as pipeline:
            time.sleep(3)
            
            studies_response = requests.get(f"{orthanc.base_url}/studies")
            studies = studies_response.json()
            
            for study_id in studies:
                study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
                study_uid = study_info['MainDicomTags']['StudyInstanceUID']
                
                move_study(
                    host="localhost",
                    port=orthanc.dicom_port,
                    calling_aet="TEST_AET",
                    called_aet=orthanc.aet,
                    move_destination="TEST_AET",
                    study_uid=study_uid
                )
            
            start_time = time.time()
            timeout = 60
            
            while not pipeline.is_complete():
                if time.time() - start_time > timeout:
                    raise TimeoutError("Pipeline did not complete")
                time.sleep(1)
            
            images_dir = Path(output_dir) / "images"
            output_files = list(images_dir.rglob("*.dcm"))
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
        
        with CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imageqr",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            filter_script='Modality.contains("CT")'
        ) as pipeline:
            time.sleep(3)
            
            studies_response = requests.get(f"{orthanc.base_url}/studies")
            studies = studies_response.json()
            
            for study_id in studies:
                study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
                study_uid = study_info['MainDicomTags']['StudyInstanceUID']
                
                move_study(
                    host="localhost",
                    port=orthanc.dicom_port,
                    calling_aet="TEST_AET",
                    called_aet=orthanc.aet,
                    move_destination="TEST_AET",
                    study_uid=study_uid
                )
            
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
        
        with CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script
        ) as pipeline:
            time.sleep(3)
            
            studies_response = requests.get(f"{orthanc.base_url}/studies")
            studies = studies_response.json()
            
            for study_id in studies:
                study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
                study_uid = study_info['MainDicomTags']['StudyInstanceUID']
                
                move_study(
                    host="localhost",
                    port=orthanc.dicom_port,
                    calling_aet="TEST_AET",
                    called_aet=orthanc.aet,
                    move_destination="TEST_AET",
                    study_uid=study_uid
                )
            
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
        
        with CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script,
            filter_script='Modality.contains("CT")'
        ) as pipeline:
            time.sleep(3)
            
            studies_response = requests.get(f"{orthanc.base_url}/studies")
            studies = studies_response.json()
            
            for study_id in studies:
                study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
                study_uid = study_info['MainDicomTags']['StudyInstanceUID']
                
                move_study(
                    host="localhost",
                    port=orthanc.dicom_port,
                    calling_aet="TEST_AET",
                    called_aet=orthanc.aet,
                    move_destination="TEST_AET",
                    study_uid=study_uid
                )
            
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
        
        with CTPPipeline(
            source_ctp_dir=str(source_ctp),
            pipeline_type="imagedeid_pacs",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            application_aet="TEST_AET",
            anonymizer_script=anonymizer_script
        ) as pipeline:
            time.sleep(3)
            
            studies_response = requests.get(f"{orthanc.base_url}/studies")
            studies = studies_response.json()
            
            for study_id in studies:
                study_info = requests.get(f"{orthanc.base_url}/studies/{study_id}").json()
                study_uid = study_info['MainDicomTags']['StudyInstanceUID']
                
                move_study(
                    host="localhost",
                    port=orthanc.dicom_port,
                    calling_aet="TEST_AET",
                    called_aet=orthanc.aet,
                    move_destination="TEST_AET",
                    study_uid=study_uid
                )
            
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

