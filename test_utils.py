import json
import os
import socket
import subprocess
import tempfile
import time
import uuid

import numpy as np
import pytest
import pydicom
import requests
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import generate_uid


@pytest.fixture(scope="function", autouse=True)
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

