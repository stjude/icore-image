import json
import os
import socket
import subprocess
import tempfile
import time
import uuid

import numpy as np
import pandas as pd
import pytest
import pydicom
import requests
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import generate_uid

from utils import csv_string_to_xlsx


def _cleanup_test_containers():
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


@pytest.fixture(scope="function", autouse=True)
def cleanup_docker_containers():
    _cleanup_test_containers()
    yield
    _cleanup_test_containers()


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


def test_csv_string_to_xlsx_basic(tmp_path):
    csv_string = """Name,Age,City
John,30,NYC
Jane,25,LA
Bob,35,Chicago"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    assert output_file.exists()
    
    df = pd.read_excel(output_file)
    assert len(df) == 3
    assert list(df.columns) == ["Name", "Age", "City"]
    assert df.loc[0, "Name"] == "John"
    assert df.loc[1, "Name"] == "Jane"
    assert df.loc[2, "Name"] == "Bob"
    assert str(df.loc[0, "Age"]) == "30"
    assert str(df.loc[1, "Age"]) == "25"


def test_csv_string_to_xlsx_ctp_format_with_parens(tmp_path):
    csv_string = """AccessionNumber,PatientID,Status
=("ACC123"),=("MRN456"),=("Complete")
=("ACC124"),=("MRN457"),=("Pending")"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert len(df) == 2
    assert df.loc[0, "AccessionNumber"] == "ACC123"
    assert df.loc[0, "PatientID"] == "MRN456"
    assert df.loc[0, "Status"] == "Complete"
    assert df.loc[1, "AccessionNumber"] == "ACC124"


def test_csv_string_to_xlsx_ctp_format_with_quotes(tmp_path):
    csv_string = """AccessionNumber,PatientID
="ACC123",="MRN456"
="ACC124",="MRN457" """
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert len(df) == 2
    assert df.loc[0, "AccessionNumber"] == "ACC123"
    assert df.loc[0, "PatientID"] == "MRN456"
    assert df.loc[1, "AccessionNumber"] == "ACC124"
    assert df.loc[1, "PatientID"] == "MRN457"


def test_csv_string_to_xlsx_date_yyyymmdd(tmp_path):
    csv_string = """Name,StudyDate,Value
John,20250101,100
Jane,20250102,200"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert len(df) == 2
    assert isinstance(df.loc[0, "StudyDate"], pd.Timestamp)
    assert df.loc[0, "StudyDate"].year == 2025
    assert df.loc[0, "StudyDate"].month == 1
    assert df.loc[0, "StudyDate"].day == 1


def test_csv_string_to_xlsx_date_yyyy_mm_dd(tmp_path):
    csv_string = """Name,StudyDate,Value
John,2025-01-01,100
Jane,2025-01-02,200"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert isinstance(df.loc[0, "StudyDate"], pd.Timestamp)
    assert df.loc[0, "StudyDate"].year == 2025


def test_csv_string_to_xlsx_date_mm_dd_yyyy(tmp_path):
    csv_string = """Name,BirthDate,Value
John,01/15/2025,100
Jane,02/20/2025,200"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert isinstance(df.loc[0, "BirthDate"], pd.Timestamp)
    assert df.loc[0, "BirthDate"].year == 2025
    assert df.loc[0, "BirthDate"].month == 1
    assert df.loc[0, "BirthDate"].day == 15


def test_csv_string_to_xlsx_date_case_insensitive(tmp_path):
    csv_string = """Name,STUDYDATE,SeriesDate,Value
John,20250101,20250102,100"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert isinstance(df.loc[0, "STUDYDATE"], pd.Timestamp)
    assert isinstance(df.loc[0, "SeriesDate"], pd.Timestamp)


def test_csv_string_to_xlsx_mixed_formats(tmp_path):
    from openpyxl import load_workbook
    
    csv_string = """AccessionNumber,PatientID,StudyDate,Value
=("ACC123"),="MRN456",20250101,="100"
=("ACC124"),="MRN457",20250102,="200" """
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert df.loc[0, "AccessionNumber"] == "ACC123"
    assert df.loc[0, "PatientID"] == "MRN456"
    assert isinstance(df.loc[0, "StudyDate"], pd.Timestamp)
    
    wb = load_workbook(output_file)
    ws = wb.active
    assert ws.cell(row=2, column=1).number_format == '@', "AccessionNumber should be text format"
    assert ws.cell(row=2, column=2).number_format == '@', "PatientID should be text format"
    assert ws.cell(row=2, column=4).number_format == '@', "Value should be text format"
    assert ws.cell(row=2, column=1).value == "ACC123"
    assert ws.cell(row=2, column=4).value == "100"


def test_csv_string_to_xlsx_empty_string(tmp_path):
    csv_string = ""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    assert output_file.exists()
    df = pd.read_excel(output_file)
    assert len(df) == 0


def test_csv_string_to_xlsx_only_header(tmp_path):
    csv_string = "Name,Age,City"
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    assert output_file.exists()
    df = pd.read_excel(output_file)
    assert len(df) == 0
    assert list(df.columns) == ["Name", "Age", "City"]


def test_csv_string_to_xlsx_all_cells_as_strings(tmp_path):
    from openpyxl import load_workbook
    
    csv_string = """Name,Age,Value
John,30,123
Jane,25,456"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    wb = load_workbook(output_file)
    ws = wb.active
    assert ws.cell(row=2, column=1).number_format == '@', "Name should be text format"
    assert ws.cell(row=2, column=2).number_format == '@', "Age should be text format"
    assert ws.cell(row=2, column=3).number_format == '@', "Value should be text format"


def test_csv_string_to_xlsx_invalid_date_stays_string(tmp_path):
    from datetime import datetime
    
    csv_string = """Name,StudyDate,Value
John,NotADate,100
Jane,20250102,200"""
    
    output_file = tmp_path / "output.xlsx"
    csv_string_to_xlsx(csv_string, str(output_file))
    
    df = pd.read_excel(output_file)
    assert isinstance(df.loc[0, "StudyDate"], str)
    assert df.loc[0, "StudyDate"] == "NotADate"
    assert isinstance(df.loc[1, "StudyDate"], (pd.Timestamp, datetime))


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
            "--add-host", "host.docker.internal:host-gateway",
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
