import os
import shutil
import time
from pathlib import Path

import numpy as np
import pytest
import pydicom
from pydicom.dataset import Dataset

from ctp import CTPServer


@pytest.fixture
def setup_ctp_environment(tmp_path):
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
    
    return {
        'input_dir': input_dir,
        'output_dir': output_dir,
        'tempdir': tempdir,
        'ctp_dir': ctp_dir
    }


def create_test_dicoms(input_dir, num_files=100):
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


def create_config_xml(input_dir, output_dir, tempdir, structure=""):
    config_template = """<Configuration>
    <Server
        maxThreads="20"
        port="50000">
        <Log/>
    </Server>
    <Pipeline name="test">
        <ArchiveImportService
            class="org.rsna.ctp.stdstages.ArchiveImportService"
            name="ArchiveImportService"
            fsName="DICOM Image Directory"
            root="{tempdir}/roots/ArchiveImportService"
            treeRoot="{input_dir}"
            quarantine="{tempdir}/quarantine/ArchiveImportService"
            minAge="1000"
            acceptFileObjects="no"
            acceptXmlObjects="no"
            acceptZipObjects="no"
            expandTARs="no"/>
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}/"
            structure="{structure}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="{tempdir}/quarantine/DirectoryStorageService"
            whitespaceReplacement="_" />
    </Pipeline>
</Configuration>"""
    
    config = config_template.format(
        input_dir=str(input_dir.absolute()),
        output_dir=str(output_dir.absolute()),
        tempdir=str(tempdir.absolute()),
        structure=structure
    )
    
    return config


def test_archive_import_to_directory_storage(setup_ctp_environment):
    env = setup_ctp_environment
    input_dir = env['input_dir']
    output_dir = env['output_dir']
    tempdir = env['tempdir']
    ctp_dir = env['ctp_dir']
    
    create_test_dicoms(input_dir, num_files=100)
    
    time.sleep(2)
    
    config_xml = create_config_xml(input_dir, output_dir, tempdir, structure="")
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
    
    finally:
        server.stop()


def test_kills_existing_ctp_instance(setup_ctp_environment):
    env = setup_ctp_environment
    input_dir = env['input_dir']
    output_dir = env['output_dir']
    tempdir = env['tempdir']
    ctp_dir = env['ctp_dir']
    
    create_test_dicoms(input_dir, num_files=100)
    
    time.sleep(2)
    
    config_xml = create_config_xml(input_dir, output_dir, tempdir, structure="")
    config_path = ctp_dir / "config.xml"
    config_path.write_text(config_xml)
    
    server1 = CTPServer(str(ctp_dir))
    server1.start()
    
    time.sleep(2)
    
    assert server1.process.poll() is None, "Server1 should be running"
    
    import requests
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
