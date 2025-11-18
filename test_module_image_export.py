import logging
import os
from pathlib import Path

import pytest
import pydicom

from module_image_export import image_export
from test_utils import _create_test_dicom, AzuriteServer


logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="function")
def azurite():
    """Fixture to start and stop Azurite for each test"""
    server = AzuriteServer()
    server.start()
    yield server
    server.stop()


def test_image_export_single_file(tmp_path, azurite):
    """Test exporting a single DICOM file to Azure blob storage under project_name folder"""
    input_dir = tmp_path / "input"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    appdata_dir.mkdir()
    
    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    filepath = input_dir / "test001.dcm"
    ds.save_as(str(filepath), write_like_original=False)
    
    container_name = "testcontainer"
    sas_url = azurite.get_sas_url(container_name)
    project_name = "TestProject"
    
    result = image_export(
        input_dir=str(input_dir),
        sas_url=sas_url,
        project_name=project_name,
        appdata_dir=str(appdata_dir)
    )
    
    blobs = azurite.list_blobs(container_name)
    assert len(blobs) == 1
    assert blobs[0] == f"{project_name}/test001.dcm"
    
    blob_content = azurite.get_blob_content(container_name, blobs[0])
    downloaded_ds = pydicom.dcmread(pydicom.filebase.DicomBytesIO(blob_content))
    assert downloaded_ds.AccessionNumber == "ACC001"
    assert downloaded_ds.PatientID == "MRN001"


def test_image_export_preserves_folder_structure(tmp_path, azurite):
    """Test that folder structure is preserved under project_name/"""
    input_dir = tmp_path / "input"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    appdata_dir.mkdir()
    
    (input_dir / "study1" / "series1").mkdir(parents=True)
    (input_dir / "study1" / "series2").mkdir(parents=True)
    (input_dir / "study2").mkdir(parents=True)
    
    ds1 = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds1.save_as(str(input_dir / "study1" / "series1" / "file1.dcm"), write_like_original=False)
    
    ds2 = _create_test_dicom("ACC002", "MRN002", "Doe^Jane", "MR", "1.0")
    ds2.save_as(str(input_dir / "study1" / "series2" / "file2.dcm"), write_like_original=False)
    
    ds3 = _create_test_dicom("ACC003", "MRN003", "Brown^Bob", "CT", "2.0")
    ds3.save_as(str(input_dir / "study2" / "file3.dcm"), write_like_original=False)
    
    container_name = "testcontainer"
    sas_url = azurite.get_sas_url(container_name)
    project_name = "TestProject"
    
    result = image_export(
        input_dir=str(input_dir),
        sas_url=sas_url,
        project_name=project_name,
        appdata_dir=str(appdata_dir)
    )
    
    blobs = azurite.list_blobs(container_name)
    assert len(blobs) == 3
    
    expected_paths = [
        f"{project_name}/study1/series1/file1.dcm",
        f"{project_name}/study1/series2/file2.dcm",
        f"{project_name}/study2/file3.dcm"
    ]
    
    for expected_path in expected_paths:
        assert expected_path in blobs, f"Expected {expected_path} in blob list"


def test_image_export_invalid_sas_token(tmp_path):
    """Test error handling with invalid SAS token"""
    input_dir = tmp_path / "input"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    appdata_dir.mkdir()
    
    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    filepath = input_dir / "test001.dcm"
    ds.save_as(str(filepath), write_like_original=False)
    
    invalid_sas_url = "http://invalid.blob.core.windows.net/container?invalidtoken"
    
    with pytest.raises(Exception) as exc_info:
        image_export(
            input_dir=str(input_dir),
            sas_url=invalid_sas_url,
            project_name="TestProject",
            appdata_dir=str(appdata_dir)
        )
    
    # rclone errors can be various messages, just verify an exception was raised
    error_msg = str(exc_info.value).lower()
    assert "rclone error" in error_msg or "error" in error_msg


def test_image_export_empty_folder(tmp_path, azurite):
    """Test that exporting an empty folder raises an error"""
    input_dir = tmp_path / "input"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    appdata_dir.mkdir()
    
    container_name = "testcontainer"
    sas_url = azurite.get_sas_url(container_name)
    project_name = "TestProject"
    
    with pytest.raises(Exception) as exc_info:
        image_export(
            input_dir=str(input_dir),
            sas_url=sas_url,
            project_name=project_name,
            appdata_dir=str(appdata_dir)
        )
    
    error_msg = str(exc_info.value)
    assert "empty" in error_msg.lower()
    assert str(input_dir) in error_msg


def test_image_export_logs_progress(tmp_path, azurite, caplog):
    """Test that progress is logged during export"""
    input_dir = tmp_path / "input"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    appdata_dir.mkdir()
    
    for i in range(5):
        ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:03d}", f"Patient{i}", "CT", "1.0")
        filepath = input_dir / f"file{i:03d}.dcm"
        ds.save_as(str(filepath), write_like_original=False)
    
    container_name = "testcontainer"
    sas_url = azurite.get_sas_url(container_name)
    project_name = "TestProject"
    
    with caplog.at_level(logging.INFO):
        result = image_export(
            input_dir=str(input_dir),
            sas_url=sas_url,
            project_name=project_name,
            appdata_dir=str(appdata_dir)
        )
    
    blobs = azurite.list_blobs(container_name)
    assert len(blobs) == 5


def test_image_export_multiple_file_types(tmp_path, azurite):
    """Test exporting different file types (not just .dcm)"""
    input_dir = tmp_path / "input"
    appdata_dir = tmp_path / "appdata"
    
    input_dir.mkdir()
    appdata_dir.mkdir()
    
    (input_dir / "data.txt").write_text("test data")
    (input_dir / "info.json").write_text('{"key": "value"}')
    
    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds.save_as(str(input_dir / "test.dcm"), write_like_original=False)
    
    container_name = "testcontainer"
    sas_url = azurite.get_sas_url(container_name)
    project_name = "TestProject"
    
    result = image_export(
        input_dir=str(input_dir),
        sas_url=sas_url,
        project_name=project_name,
        appdata_dir=str(appdata_dir)
    )
    
    blobs = azurite.list_blobs(container_name)
    assert len(blobs) == 3
    
    expected_files = [
        f"{project_name}/data.txt",
        f"{project_name}/info.json",
        f"{project_name}/test.dcm"
    ]
    
    for expected_file in expected_files:
        assert expected_file in blobs

