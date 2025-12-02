import logging
import os
import time
from pathlib import Path

import pandas as pd
import pydicom
import pytest

from test_utils import _create_test_dicom, _upload_dicom_to_orthanc, AzuriteServer, OrthancServer
from utils import PacsConfiguration, Spreadsheet


logging.basicConfig(level=logging.INFO)


def test_imagedeidexport_basic_workflow(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    azurite = AzuriteServer()
    azurite.start()
    
    try:
        ds1 = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        ds1.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds1, orthanc)
        
        ds2 = _create_test_dicom("ACC002", "MRN002", "Patient2", "CT", "3.0")
        ds2.InstanceNumber = 2
        _upload_dicom_to_orthanc(ds2, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
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
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        result = imagedeidexport(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 2
        assert result["num_images_exported"] == 2
        
        blobs = azurite.list_blobs(container_name)
        assert len(blobs) == 2
        for blob in blobs:
            assert blob.startswith(f"{project_name}/")
            assert blob.endswith(".dcm")
        
        metadata_files = ["metadata.xlsx", "deid_metadata.xlsx", "linker.xlsx"]
        for metadata_file in metadata_files:
            metadata_path = appdata_dir / metadata_file
            assert metadata_path.exists(), f"{metadata_file} should exist in appdata"
        
        quarantine_dir = appdata_dir / "quarantine"
        assert quarantine_dir.exists()
        
        dcm_files_in_output = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files_in_output) == 2, "Deidentified DICOM files should be preserved in output_dir"
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_imagedeidexport_preserves_metadata_and_dicoms(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    azurite = AzuriteServer()
    azurite.start()
    
    try:
        ds = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        ds.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001"]})
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
<e en="T" t="00080050" n="AccessionNumber">@hashPtID(@UID(),13)</e>
</script>"""
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        result = imagedeidexport(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        metadata_files = ["metadata.xlsx", "deid_metadata.xlsx", "linker.xlsx"]
        for metadata_file in metadata_files:
            metadata_path = appdata_dir / metadata_file
            assert metadata_path.exists(), f"{metadata_file} should be preserved in appdata"
            assert metadata_path.stat().st_size > 0, f"{metadata_file} should not be empty"
        
        dcm_files_in_output = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files_in_output) == 1, "Deidentified DICOM files should be preserved in output_dir"
        
        quarantine_dir = appdata_dir / "quarantine"
        if quarantine_dir.exists():
            quarantine_files = list(quarantine_dir.rglob("*.dcm"))
            for qfile in quarantine_files:
                assert "quarantine" in str(qfile), "Quarantined files should be in quarantine subdirectory"
        
        blobs = azurite.list_blobs(container_name)
        assert len(blobs) == 1, "Exactly 1 DICOM file should be uploaded to Azure"
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_imagedeidexport_handles_pacs_failures(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    azurite = AzuriteServer()
    azurite.start()
    
    try:
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        invalid_pacs_config = PacsConfiguration(
            host="invalid-host.local",
            port=99999,
            aet="INVALID"
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        result = imagedeidexport(
            pacs_list=[invalid_pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 0
        assert result["num_images_exported"] == 0
        assert len(result["failed_query_indices"]) == 2
        
        blobs = azurite.list_blobs(container_name)
        assert len(blobs) == 0, "No files should be uploaded when PACS queries fail"
    
    finally:
        azurite.stop()


def test_imagedeidexport_handles_export_failures(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    try:
        ds = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        ds.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
        
        invalid_sas_url = "http://invalid-storage.blob.core.windows.net/container?invalidtoken"
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        with pytest.raises(Exception) as exc_info:
            imagedeidexport(
                pacs_list=[pacs_config],
                query_spreadsheet=query_spreadsheet,
                application_aet="TEST_AET",
                sas_url=invalid_sas_url,
                project_name=project_name,
                output_dir=str(output_dir),
                appdata_dir=str(appdata_dir),
                anonymizer_script=anonymizer_script,
                apply_default_filter_script=False
            )
        
        error_msg = str(exc_info.value).lower()
        assert "rclone" in error_msg or "error" in error_msg
    
    finally:
        orthanc.stop()


def test_imagedeidexport_with_filter_script(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    azurite = AzuriteServer()
    azurite.start()
    
    try:
        for i, slice_thickness in enumerate(["0.5", "3.0", "5.0"]):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", slice_thickness)
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC000", "ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
        
        filter_script = 'SliceThickness.isGreaterThan("1") * SliceThickness.isLessThan("5")'
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        result = imagedeidexport(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            filter_script=filter_script,
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 3
        assert result["num_images_exported"] == 1
        assert result["num_images_quarantined"] == 2
        
        blobs = azurite.list_blobs(container_name)
        assert len(blobs) == 1, "Only 1 file matching filter should be exported"
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_imagedeidexport_with_mapping_file(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    mapping_file = tmp_path / "mapping.xlsx"
    
    df_mapping = pd.DataFrame({
        "AccessionNumber": ["ACC001", "ACC002"],
        "New-AccessionNumber": ["MAPPED001", "MAPPED002"]
    })
    df_mapping.to_excel(mapping_file, index=False)
    
    orthanc = OrthancServer()
    orthanc.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc.start()
    
    azurite = AzuriteServer()
    azurite.start()
    
    try:
        for i, acc in enumerate(["ACC001", "ACC002"]):
            ds = _create_test_dicom(acc, f"MRN{i:04d}", f"Patient{i}", "CT", "3.0")
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "query.xlsx"
        query_df = pd.DataFrame({"AccessionNumber": ["ACC001", "ACC002"]})
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00080050" n="AccessionNumber">@keep()</e>
</script>"""
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        result = imagedeidexport(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            mapping_file_path=str(mapping_file),
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 2
        assert result["num_images_exported"] == 2
        
        blobs = azurite.list_blobs(container_name)
        assert len(blobs) == 2
        
        for blob in blobs:
            blob_content = azurite.get_blob_content(container_name, blob)
            ds = pydicom.dcmread(pydicom.filebase.DicomBytesIO(blob_content))
            assert ds.AccessionNumber in ["MAPPED001", "MAPPED002"]
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_imagedeidexport_with_multiple_pacs(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    orthanc1 = OrthancServer(aet="ORTHANC1")
    orthanc1.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc1.start()
    
    orthanc2 = OrthancServer(aet="ORTHANC2")
    orthanc2.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", 50001)
    orthanc2.start()
    
    azurite = AzuriteServer()
    azurite.start()
    
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
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
</script>"""
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "TestProject"
        
        from module_imagedeidexport import imagedeidexport
        
        result = imagedeidexport(
            pacs_list=pacs_configs,
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        assert result["num_studies_found"] == 4
        assert result["num_images_exported"] == 4
        
        blobs = azurite.list_blobs(container_name)
        assert len(blobs) == 4, "All 4 files from both PACS should be exported"
        
        dcm_files_in_output = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files_in_output) == 4, "Deidentified DICOM files should be preserved in output_dir"
    
    finally:
        orthanc1.stop()
        orthanc2.stop()
        azurite.stop()

