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


def test_singleclickicore_basic_workflow(tmp_path):
    """Test basic workflow: image deid from PACS + text deid + export to Azure"""
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
        # Upload test DICOMs to PACS
        ds1 = _create_test_dicom("ACC001", "MRN001", "Patient One", "CT", "3.0")
        ds1.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds1, orthanc)
        
        ds2 = _create_test_dicom("ACC002", "MRN002", "Patient Two", "CT", "3.0")
        ds2.InstanceNumber = 2
        _upload_dicom_to_orthanc(ds2, orthanc)
        
        time.sleep(2)
        
        # Create input Excel file with PHI in text columns
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001", "ACC002"],
            "PatientName": ["John Smith was seen on January 5th, 2024", "Jane Doe at (555) 123-4567"],
            "Notes": ["Contact john.smith@example.com", "MRN 1234567 on file"]
        })
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
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        # Verify image deid results
        assert result["num_studies_found"] == 2
        assert result["num_images_exported"] == 2
        
        # Verify text deid was run
        assert result["num_rows_processed"] == 2
        
        # Verify output files exist locally
        dcm_files_in_output = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files_in_output) == 2, "Deidentified DICOM files should be in output_dir"
        
        output_xlsx = output_dir / "output.xlsx"
        assert output_xlsx.exists(), "Deidentified Excel should be in output_dir"
        
        # Verify text deid removed PHI from Excel
        result_df = pd.read_excel(output_xlsx)
        all_text = str(result_df.to_dict())
        assert "Smith" not in all_text, "PatientName PHI should be removed"
        assert "(555) 123-4567" not in all_text, "Phone should be removed"
        assert "john.smith@example.com" not in all_text, "Email should be removed"
        assert "1234567" not in all_text, "MRN should be removed"
        # Verify redaction markers are present
        assert "[PERSONALNAME]" in all_text or "[ALPHANUMERICID]" in all_text, "Should have redaction markers"
        
        # Verify blobs were uploaded to Azure
        blobs = azurite.list_blobs(container_name)
        # Should have 2 DICOM files + 1 Excel file
        dcm_blobs = [b for b in blobs if b.endswith('.dcm')]
        xlsx_blobs = [b for b in blobs if b.endswith('.xlsx')]
        assert len(dcm_blobs) == 2, "2 DICOM files should be uploaded"
        assert len(xlsx_blobs) == 1, "1 Excel file should be uploaded"
        
        # Verify all blobs are under project name
        for blob in blobs:
            assert blob.startswith(f"{project_name}/"), f"Blob {blob} should be under project folder"
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_singleclickicore_with_filter_script(tmp_path):
    """Test that filter scripts work for image deid"""
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
        # Upload DICOMs with different slice thicknesses
        for i, slice_thickness in enumerate(["0.5", "3.0", "5.0"]):
            ds = _create_test_dicom(f"ACC{i:03d}", f"MRN{i:04d}", f"Patient{i}", "CT", slice_thickness)
            ds.InstanceNumber = i + 1
            _upload_dicom_to_orthanc(ds, orthanc)
        
        time.sleep(2)
        
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC000", "ACC001", "ACC002"],
            "Notes": ["Test note 1", "Test note 2", "Test note 3"]
        })
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
        
        # Filter to only accept slice thickness between 1 and 5
        filter_script = 'SliceThickness.isGreaterThan("1") * SliceThickness.isLessThan("5")'
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "FilteredProject"
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            filter_script=filter_script,
            apply_default_filter_script=False
        )
        
        # Only 1 image should pass filter (3.0 slice thickness)
        assert result["num_studies_found"] == 3
        assert result["num_images_exported"] == 1
        assert result["num_images_quarantined"] == 2
        
        # Text deid should still process all rows
        assert result["num_rows_processed"] == 3
        
        blobs = azurite.list_blobs(container_name)
        dcm_blobs = [b for b in blobs if b.endswith('.dcm')]
        assert len(dcm_blobs) == 1, "Only 1 filtered DICOM should be exported"
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_singleclickicore_with_text_deid_columns(tmp_path):
    """Test that columns_to_deid and columns_to_drop work"""
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
        
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001"],
            "SensitiveColumn": ["This has John Smith PHI"],
            "DropMe": ["Should be dropped"],
            "KeepAsIs": ["No PHI here, keep unchanged"]
        })
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
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "ColumnTest"
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            columns_to_deid=["SensitiveColumn"],
            columns_to_drop=["DropMe"],
            apply_default_filter_script=False
        )
        
        output_xlsx = output_dir / "output.xlsx"
        result_df = pd.read_excel(output_xlsx)
        
        # DropMe column should be removed
        assert "DropMe" not in result_df.columns
        
        # KeepAsIs should be unchanged
        assert "KeepAsIs" in result_df.columns
        assert result_df.loc[0, "KeepAsIs"] == "No PHI here, keep unchanged"
        
        # SensitiveColumn should be deidentified - Smith should be removed
        assert "Smith" not in str(result_df["SensitiveColumn"].tolist())
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_singleclickicore_handles_pacs_failures(tmp_path):
    """Test that PACS failures are handled gracefully"""
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    azurite = AzuriteServer()
    azurite.start()
    
    try:
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001", "ACC002"],
            "Notes": ["Note 1", "Note 2"]
        })
        query_df.to_excel(query_file, index=False)
        
        query_spreadsheet = Spreadsheet.from_file(str(query_file), acc_col="AccessionNumber")
        
        # Invalid PACS config - will fail to connect
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
        project_name = "FailedProject"
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[invalid_pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        # PACS queries should fail
        assert result["num_studies_found"] == 0
        assert result["num_images_exported"] == 0
        assert len(result["failed_query_indices"]) == 2
        
        # But text deid should still complete
        assert result["num_rows_processed"] == 2
        
        output_xlsx = output_dir / "output.xlsx"
        assert output_xlsx.exists(), "Text deid should still produce output"
        
        # Export should still work for the text file
        blobs = azurite.list_blobs(container_name)
        xlsx_blobs = [b for b in blobs if b.endswith('.xlsx')]
        assert len(xlsx_blobs) == 1, "Excel file should still be exported"
    
    finally:
        azurite.stop()


def test_singleclickicore_handles_export_failures(tmp_path):
    """Test that export failures raise appropriate errors"""
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
        
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001"],
            "Notes": ["Test note"]
        })
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
        
        # Invalid SAS URL - export will fail
        invalid_sas_url = "http://invalid-storage.blob.core.windows.net/container?invalidtoken"
        project_name = "ExportFail"
        
        from module_singleclickicore import singleclickicore
        
        with pytest.raises(Exception) as exc_info:
            singleclickicore(
                pacs_list=[pacs_config],
                query_spreadsheet=query_spreadsheet,
                application_aet="TEST_AET",
                sas_url=invalid_sas_url,
                project_name=project_name,
                output_dir=str(output_dir),
                appdata_dir=str(appdata_dir),
                input_file=str(query_file),
                anonymizer_script=anonymizer_script,
                apply_default_filter_script=False
            )
        
        # Should raise an rclone error
        error_msg = str(exc_info.value).lower()
        assert "rclone" in error_msg or "error" in error_msg
        
        # But local files should still exist
        dcm_files = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files) == 1, "Local DICOM file should still exist"
        
        output_xlsx = output_dir / "output.xlsx"
        assert output_xlsx.exists(), "Local Excel file should still exist"
    
    finally:
        orthanc.stop()


def test_singleclickicore_skip_export_option(tmp_path):
    """Test that skip_export=True prevents Azure upload but preserves local files"""
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
        
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001"],
            "Notes": ["Test note"]
        })
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
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url="",
            project_name="TestProject",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False,
            skip_export=True
        )
        
        assert result["num_studies_found"] == 1
        assert result["num_images_exported"] == 1
        assert result["num_rows_processed"] == 1
        assert result["export_performed"] == False
        
        dcm_files = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files) == 1, "Local DICOM file should exist"
        
        output_xlsx = output_dir / "output.xlsx"
        assert output_xlsx.exists(), "Local Excel file should exist"
    
    finally:
        orthanc.stop()


def test_singleclickicore_export_enabled_by_default(tmp_path):
    """Test that export happens by default when skip_export is not specified"""
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
        
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001"],
            "Notes": ["Test note"]
        })
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
        
        container_name = "testcontainer"
        sas_url = azurite.get_sas_url(container_name)
        project_name = "DefaultExportProject"
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=sas_url,
            project_name=project_name,
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False
        )
        
        assert result["export_performed"] == True
        
        blobs = azurite.list_blobs(container_name)
        dcm_blobs = [b for b in blobs if b.endswith('.dcm')]
        xlsx_blobs = [b for b in blobs if b.endswith('.xlsx')]
        assert len(dcm_blobs) == 1, "DICOM should be uploaded to Azure"
        assert len(xlsx_blobs) == 1, "Excel should be uploaded to Azure"
    
    finally:
        orthanc.stop()
        azurite.stop()


def test_singleclickicore_skip_export_no_sas_required(tmp_path):
    """Test that SAS URL is not required when skip_export=True"""
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
        
        query_file = appdata_dir / "input.xlsx"
        query_df = pd.DataFrame({
            "AccessionNumber": ["ACC001"],
            "Notes": ["Test note"]
        })
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
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=query_spreadsheet,
            application_aet="TEST_AET",
            sas_url=None,
            project_name="NoSASProject",
            output_dir=str(output_dir),
            appdata_dir=str(appdata_dir),
            input_file=str(query_file),
            anonymizer_script=anonymizer_script,
            apply_default_filter_script=False,
            skip_export=True
        )
        
        assert result["num_studies_found"] == 1
        assert result["num_images_exported"] == 1
        assert result["export_performed"] == False
        
        dcm_files = list(output_dir.rglob("*.dcm"))
        assert len(dcm_files) == 1
        
        output_xlsx = output_dir / "output.xlsx"
        assert output_xlsx.exists()
    
    finally:
        orthanc.stop()


def test_singleclickicore_saves_failed_queries_csv(tmp_path):
    """Test that failed_queries.csv is created via imagedeid_pacs"""
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
        ds1 = _create_test_dicom("ACC001", "MRN001", "Patient1", "CT", "3.0")
        ds1.InstanceNumber = 1
        _upload_dicom_to_orthanc(ds1, orthanc)
        
        time.sleep(2)
        
        input_file = appdata_dir / "input.xlsx"
        input_df = pd.DataFrame({
            "AccessionNumber": ["ACC001", "ACC999"],
            "PatientName": ["Test Patient", "Missing Patient"]
        })
        input_df.to_excel(input_file, index=False)
        
        pacs_config = PacsConfiguration(
            host="localhost",
            port=orthanc.dicom_port,
            aet=orthanc.aet
        )
        
        anonymizer_script = """<script>
<e en="T" t="00100010" n="PatientName">@empty()</e>
<e en="T" t="00100020" n="PatientID">@empty()</e>
</script>"""
        
        from module_singleclickicore import singleclickicore
        
        result = singleclickicore(
            pacs_list=[pacs_config],
            query_spreadsheet=Spreadsheet.from_file(str(input_file), acc_col="AccessionNumber"),
            application_aet="TEST_AET",
            sas_url="https://dummy.blob.core.windows.net/container?sas",
            project_name="TestProject",
            output_dir=str(output_dir),
            input_file=str(input_file),
            appdata_dir=str(appdata_dir),
            anonymizer_script=anonymizer_script,
            columns_to_drop=["PatientName"],
            apply_default_filter_script=False,
            skip_export=True
        )
        
        assert result["num_studies_found"] == 1
        assert len(result["failed_query_indices"]) == 1
        
        csv_path = appdata_dir / "failed_queries.csv"
        assert csv_path.exists(), "failed_queries.csv should be created by imagedeid_pacs"
        
        df = pd.read_csv(csv_path)
        assert len(df) == 1
        assert df.loc[0, "Accession Number"] == "ACC999"
        assert df.loc[0, "Failure Reason"] == "Failed to find images"
    
    finally:
        orthanc.stop()


