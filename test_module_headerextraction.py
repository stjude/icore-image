import json
import logging
import os
from pathlib import Path

import pandas as pd
from pydicom.uid import generate_uid

from test_utils import _create_test_dicom
from module_headerextraction import headerextraction


logging.basicConfig(level=logging.INFO)


def test_headerextraction_basic(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds.PatientSex = "M"
    ds.StudyDescription = "Test Study"
    filepath = input_dir / "f001.dcm"
    ds.save_as(str(filepath), write_like_original=False)
    
    result = headerextraction(
        input_dir=str(input_dir),
        output_dir=str(output_dir)
    )
    
    metadata_path = output_dir / "metadata.xlsx"
    assert metadata_path.exists(), "metadata.xlsx should exist"
    
    df = pd.read_excel(metadata_path)
    assert len(df) == 1, "Should have 1 study"
    
    assert "AccessionNumber" in df.columns
    assert "StudyInstanceUID" in df.columns
    assert "PatientName" in df.columns
    assert "PatientID" in df.columns
    assert "PatientSex" in df.columns
    assert "Manufacturer" in df.columns
    assert "ManufacturerModelName" in df.columns
    assert "StudyDescription" in df.columns
    assert "StudyDate" in df.columns
    assert "SeriesInstanceUID" in df.columns
    assert "SOPClassUID" in df.columns
    assert "Modality" in df.columns
    assert "SeriesDescription" in df.columns
    assert "Rows" in df.columns
    assert "Columns" in df.columns
    assert "InstitutionName" in df.columns
    assert "StudyTime" in df.columns
    
    assert df.loc[0, "AccessionNumber"] == "ACC001"
    assert df.loc[0, "PatientID"] == "MRN001"
    assert df.loc[0, "PatientName"] == "Smith^John"
    assert df.loc[0, "PatientSex"] == "M"
    assert df.loc[0, "Modality"] == "CT"
    assert df.loc[0, "StudyDescription"] == "Test Study"
    assert str(df.loc[0, "Rows"]) == "64"
    assert str(df.loc[0, "Columns"]) == "64"
    
    assert result["num_files_processed"] == 1


def test_headerextraction_extract_all_headers(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds.PatientSex = "M"
    ds.StudyDescription = "Test Study"
    ds.InstanceNumber = 1
    filepath = input_dir / "f001.dcm"
    ds.save_as(str(filepath), write_like_original=False)
    
    result = headerextraction(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        extract_all_headers=True
    )
    
    metadata_path = output_dir / "metadata.xlsx"
    assert metadata_path.exists(), "metadata.xlsx should exist"
    
    df = pd.read_excel(metadata_path)
    assert len(df) == 1, "Should have 1 study"
    
    assert "AccessionNumber" in df.columns
    assert "StudyInstanceUID" in df.columns
    assert "PatientName" in df.columns
    assert "PatientID" in df.columns
    assert "Modality" in df.columns
    assert "InstanceNumber" in df.columns
    
    assert len(df.columns) > 17, "Should have more than default headers when extract_all_headers=True"
    
    assert df.loc[0, "AccessionNumber"] == "ACC001"
    assert df.loc[0, "PatientID"] == "MRN001"
    assert df.loc[0, "Modality"] == "CT"
    assert str(df.loc[0, "InstanceNumber"]) == "1"
    
    assert result["num_files_processed"] == 1


def test_headerextraction_study_level_aggregation(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    from pydicom.uid import generate_uid
    
    study_uid_1 = generate_uid()
    study_uid_2 = generate_uid()
    
    ds1 = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds1.StudyInstanceUID = study_uid_1
    ds1.SeriesInstanceUID = generate_uid()
    ds1.InstanceNumber = 1
    filepath1 = input_dir / "f001.dcm"
    ds1.save_as(str(filepath1), write_like_original=False)
    
    ds2 = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds2.StudyInstanceUID = study_uid_1
    ds2.SeriesInstanceUID = generate_uid()
    ds2.InstanceNumber = 2
    filepath2 = input_dir / "f002.dcm"
    ds2.save_as(str(filepath2), write_like_original=False)
    
    ds3 = _create_test_dicom("ACC002", "MRN002", "Doe^Jane", "MR", "3.0")
    ds3.StudyInstanceUID = study_uid_2
    ds3.SeriesInstanceUID = generate_uid()
    ds3.InstanceNumber = 1
    filepath3 = input_dir / "f003.dcm"
    ds3.save_as(str(filepath3), write_like_original=False)
    
    result = headerextraction(
        input_dir=str(input_dir),
        output_dir=str(output_dir)
    )
    
    metadata_path = output_dir / "metadata.xlsx"
    assert metadata_path.exists(), "metadata.xlsx should exist"
    
    df = pd.read_excel(metadata_path)
    assert len(df) == 2, "Should have 2 studies (aggregated from 3 files)"
    
    study_1_rows = df[df["StudyInstanceUID"] == study_uid_1]
    assert len(study_1_rows) == 1, "Study 1 should appear only once"
    assert study_1_rows.iloc[0]["AccessionNumber"] == "ACC001"
    assert study_1_rows.iloc[0]["PatientID"] == "MRN001"
    
    study_2_rows = df[df["StudyInstanceUID"] == study_uid_2]
    assert len(study_2_rows) == 1, "Study 2 should appear only once"
    assert study_2_rows.iloc[0]["AccessionNumber"] == "ACC002"
    assert study_2_rows.iloc[0]["PatientID"] == "MRN002"
    
    assert result["num_files_processed"] == 3
    assert result["num_studies"] == 2



def test_headerextraction_concatenates_multiple_values(tmp_path):
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    
    input_dir.mkdir()
    output_dir.mkdir()
    
    study_uid = generate_uid()
    series_uid_1 = generate_uid()
    series_uid_2 = generate_uid()
    
    ds1 = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds1.StudyInstanceUID = study_uid
    ds1.SeriesInstanceUID = series_uid_1
    ds1.SeriesDescription = "xyz"
    ds1.SeriesNumber = 1
    filepath1 = input_dir / "f001.dcm"
    ds1.save_as(str(filepath1), write_like_original=False)
    
    ds2 = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds2.StudyInstanceUID = study_uid
    ds2.SeriesInstanceUID = series_uid_2
    ds2.SeriesDescription = "abc"
    ds2.SeriesNumber = 2
    filepath2 = input_dir / "f002.dcm"
    ds2.save_as(str(filepath2), write_like_original=False)

    
    result = headerextraction(
        input_dir=str(input_dir),
        output_dir=str(output_dir)
    )
    
    metadata_path = output_dir / "metadata.xlsx"
    assert metadata_path.exists(), "metadata.xlsx should exist"
    
    df = pd.read_excel(metadata_path)
    assert len(df) == 1, "Should have 1 study (aggregated from 2 series)"
    
    series_instance_uids = str(df.loc[0, "SeriesInstanceUID"])
    series_descriptions = str(df.loc[0, "SeriesDescription"])
    
    series_uids_list = json.loads(series_instance_uids)
    series_desc_list = json.loads(series_descriptions)
    
    assert len(series_uids_list) == 2, "Should have 2 series UIDs in JSON array"
    assert len(series_desc_list) == 2, "Should have 2 series descriptions in JSON array"
    assert series_uid_1 in series_uids_list, "Should contain first series UID"
    assert series_uid_2 in series_uids_list, "Should contain second series UID"
    assert "xyz" in series_desc_list, "Should contain first series description"
    assert "abc" in series_desc_list, "Should contain second series description"
    
    assert result["num_files_processed"] == 2
    assert result["num_studies"] == 1

