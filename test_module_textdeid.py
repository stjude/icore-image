import os
import tempfile
import pytest
import pandas as pd
from module_textdeid import textdeid


def test_textdeid_removes_phi():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "Notes": [
                "Patient: John SMITH was admitted on January 5th, 2024",
                "Contact at (555) 123-4567 for updates",
                "Email john.smith@example.com for correspondence",
                "Medical Record Number 1234567 on file"
            ]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir)
        
        output_file = os.path.join(output_dir, "output.xlsx")
        assert os.path.exists(output_file)
        
        result_df = pd.read_excel(output_file)
        assert "Notes" in result_df.columns
        
        deid_data = result_df["Notes"].tolist()
        
        assert "SMITH" not in str(deid_data)
        assert "(555) 123-4567" not in str(deid_data)
        assert "john.smith@example.com" not in str(deid_data)
        assert "1234567" not in str(deid_data)


def test_textdeid_drops_specified_columns():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "ID": [1, 2, 3],
            "Name": ["John SMITH", "Jane DOE", "Bob JONES"],
            "DropMe": ["x", "y", "z"],
            "AlsoDropMe": ["a", "b", "c"]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir, columns_to_drop=["DropMe", "AlsoDropMe"])
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        assert "DropMe" not in result_df.columns
        assert "AlsoDropMe" not in result_df.columns
        assert "ID" in result_df.columns
        assert "Name" in result_df.columns


def test_textdeid_deids_only_specified_columns():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "ID": [1, 2, 3],
            "PatientName": ["Patient: John SMITH", "Patient: Jane ANDERSON", "Patient: Bob JONES"],
            "Diagnosis": ["Condition A", "Condition B", "Condition C"]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir, columns_to_deid=["PatientName"])
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        assert "SMITH" not in str(result_df["PatientName"].tolist())
        assert "ANDERSON" not in str(result_df["PatientName"].tolist())
        assert "[PERSONALNAME]" in str(result_df["PatientName"].tolist())
        
        assert result_df["ID"].tolist() == [1, 2, 3]
        assert "Condition A" in str(result_df["Diagnosis"].tolist())


def test_textdeid_deids_all_columns_when_none_specified():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "Name": ["John SMITH"],
            "Phone": ["(555) 123-4567"]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir, columns_to_deid=None)
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        assert "SMITH" not in str(result_df["Name"].tolist())
        assert "(555) 123-4567" not in str(result_df["Phone"].tolist())


def test_textdeid_drops_no_columns_when_none_specified():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "Col1": ["Data1"],
            "Col2": ["Data2"],
            "Col3": ["Data3"]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir, columns_to_drop=None)
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        assert "Col1" in result_df.columns
        assert "Col2" in result_df.columns
        assert "Col3" in result_df.columns


def test_textdeid_preserves_header_row():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "PatientName": ["John SMITH"],
            "MRN": ["1234567"]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir)
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        assert list(result_df.columns) == ["PatientName", "MRN"]


def test_textdeid_keeps_unspecified_columns_as_is():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "ID": ["ID-001", "ID-002"],
            "PatientName": ["Patient: John SMITH", "Patient: Jane ANDERSON"],
            "Procedure": ["X-Ray", "CT Scan"]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        textdeid(input_file, output_dir, columns_to_deid=["PatientName"])
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        assert result_df["ID"].tolist() == ["ID-001", "ID-002"]
        assert result_df["Procedure"].tolist() == ["X-Ray", "CT Scan"]
        assert "SMITH" not in str(result_df["PatientName"].tolist())
        assert "[PERSONALNAME]" in str(result_df["PatientName"].tolist())


def test_textdeid_honors_blacklist_and_whitelist():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        data = {
            "Notes": [
                "Patient seen at Boston General Hospital",
                "Radiology department processed the scan",
                "Chicago Medical Center provided care"
            ]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(input_file, index=False)
        
        to_keep_list = ["boston", "chicago"]
        to_remove_list = ["radiology"]
        
        textdeid(input_file, output_dir, to_keep_list=to_keep_list, to_remove_list=to_remove_list)
        
        output_file = os.path.join(output_dir, "output.xlsx")
        result_df = pd.read_excel(output_file)
        
        notes = result_df["Notes"].tolist()
        
        assert "Boston" in notes[0]
        assert "Chicago" in notes[2]
        
        assert "Radiology" not in notes[1]
        assert "[REDACTED]" in notes[1]

