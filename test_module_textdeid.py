import os
import tempfile
import pytest
import pandas as pd
from module_textdeid import textdeid


def test_textdeid_removes_phi():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input.xlsx")
        output_dir = temp_dir
        
        phi_data = [
            "Patient: John SMITH was admitted on January 5th, 2024",
            "SSN: 123-45-6789 for records",
            "Contact at (555) 123-4567 for updates",
            "Email john.smith@example.com for correspondence",
            "Medical Record Number 1234567 on file"
        ]
        
        df = pd.DataFrame(phi_data)
        df.to_excel(input_file, index=False, header=False)
        
        textdeid(input_file, output_dir)
        
        output_file = os.path.join(output_dir, "output.xlsx")
        assert os.path.exists(output_file)
        
        result_df = pd.read_excel(output_file, header=None)
        deid_data = result_df.iloc[:, 0].tolist()
        
        assert "SMITH" not in str(deid_data)
        assert "123-45-6789" not in str(deid_data)
        assert "(555) 123-4567" not in str(deid_data)
        assert "john.smith@example.com" not in str(deid_data)
        assert "1234567" not in str(deid_data)
        
        combined_output = " ".join(str(row) for row in deid_data)
        assert "[PERSONALNAME]" in combined_output
        assert "[SSN]" in combined_output
        assert "[PHONE]" in combined_output
        assert "[EMAIL]" in combined_output
        assert "[MRN]" in combined_output

