import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from cli import (
    determine_module,
    build_imageqr_params,
    build_imagedeid_pacs_params,
    build_imagedeid_local_params,
    build_textdeid_params,
    run
)
from utils import PacsConfiguration, Spreadsheet


def test_determine_module_imageqr(tmp_path):
    config = {"module": "imageqr"}
    input_dir = str(tmp_path)
    
    result = determine_module(config, input_dir)
    
    assert result == "imageqr"


def test_determine_module_imagedeid_with_input_xlsx_routes_to_pacs(tmp_path):
    config = {"module": "imagedeid"}
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    
    result = determine_module(config, input_dir)
    
    assert result == "imagedeid_pacs"


def test_determine_module_imagedeid_without_input_xlsx_routes_to_local(tmp_path):
    config = {"module": "imagedeid"}
    input_dir = str(tmp_path)
    
    result = determine_module(config, input_dir)
    
    assert result == "imagedeid_local"


def test_build_imageqr_params_builds_pacs_configuration_list(tmp_path):
    config = {
        "pacs": [
            {"ip": "192.168.1.1", "port": 104, "ae": "PACS1"},
            {"ip": "192.168.1.2", "port": 105, "ae": "PACS2"}
        ],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imageqr_params(config, input_dir, output_dir, {})
    
    assert "pacs_list" in params
    assert len(params["pacs_list"]) == 2
    assert isinstance(params["pacs_list"][0], PacsConfiguration)
    assert params["pacs_list"][0].host == "192.168.1.1"
    assert params["pacs_list"][0].port == 104
    assert params["pacs_list"][0].aet == "PACS1"
    assert params["pacs_list"][1].host == "192.168.1.2"
    assert params["pacs_list"][1].port == 105
    assert params["pacs_list"][1].aet == "PACS2"


def test_build_imageqr_params_builds_spreadsheet_with_acc_col(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    input_file = tmp_path / "input.xlsx"
    input_file.touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file') as mock_from_file:
        mock_spreadsheet = MagicMock()
        mock_from_file.return_value = mock_spreadsheet
        
        params = build_imageqr_params(config, input_dir, output_dir, {})
        
        mock_from_file.assert_called_once_with(
            str(input_file),
            acc_col="AccessionNumber",
            mrn_col=None,
            date_col=None
        )
        assert params["query_spreadsheet"] == mock_spreadsheet


def test_build_imageqr_params_builds_spreadsheet_with_mrn_and_date_cols(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "mrn_col": "PatientID",
        "date_col": "StudyDate",
        "date_window": 5
    }
    input_dir = str(tmp_path)
    input_file = tmp_path / "input.xlsx"
    input_file.touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file') as mock_from_file:
        mock_spreadsheet = MagicMock()
        mock_from_file.return_value = mock_spreadsheet
        
        params = build_imageqr_params(config, input_dir, output_dir, {})
        
        mock_from_file.assert_called_once_with(
            str(input_file),
            acc_col=None,
            mrn_col="PatientID",
            date_col="StudyDate"
        )
        assert params["query_spreadsheet"] == mock_spreadsheet


def test_build_imageqr_params_maps_config_keys_correctly(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "ctp_filters": "Modality.contains(\"CT\")",
        "date_window": 3
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imageqr_params(config, input_dir, output_dir, {})
    
    assert params["application_aet"] == "ICORE"
    assert params["output_dir"] == output_dir
    assert params["filter_script"] == "Modality.contains(\"CT\")"
    assert params["date_window_days"] == 3


def test_build_imageqr_params_date_window_defaults_to_zero(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imageqr_params(config, input_dir, output_dir, {})
    
    assert params["date_window_days"] == 0


def test_build_imageqr_params_debug_defaults_to_false(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imageqr_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is False


def test_build_imageqr_params_includes_debug_when_specified(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "debug": True
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imageqr_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is True


def test_build_imagedeid_pacs_params_builds_pacs_configuration_list(tmp_path):
    config = {
        "pacs": [
            {"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}
        ],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert "pacs_list" in params
    assert len(params["pacs_list"]) == 1
    assert isinstance(params["pacs_list"][0], PacsConfiguration)


def test_build_imagedeid_pacs_params_maps_config_keys_correctly(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "ctp_filters": "Modality.contains(\"CT\")",
        "ctp_anonymizer": "<script></script>",
        "ctp_lookup_table": "key=value",
        "date_window": 7
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["application_aet"] == "ICORE"
    assert params["output_dir"] == output_dir
    assert params["filter_script"] == "Modality.contains(\"CT\")"
    assert params["anonymizer_script"] == "<script></script>"
    assert params["lookup_table"] == "key=value"
    assert params["date_window_days"] == 7


def test_build_imagedeid_local_params_maps_config_keys_correctly(tmp_path):
    config = {
        "ctp_filters": "Modality.contains(\"CT\")",
        "ctp_anonymizer": "<script></script>",
        "ctp_lookup_table": "key=value"
    }
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["input_dir"] == input_dir
    assert params["output_dir"] == output_dir
    assert params["filter_script"] == "Modality.contains(\"CT\")"
    assert params["anonymizer_script"] == "<script></script>"
    assert params["lookup_table"] == "key=value"


def test_build_imagedeid_local_params_handles_missing_optional_params(tmp_path):
    config = {}
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["input_dir"] == input_dir
    assert params["output_dir"] == output_dir
    assert params["filter_script"] is None
    assert params["anonymizer_script"] is None
    assert params["lookup_table"] is None


def test_build_imagedeid_pacs_params_includes_deid_pixels_when_specified(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "deid_pixels": True
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["deid_pixels"] is True


def test_build_imagedeid_pacs_params_defaults_deid_pixels_to_false(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["deid_pixels"] is False


def test_build_imagedeid_pacs_params_debug_defaults_to_false(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is False


def test_build_imagedeid_pacs_params_includes_debug_when_specified(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "debug": True
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is True


def test_build_imagedeid_local_params_includes_deid_pixels_when_specified(tmp_path):
    config = {
        "ctp_filters": "Modality.contains(\"CT\")",
        "deid_pixels": True
    }
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["deid_pixels"] is True


def test_build_imagedeid_local_params_defaults_deid_pixels_to_false(tmp_path):
    config = {}
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["deid_pixels"] is False


def test_build_imagedeid_local_params_debug_defaults_to_false(tmp_path):
    config = {}
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is False


def test_build_imagedeid_local_params_includes_debug_when_specified(tmp_path):
    config = {
        "debug": True
    }
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is True


def test_build_imagedeid_pacs_params_defaults_apply_default_filter_script_to_true(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber"
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["apply_default_filter_script"] is True


def test_build_imagedeid_pacs_params_includes_apply_default_filter_script_when_false(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "apply_default_ctp_filter_script": False
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["apply_default_filter_script"] is False


def test_build_imagedeid_pacs_params_includes_apply_default_filter_script_when_true(tmp_path):
    config = {
        "pacs": [{"ip": "192.168.1.1", "port": 104, "ae": "PACS1"}],
        "application_aet": "ICORE",
        "acc_col": "AccessionNumber",
        "apply_default_ctp_filter_script": True
    }
    input_dir = str(tmp_path)
    (tmp_path / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('utils.Spreadsheet.from_file'):
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, {})
    
    assert params["apply_default_filter_script"] is True


def test_build_imagedeid_local_params_defaults_apply_default_filter_script_to_true(tmp_path):
    config = {}
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["apply_default_filter_script"] is True


def test_build_imagedeid_local_params_includes_apply_default_filter_script_when_false(tmp_path):
    config = {
        "apply_default_ctp_filter_script": False
    }
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["apply_default_filter_script"] is False


def test_build_imagedeid_local_params_includes_apply_default_filter_script_when_true(tmp_path):
    config = {
        "apply_default_ctp_filter_script": True
    }
    input_dir = str(tmp_path)
    output_dir = str(tmp_path / "output")
    
    params = build_imagedeid_local_params(config, input_dir, output_dir, {})
    
    assert params["apply_default_filter_script"] is True


def test_run_calls_imageqr_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("module: imageqr\napplication_aet: ICORE\npacs:\n  - ip: localhost\n    port: 104\n    ae: PACS1\nacc_col: AccessionNumber")
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('module_imageqr.imageqr') as mock_imageqr:
        with patch('utils.Spreadsheet.from_file'):
            mock_imageqr.return_value = {"num_studies_found": 5}
            
            result = run(str(config_path), input_dir, output_dir)
            
            mock_imageqr.assert_called_once()
            call_kwargs = mock_imageqr.call_args.kwargs
            assert call_kwargs["application_aet"] == "ICORE"
            assert call_kwargs["output_dir"] == output_dir
            assert result == {"num_studies_found": 5}


def test_run_calls_imagedeid_pacs_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("module: imagedeid\napplication_aet: ICORE\npacs:\n  - ip: localhost\n    port: 104\n    ae: PACS1\nacc_col: AccessionNumber")
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('module_imagedeid_pacs.imagedeid_pacs') as mock_imagedeid_pacs:
        with patch('utils.Spreadsheet.from_file'):
            mock_imagedeid_pacs.return_value = {"num_images_saved": 100}
            
            result = run(str(config_path), input_dir, output_dir)
            
            mock_imagedeid_pacs.assert_called_once()
            call_kwargs = mock_imagedeid_pacs.call_args.kwargs
            assert call_kwargs["application_aet"] == "ICORE"
            assert call_kwargs["output_dir"] == output_dir
            assert result == {"num_images_saved": 100}


def test_run_calls_imagedeid_local_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("module: imagedeid\nctp_filters: Modality.contains(\"CT\")")
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    output_dir = str(tmp_path / "output")
    
    with patch('module_imagedeid_local.imagedeid_local') as mock_imagedeid_local:
        mock_imagedeid_local.return_value = {"num_images_saved": 50}
        
        result = run(str(config_path), input_dir, output_dir)
        
        mock_imagedeid_local.assert_called_once()
        call_kwargs = mock_imagedeid_local.call_args.kwargs
        assert call_kwargs["input_dir"] == input_dir
        assert call_kwargs["output_dir"] == output_dir
        assert call_kwargs["filter_script"] == "Modality.contains(\"CT\")"
        assert result == {"num_images_saved": 50}


def test_build_textdeid_params_maps_config_keys_correctly(tmp_path):
    config = {
        "to_keep_list": ["medical", "term"],
        "to_remove_list": ["secret", "data"],
        "columns_to_drop": ["DropColumn1", "DropColumn2"],
        "columns_to_deid": ["PatientName", "SSN"]
    }
    input_dir = str(tmp_path)
    input_file = tmp_path / "input.xlsx"
    input_file.touch()
    output_dir = str(tmp_path / "output")
    
    params = build_textdeid_params(config, input_dir, output_dir, {})
    
    assert params["input_file"] == str(input_file)
    assert params["output_dir"] == output_dir
    assert params["to_keep_list"] == ["medical", "term"]
    assert params["to_remove_list"] == ["secret", "data"]
    assert params["columns_to_drop"] == ["DropColumn1", "DropColumn2"]
    assert params["columns_to_deid"] == ["PatientName", "SSN"]


def test_build_textdeid_params_handles_missing_optional_params(tmp_path):
    config = {}
    input_dir = str(tmp_path)
    input_file = tmp_path / "input.xlsx"
    input_file.touch()
    output_dir = str(tmp_path / "output")
    
    params = build_textdeid_params(config, input_dir, output_dir, {})
    
    assert params["input_file"] == str(input_file)
    assert params["output_dir"] == output_dir
    assert params["to_keep_list"] is None
    assert params["to_remove_list"] is None
    assert params["columns_to_drop"] is None
    assert params["columns_to_deid"] is None


def test_build_textdeid_params_debug_defaults_to_false(tmp_path):
    config = {}
    input_dir = str(tmp_path)
    input_file = tmp_path / "input.xlsx"
    input_file.touch()
    output_dir = str(tmp_path / "output")
    
    params = build_textdeid_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is False


def test_build_textdeid_params_includes_debug_when_specified(tmp_path):
    config = {
        "debug": True
    }
    input_dir = str(tmp_path)
    input_file = tmp_path / "input.xlsx"
    input_file.touch()
    output_dir = str(tmp_path / "output")
    
    params = build_textdeid_params(config, input_dir, output_dir, {})
    
    assert params["debug"] is True


def test_run_calls_textdeid_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("module: textdeid\nto_keep_list:\n  - medical\nto_remove_list:\n  - secret\ncolumns_to_drop:\n  - DropColumn\ncolumns_to_deid:\n  - PatientName")
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")
    
    with patch('module_textdeid.textdeid') as mock_textdeid:
        mock_textdeid.return_value = {"num_rows_processed": 10}
        
        result = run(str(config_path), input_dir, output_dir)
        
        mock_textdeid.assert_called_once()
        call_kwargs = mock_textdeid.call_args.kwargs
        assert call_kwargs["input_file"] == os.path.join(input_dir, "input.xlsx")
        assert call_kwargs["output_dir"] == output_dir
        assert call_kwargs["to_keep_list"] == ["medical"]
        assert call_kwargs["to_remove_list"] == ["secret"]
        assert result == {"num_rows_processed": 10}

