import os
from unittest.mock import patch

from cli import determine_module, run
from config import IcoreConfig
from utils import PacsConfiguration


# ---------------------------------------------------------------------------
# determine_module
# ---------------------------------------------------------------------------


def test_determine_module_imageqr(tmp_path):
    result = determine_module(IcoreConfig(module="imageqr"), str(tmp_path))
    assert result == "imageqr"


def test_determine_module_imagedeid_with_input_xlsx_routes_to_pacs(tmp_path):
    (tmp_path / "input.xlsx").touch()
    result = determine_module(IcoreConfig(module="imagedeid"), str(tmp_path))
    assert result == "imagedeid_pacs"


def test_determine_module_imagedeid_without_input_xlsx_routes_to_local(tmp_path):
    result = determine_module(IcoreConfig(module="imagedeid"), str(tmp_path))
    assert result == "imagedeid_local"


def test_determine_module_image_export(tmp_path):
    result = determine_module(IcoreConfig(module="imageexport"), str(tmp_path))
    assert result == "imageexport"


def test_determine_module_headerextract(tmp_path):
    result = determine_module(IcoreConfig(module="headerextract"), str(tmp_path))
    assert result == "headerextract_local"


def test_determine_module_imagedeidexport(tmp_path):
    result = determine_module(IcoreConfig(module="imagedeidexport"), str(tmp_path))
    assert result == "imagedeidexport"


# ---------------------------------------------------------------------------
# IcoreConfig.model_validate — YAML alias mapping + pacs coercion
# ---------------------------------------------------------------------------


def test_config_coerces_pacs_dicts_to_pacs_configuration():
    config = IcoreConfig.model_validate(
        {
            "pacs": [
                {"ip": "192.168.1.1", "port": 104, "ae": "PACS1"},
                {"ip": "192.168.1.2", "port": 105, "ae": "PACS2"},
            ]
        }
    )

    assert len(config.pacs) == 2
    assert isinstance(config.pacs[0], PacsConfiguration)
    assert config.pacs[0].host == "192.168.1.1"
    assert config.pacs[0].port == 104
    assert config.pacs[0].aet == "PACS1"
    assert config.pacs[1].host == "192.168.1.2"
    assert config.pacs[1].port == 105
    assert config.pacs[1].aet == "PACS2"


def test_config_maps_ctp_yaml_keys_to_python_fields():
    config = IcoreConfig.model_validate(
        {
            "application_aet": "ICORE",
            "ctp_filters": 'Modality.contains("CT")',
            "ctp_anonymizer": "<script></script>",
            "ctp_lookup_table": "key=value",
            "date_window": 7,
            "apply_default_ctp_filter_script": False,
        }
    )

    assert config.application_aet == "ICORE"
    assert config.filter_script == 'Modality.contains("CT")'
    assert config.anonymizer_script == "<script></script>"
    assert config.lookup_table == "key=value"
    assert config.date_window_days == 7
    assert config.apply_default_filter_script is False


def test_config_spreadsheet_column_keys():
    config = IcoreConfig.model_validate(
        {"acc_col": "AccessionNumber", "mrn_col": "PatientID", "date_col": "StudyDate"}
    )
    assert config.acc_col == "AccessionNumber"
    assert config.mrn_col == "PatientID"
    assert config.date_col == "StudyDate"


def test_config_text_and_header_lists():
    config = IcoreConfig.model_validate(
        {
            "to_keep_list": ["medical", "term"],
            "to_remove_list": ["secret", "data"],
            "columns_to_drop": ["DropColumn1"],
            "columns_to_deid": ["PatientName", "SSN"],
            "headers_to_extract": ["AccessionNumber", "PatientID"],
            "extract_all_headers": True,
        }
    )
    assert config.to_keep_list == ["medical", "term"]
    assert config.to_remove_list == ["secret", "data"]
    assert config.columns_to_drop == ["DropColumn1"]
    assert config.columns_to_deid == ["PatientName", "SSN"]
    assert config.headers_to_extract == ["AccessionNumber", "PatientID"]
    assert config.extract_all_headers is True


def test_config_defaults():
    config = IcoreConfig()

    assert config.pacs == []
    assert config.application_aet is None
    assert config.filter_script is None
    assert config.anonymizer_script is None
    assert config.lookup_table is None
    assert config.mapping_file_path is None
    assert config.date_window_days == 0
    assert config.debug is False
    assert config.deid_pixels is False
    assert config.deid_engine == "ctp"
    assert config.apply_default_filter_script is True
    assert config.cmove_batch_size == 50
    assert config.storescp_port == 50001
    assert config.use_fallback_query is False
    assert config.deferred_delivery is False
    assert config.deferred_delivery_timeout == 172800
    assert config.extract_all_headers is False
    assert config.headers_to_extract is None
    assert config.skip_export is False


def test_config_deferred_delivery_overrides():
    config = IcoreConfig.model_validate(
        {"deferred_delivery": True, "deferred_delivery_timeout": 3600}
    )
    assert config.deferred_delivery is True
    assert config.deferred_delivery_timeout == 3600


def test_config_ignores_unknown_keys():
    # ``module`` routing key and any other extras must not raise.
    config = IcoreConfig.model_validate({"module": "imageqr", "some_future_key": 1})
    assert config.module == "imageqr"


# ---------------------------------------------------------------------------
# run() dispatch — config passed positionally, IO values as kwargs
# ---------------------------------------------------------------------------


def test_run_calls_imageqr_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "module: imageqr\napplication_aet: ICORE\npacs:\n  - ip: localhost\n"
        "    port: 104\n    ae: PACS1\nacc_col: AccessionNumber"
    )
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")

    with patch("module_imageqr.imageqr") as mock_imageqr:
        with patch("utils.Spreadsheet.from_file"):
            mock_imageqr.return_value = {"num_studies_found": 5}

            result = run(str(config_path), input_dir, output_dir)

            mock_imageqr.assert_called_once()
            config = mock_imageqr.call_args.args[0]
            assert isinstance(config, IcoreConfig)
            assert config.application_aet == "ICORE"
            assert config.pacs[0].host == "localhost"
            assert mock_imageqr.call_args.kwargs["output_dir"] == output_dir
            assert result == {"num_studies_found": 5}


def test_run_calls_imagedeid_pacs_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "module: imagedeid\napplication_aet: ICORE\npacs:\n  - ip: localhost\n"
        "    port: 104\n    ae: PACS1\nacc_col: AccessionNumber"
    )
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")

    with patch("module_imagedeid_pacs.imagedeid_pacs") as mock_imagedeid_pacs:
        with patch("utils.Spreadsheet.from_file"):
            mock_imagedeid_pacs.return_value = {"num_images_saved": 100}

            result = run(str(config_path), input_dir, output_dir)

            mock_imagedeid_pacs.assert_called_once()
            config = mock_imagedeid_pacs.call_args.args[0]
            assert config.application_aet == "ICORE"
            assert mock_imagedeid_pacs.call_args.kwargs["output_dir"] == output_dir
            assert result == {"num_images_saved": 100}


def test_run_calls_imagedeid_local_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text('module: imagedeid\nctp_filters: Modality.contains("CT")')
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    output_dir = str(tmp_path / "output")

    with patch("module_imagedeid_local.imagedeid_local") as mock_imagedeid_local:
        mock_imagedeid_local.return_value = {"num_images_saved": 50}

        result = run(str(config_path), input_dir, output_dir)

        mock_imagedeid_local.assert_called_once()
        config = mock_imagedeid_local.call_args.args[0]
        assert config.filter_script == 'Modality.contains("CT")'
        assert mock_imagedeid_local.call_args.kwargs["input_dir"] == input_dir
        assert mock_imagedeid_local.call_args.kwargs["output_dir"] == output_dir
        assert result == {"num_images_saved": 50}


def test_run_calls_textdeid_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "module: textdeid\nto_keep_list:\n  - medical\nto_remove_list:\n  - secret\n"
        "columns_to_drop:\n  - DropColumn\ncolumns_to_deid:\n  - PatientName"
    )
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")

    with patch("module_textdeid.textdeid") as mock_textdeid:
        mock_textdeid.return_value = {"num_rows_processed": 10}

        result = run(str(config_path), input_dir, output_dir)

        mock_textdeid.assert_called_once()
        config = mock_textdeid.call_args.args[0]
        assert config.to_keep_list == ["medical"]
        assert config.to_remove_list == ["secret"]
        assert mock_textdeid.call_args.kwargs["input_file"] == os.path.join(
            input_dir, "input.xlsx"
        )
        assert mock_textdeid.call_args.kwargs["output_dir"] == output_dir
        assert result == {"num_rows_processed": 10}


def test_run_calls_image_export_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "module: imageexport\n"
        "sas_url: http://127.0.0.1:10000/devstoreaccount1/container?sig=token\n"
        "project_name: TestProject"
    )
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    output_dir = str(tmp_path / "output")

    with patch("module_image_export.image_export") as mock_image_export:
        mock_image_export.return_value = {"files_uploaded": 5, "bytes_uploaded": 1024}

        result = run(str(config_path), input_dir, output_dir)

        mock_image_export.assert_called_once()
        config = mock_image_export.call_args.args[0]
        assert (
            config.sas_url
            == "http://127.0.0.1:10000/devstoreaccount1/container?sig=token"
        )
        assert config.project_name == "TestProject"
        assert mock_image_export.call_args.kwargs["input_dir"] == input_dir
        assert result == {"files_uploaded": 5, "bytes_uploaded": 1024}


def test_run_calls_headerextract_local_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "module: headerextract_local\nextract_all_headers: true\ndebug: false"
    )
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    output_dir = str(tmp_path / "output")

    with patch(
        "module_headerextract_local.headerextract_local"
    ) as mock_headerextract_local:
        mock_headerextract_local.return_value = {
            "num_files_processed": 10,
            "num_studies": 5,
        }

        result = run(str(config_path), input_dir, output_dir)

        mock_headerextract_local.assert_called_once()
        config = mock_headerextract_local.call_args.args[0]
        assert config.extract_all_headers is True
        assert config.debug is False
        assert mock_headerextract_local.call_args.kwargs["input_dir"] == input_dir
        assert mock_headerextract_local.call_args.kwargs["output_dir"] == output_dir
        assert result == {"num_files_processed": 10, "num_studies": 5}


def test_run_calls_imagedeidexport_with_correct_params(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "module: imagedeidexport\n"
        "sas_url: http://127.0.0.1:10000/devstoreaccount1/container?sig=token\n"
        "project_name: TestProject"
    )
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir)
    (tmp_path / "input" / "input.xlsx").touch()
    output_dir = str(tmp_path / "output")

    with patch("module_imagedeidexport.imagedeidexport") as mock_imagedeidexport:
        with patch("utils.Spreadsheet.from_file"):
            mock_imagedeidexport.return_value = {
                "files_uploaded": 5,
                "bytes_uploaded": 1024,
            }

            result = run(str(config_path), input_dir, output_dir)

            mock_imagedeidexport.assert_called_once()
            config = mock_imagedeidexport.call_args.args[0]
            assert (
                config.sas_url
                == "http://127.0.0.1:10000/devstoreaccount1/container?sig=token"
            )
            assert config.project_name == "TestProject"
            assert mock_imagedeidexport.call_args.kwargs["output_dir"] == output_dir
            assert result == {"files_uploaded": 5, "bytes_uploaded": 1024}
