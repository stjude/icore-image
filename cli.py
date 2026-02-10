import json
import os
import sys

import yaml


def determine_module(config, input_dir):
    module = config.get("module")
    
    if module == "imageqr":
        return "imageqr"
    
    if module == "imagedeidexport":
        return "imagedeidexport"
    
    if module == "singleclickicore":
        return "singleclickicore"
    
    if module == "imagedeid":
        input_xlsx_path = os.path.join(input_dir, "input.xlsx")
        if os.path.exists(input_xlsx_path):
            return "imagedeid_pacs"
        else:
            return "imagedeid_local"
    
    if module == "headerextract":
        return "headerextract_local"
    
    return module


def build_imageqr_params(config, input_dir, output_dir, run_dirs):
    from utils import PacsConfiguration, Spreadsheet
    
    pacs_list = [
        PacsConfiguration(
            host=pacs["ip"],
            port=pacs["port"],
            aet=pacs["ae"]
        )
        for pacs in config.get("pacs", [])
    ]
    
    input_xlsx_path = os.path.join(input_dir, "input.xlsx")
    query_spreadsheet = Spreadsheet.from_file(
        input_xlsx_path,
        acc_col=config.get("acc_col"),
        mrn_col=config.get("mrn_col"),
        date_col=config.get("date_col")
    )
    
    return {
        "pacs_list": pacs_list,
        "query_spreadsheet": query_spreadsheet,
        "application_aet": config.get("application_aet"),
        "output_dir": output_dir,
        "filter_script": config.get("ctp_filters"),
        "date_window_days": config.get("date_window", 0),
        "debug": config.get("debug", False),
        "run_dirs": run_dirs
    }


def build_imagedeid_pacs_params(config, input_dir, output_dir, run_dirs):
    from utils import PacsConfiguration, Spreadsheet
    
    pacs_list = [
        PacsConfiguration(
            host=pacs["ip"],
            port=pacs["port"],
            aet=pacs["ae"]
        )
        for pacs in config.get("pacs", [])
    ]
    
    input_xlsx_path = os.path.join(input_dir, "input.xlsx")
    query_spreadsheet = Spreadsheet.from_file(
        input_xlsx_path,
        acc_col=config.get("acc_col"),
        mrn_col=config.get("mrn_col"),
        date_col=config.get("date_col")
    )
    
    return {
        "pacs_list": pacs_list,
        "query_spreadsheet": query_spreadsheet,
        "application_aet": config.get("application_aet"),
        "output_dir": output_dir,
        "filter_script": config.get("ctp_filters"),
        "anonymizer_script": config.get("ctp_anonymizer"),
        "lookup_table": config.get("ctp_lookup_table"),
        "mapping_file_path": config.get("mapping_file_path"),
        "date_window_days": config.get("date_window", 0),
        "deid_pixels": config.get("deid_pixels", False),
        "debug": config.get("debug", False),
        "apply_default_filter_script": config.get("apply_default_ctp_filter_script", True),
        "run_dirs": run_dirs,
        "sc_pdf_output_dir": config.get("sc_pdf_output_dir")
    }


def build_imagedeid_local_params(config, input_dir, output_dir, run_dirs):
    return {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "filter_script": config.get("ctp_filters"),
        "anonymizer_script": config.get("ctp_anonymizer"),
        "lookup_table": config.get("ctp_lookup_table"),
        "mapping_file_path": config.get("mapping_file_path"),
        "deid_pixels": config.get("deid_pixels", False),
        "debug": config.get("debug", False),
        "apply_default_filter_script": config.get("apply_default_ctp_filter_script", True),
        "run_dirs": run_dirs,
        "sc_pdf_output_dir": config.get("sc_pdf_output_dir")
    }


def build_textdeid_params(config, input_dir, output_dir, run_dirs):
    input_file = os.path.join(input_dir, "input.xlsx")
    return {
        "input_file": input_file,
        "output_dir": output_dir,
        "to_keep_list": config.get("to_keep_list"),
        "to_remove_list": config.get("to_remove_list"),
        "columns_to_drop": config.get("columns_to_drop"),
        "columns_to_deid": config.get("columns_to_deid"),
        "debug": config.get("debug", False),
        "run_dirs": run_dirs
    }


def build_image_export_params(config, input_dir, run_dirs):
    appdata_dir = os.environ.get('ICORE_APPDATA_DIR')
    return {
        "input_dir": input_dir,
        "sas_url": config.get("sas_url"),
        "project_name": config.get("project_name"),
        "appdata_dir": appdata_dir,
        "debug": config.get("debug", False),
        "run_dirs": run_dirs
    }


def build_imagedeidexport_params(config, input_dir, output_dir, run_dirs):
    from utils import PacsConfiguration, Spreadsheet
    
    pacs_list = [
        PacsConfiguration(
            host=pacs["ip"],
            port=pacs["port"],
            aet=pacs["ae"]
        )
        for pacs in config.get("pacs", [])
    ]
    
    input_xlsx_path = os.path.join(input_dir, "input.xlsx")
    query_spreadsheet = Spreadsheet.from_file(
        input_xlsx_path,
        acc_col=config.get("acc_col"),
        mrn_col=config.get("mrn_col"),
        date_col=config.get("date_col")
    )
    
    appdata_dir = os.environ.get('ICORE_APPDATA_DIR')
    
    return {
        "pacs_list": pacs_list,
        "query_spreadsheet": query_spreadsheet,
        "application_aet": config.get("application_aet"),
        "sas_url": config.get("sas_url"),
        "project_name": config.get("project_name"),
        "output_dir": output_dir,
        "appdata_dir": appdata_dir,
        "filter_script": config.get("ctp_filters"),
        "anonymizer_script": config.get("ctp_anonymizer"),
        "lookup_table": config.get("ctp_lookup_table"),
        "mapping_file_path": config.get("mapping_file_path"),
        "date_window_days": config.get("date_window", 0),
        "deid_pixels": config.get("deid_pixels", False),
        "debug": config.get("debug", False),
        "apply_default_filter_script": config.get("apply_default_ctp_filter_script", True),
        "run_dirs": run_dirs,
        "sc_pdf_output_dir": config.get("sc_pdf_output_dir")
    }

  
def build_headerextract_local_params(config, input_dir, output_dir, run_dirs):
    return {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "headers_to_extract": config.get("headers_to_extract"),
        "extract_all_headers": config.get("extract_all_headers", False),
        "debug": config.get("debug", False),
        "run_dirs": run_dirs
    }


def build_singleclickicore_params(config, input_dir, output_dir, run_dirs):
    from utils import PacsConfiguration, Spreadsheet
    
    pacs_list = [
        PacsConfiguration(
            host=pacs["ip"],
            port=pacs["port"],
            aet=pacs["ae"]
        )
        for pacs in config.get("pacs", [])
    ]
    
    input_xlsx_path = os.path.join(input_dir, "input.xlsx")
    query_spreadsheet = Spreadsheet.from_file(
        input_xlsx_path,
        acc_col=config.get("acc_col"),
        mrn_col=config.get("mrn_col"),
        date_col=config.get("date_col")
    )
    
    appdata_dir = os.environ.get('ICORE_APPDATA_DIR')
    
    return {
        "pacs_list": pacs_list,
        "query_spreadsheet": query_spreadsheet,
        "application_aet": config.get("application_aet"),
        "sas_url": config.get("sas_url"),
        "project_name": config.get("project_name"),
        "output_dir": output_dir,
        "input_file": input_xlsx_path,
        "appdata_dir": appdata_dir,
        "filter_script": config.get("ctp_filters"),
        "anonymizer_script": config.get("ctp_anonymizer"),
        "lookup_table": config.get("ctp_lookup_table"),
        "mapping_file_path": config.get("mapping_file_path"),
        "date_window_days": config.get("date_window", 0),
        "deid_pixels": config.get("deid_pixels", False),
        "to_keep_list": config.get("to_keep_list"),
        "to_remove_list": config.get("to_remove_list"),
        "columns_to_drop": config.get("columns_to_drop"),
        "columns_to_deid": config.get("columns_to_deid"),
        "debug": config.get("debug", False),
        "apply_default_filter_script": config.get("apply_default_ctp_filter_script", True),
        "skip_export": config.get("skip_export", False),
        "run_dirs": run_dirs,
        "sc_pdf_output_dir": config.get("sc_pdf_output_dir")
    }


def run(config_path, input_dir, output_dir):
    from utils import setup_run_directories
    
    run_dirs = setup_run_directories()
    print(json.dumps({"log_path": run_dirs["run_log_path"]}), flush=True)
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    module = determine_module(config, input_dir)
    
    if module == "imageqr":
        from module_imageqr import imageqr
        params = build_imageqr_params(config, input_dir, output_dir, run_dirs)
        return imageqr(**params)
    elif module == "imagedeid_pacs":
        from module_imagedeid_pacs import imagedeid_pacs
        params = build_imagedeid_pacs_params(config, input_dir, output_dir, run_dirs)
        return imagedeid_pacs(**params)
    elif module == "imagedeid_local":
        from module_imagedeid_local import imagedeid_local
        params = build_imagedeid_local_params(config, input_dir, output_dir, run_dirs)
        return imagedeid_local(**params)
    elif module == "textdeid":
        from module_textdeid import textdeid
        params = build_textdeid_params(config, input_dir, output_dir, run_dirs)
        return textdeid(**params)
    elif module == "imageexport":
        from module_image_export import image_export
        params = build_image_export_params(config, input_dir, run_dirs)
        return image_export(**params)
    elif module == "imagedeidexport":
        from module_imagedeidexport import imagedeidexport
        params = build_imagedeidexport_params(config, input_dir, output_dir, run_dirs)
        return imagedeidexport(**params)
    elif module == "headerextract_local":
        from module_headerextract_local import headerextract_local
        params = build_headerextract_local_params(config, input_dir, output_dir, run_dirs)
        return headerextract_local(**params)
    elif module == "singleclickicore":
        from module_singleclickicore import singleclickicore
        params = build_singleclickicore_params(config, input_dir, output_dir, run_dirs)
        return singleclickicore(**params)
    else:
        raise ValueError(f"Unknown module: {module}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python cli.py <config.yml> <input_dir> <output_dir>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    input_dir = sys.argv[2]
    output_dir = sys.argv[3]
    
    result = run(config_path, input_dir, output_dir)
    print(f"Processing complete: {result}")

