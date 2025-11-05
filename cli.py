import json
import os
import sys

import yaml


def determine_module(config, input_dir):
    module = config.get("module")
    
    if module == "imageqr":
        return "imageqr"
    
    if module == "imagedeid":
        input_xlsx_path = os.path.join(input_dir, "input.xlsx")
        if os.path.exists(input_xlsx_path):
            return "imagedeid_pacs"
        else:
            return "imagedeid_local"
    
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
        "date_window_days": config.get("date_window", 0),
        "run_dirs": run_dirs
    }


def build_imagedeid_local_params(config, input_dir, output_dir, run_dirs):
    return {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "filter_script": config.get("ctp_filters"),
        "anonymizer_script": config.get("ctp_anonymizer"),
        "lookup_table": config.get("ctp_lookup_table"),
        "run_dirs": run_dirs
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
        "run_dirs": run_dirs
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

