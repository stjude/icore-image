import os
import sys

import yaml

from module_imageqr import imageqr
from module_imagedeid_local import imagedeid_local
from module_imagedeid_pacs import imagedeid_pacs
from utils import PacsConfiguration, Spreadsheet


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


def build_imageqr_params(config, input_dir, output_dir):
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
        "date_window_days": config.get("date_window", 0)
    }


def build_imagedeid_pacs_params(config, input_dir, output_dir):
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
        "date_window_days": config.get("date_window", 0)
    }


def build_imagedeid_local_params(config, input_dir, output_dir):
    return {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "filter_script": config.get("ctp_filters"),
        "anonymizer_script": config.get("ctp_anonymizer"),
        "lookup_table": config.get("ctp_lookup_table")
    }


def run(config_path, input_dir, output_dir):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    module = determine_module(config, input_dir)
    
    if module == "imageqr":
        params = build_imageqr_params(config, input_dir, output_dir)
        return imageqr(**params)
    elif module == "imagedeid_pacs":
        params = build_imagedeid_pacs_params(config, input_dir, output_dir)
        return imagedeid_pacs(**params)
    elif module == "imagedeid_local":
        params = build_imagedeid_local_params(config, input_dir, output_dir)
        return imagedeid_local(**params)
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

