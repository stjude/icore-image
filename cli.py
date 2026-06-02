import json
import os
import sys

import yaml

from config import IcoreConfig


def determine_module(config: IcoreConfig, input_dir):
    module = config.module

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


def _load_query_spreadsheet(config: IcoreConfig, input_dir):
    from utils import Spreadsheet

    input_xlsx_path = os.path.join(input_dir, "input.xlsx")
    return Spreadsheet.from_file(
        input_xlsx_path,
        acc_col=config.acc_col,
        mrn_col=config.mrn_col,
        date_col=config.date_col,
    )


def run(config_path, input_dir, output_dir):
    from utils import setup_run_directories

    run_dirs = setup_run_directories()
    print(json.dumps({"log_path": run_dirs["run_log_path"]}), flush=True)

    with open(config_path, "r") as f:
        config = IcoreConfig.model_validate(yaml.safe_load(f))

    module = determine_module(config, input_dir)

    if module == "imageqr":
        from module_imageqr import imageqr

        return imageqr(
            config,
            query_spreadsheet=_load_query_spreadsheet(config, input_dir),
            output_dir=output_dir,
            run_dirs=run_dirs,
        )
    elif module == "imagedeid_pacs":
        from module_imagedeid_pacs import imagedeid_pacs

        return imagedeid_pacs(
            config,
            query_spreadsheet=_load_query_spreadsheet(config, input_dir),
            output_dir=output_dir,
            run_dirs=run_dirs,
        )
    elif module == "imagedeid_local":
        from module_imagedeid_local import imagedeid_local

        return imagedeid_local(
            config,
            input_dir=input_dir,
            output_dir=output_dir,
            run_dirs=run_dirs,
        )
    elif module == "textdeid":
        from module_textdeid import textdeid

        return textdeid(
            config,
            input_file=os.path.join(input_dir, "input.xlsx"),
            output_dir=output_dir,
            run_dirs=run_dirs,
        )
    elif module == "imageexport":
        from module_image_export import image_export

        return image_export(
            config,
            input_dir=input_dir,
            appdata_dir=os.environ.get("ICORE_APPDATA_DIR"),
            run_dirs=run_dirs,
        )
    elif module == "imagedeidexport":
        from module_imagedeidexport import imagedeidexport

        return imagedeidexport(
            config,
            query_spreadsheet=_load_query_spreadsheet(config, input_dir),
            output_dir=output_dir,
            appdata_dir=os.environ.get("ICORE_APPDATA_DIR"),
            run_dirs=run_dirs,
        )
    elif module == "headerextract_local":
        from module_headerextract_local import headerextract_local

        return headerextract_local(
            config,
            input_dir=input_dir,
            output_dir=output_dir,
            run_dirs=run_dirs,
        )
    elif module == "singleclickicore":
        from module_singleclickicore import singleclickicore

        return singleclickicore(
            config,
            query_spreadsheet=_load_query_spreadsheet(config, input_dir),
            input_file=os.path.join(input_dir, "input.xlsx"),
            output_dir=output_dir,
            appdata_dir=os.environ.get("ICORE_APPDATA_DIR"),
            run_dirs=run_dirs,
        )
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
