"""Translate ``Project`` rows into Celery task invocations.

This module ports the per-task-type configuration building that previously
lived in ``home/management/commands/worker.py`` (which wrote a YAML config and
shelled out to the iCore CLI) and the config-to-parameters translation from
``cli.py``. Instead of a YAML file handed to a subprocess, each project is
turned into one of the Pydantic args models from the top-level ``tasks``
module, ready to run as a Celery task.
"""

import json
import os

import pandas as pd
from ruamel.yaml import YAML

from grammar import (
    generate_anonymizer_script,
    generate_filters_string,
    generate_hipaa_safe_harbor_script,
)
from home.models import Project
import tasks as icore_tasks
from tasks import (
    HeaderExtractLocalArgs,
    ImageDeidExportArgs,
    ImageDeidLocalArgs,
    ImageDeidPacsArgs,
    ImageExportArgs,
    ImageQrArgs,
    PacsConfigurationArgs,
    SingleClickIcoreArgs,
    SpreadsheetArgs,
    TextDeidArgs,
)

CMOVE_BATCH_SIZE = 50

HOME_DIR = os.path.expanduser("~")
ICORE_BASE_DIR = os.path.join(HOME_DIR, "Documents", "iCore")
CONFIG_DIR = os.path.join(ICORE_BASE_DIR, "config")
SETTINGS_PATH = os.path.abspath(os.path.join(CONFIG_DIR, "settings.json"))
APP_DATA_PATH = os.path.abspath(os.path.join(ICORE_BASE_DIR, "app_data"))


def load_settings():
    with open(SETTINGS_PATH) as f:
        return json.load(f)


def _appdata_dir(project):
    return os.path.abspath(
        os.path.join(APP_DATA_PATH, f"PHI_{project.name}_{project.timestamp}")
    )


def _output_dir(project, prefix):
    return os.path.abspath(
        os.path.join(
            project.output_folder, f"{prefix}_{project.name}_{project.timestamp}"
        )
    )


def _sc_pdf_output_dir(project):
    if sc_pdf_output_dir := project.parameters.get("sc_pdf_output_dir", ""):
        return os.path.abspath(
            os.path.join(sc_pdf_output_dir, f"PHI_{project.name}_{project.timestamp}")
        )
    return None


def column_actions_to_lists(parameters):
    """Convert task parameters into (columns_to_deid, columns_to_drop) lists."""
    column_actions = parameters.get("column_actions")
    if column_actions is None:
        raise Exception("Missing column_actions parameter.")
    # The mapping is authoritative: deid exactly the "deid" columns (an
    # empty list means deid nothing), drop the "drop" columns, keep the
    # rest. The deid list is returned even when empty so it is not treated
    # as the legacy "deid everything" default downstream.
    deid_list = [col for col, action in column_actions.items() if action == "deid"]
    drop_list = [col for col, action in column_actions.items() if action == "drop"]
    return (deid_list, drop_list)


def detect_file_type_and_columns(input_file_path):
    """
    Detect file type (Primordial vs mPower) based on column names
    Returns dict with acc_col and mrn_col
    """
    df = pd.read_excel(input_file_path, nrows=0)
    columns = df.columns.tolist()

    if "Acc" in columns:
        return {"acc_col": "Acc"}
    elif "Accession Number" in columns:
        return {"acc_col": "Accession Number"}
    else:
        raise ValueError(
            f"Unknown file format. Expected Primordial (Acc column) or mPower (Accession Number column). "
            f"Found columns: {', '.join(columns)}"
        )


def _add_query_columns(config, parameters):
    """Accession/MRN/date column selection shared by the PACS-driven types."""
    if parameters["acc_col"] != "":
        config.update(
            {
                "acc_col": parameters["acc_col"],
                "mrn_col": parameters["mrn_col"],
            }
        )
        if parameters.get("use_fallback_query", False):
            config.update(
                {
                    "date_col": parameters.get("date_col", ""),
                    "date_window": parameters.get("date_window", 0),
                    "use_fallback_query": True,
                }
            )
    elif parameters["mrn_col"] != "" and parameters["date_col"] != "":
        config.update(
            {
                "mrn_col": parameters["mrn_col"],
                "date_col": parameters["date_col"],
                "date_window": parameters.get("date_window", 0),
            }
        )


def _add_filters(config, parameters):
    expression_string = generate_filters_string(
        parameters["general_filters"], parameters["modality_filters"]
    )
    if expression_string != "":
        config["ctp_filters"] = expression_string


def _add_anonymizer(config, parameters):
    config["ctp_anonymizer"] = generate_anonymizer_script(
        parameters["tags_to_keep"],
        parameters["tags_to_dateshift"],
        parameters["tags_to_randomize"],
        parameters["date_shift_days"],
        parameters["site_id"],
        None,
        remove_unspecified=parameters.get("remove_unspecified", True),
        remove_overlays=parameters.get("remove_overlays", True),
        remove_curves=parameters.get("remove_curves", True),
        remove_private=parameters.get("remove_private", True),
    )


def _add_mapping_file(config, parameters):
    mapping_file_path = (
        parameters.get("mapping_file_path", "")
        if parameters.get("use_mapping_file", False)
        else None
    )
    if mapping_file_path:
        config["mapping_file_path"] = mapping_file_path


def _add_deid_engine(config, parameters, settings):
    deid_engine = parameters.get("deid_engine", settings.get("deid_engine", "ctp"))
    if deid_engine != "ctp":
        config["deid_engine"] = deid_engine


def build_image_deid_config(project, settings):
    config = {"module": "imagedeid", "debug": settings.get("debug_logging", False)}
    parameters = project.parameters

    if project.image_source == "PACS":
        config.update(
            {
                "pacs": project.pacs_configs,
                "application_aet": project.application_aet,
            }
        )
        _add_query_columns(config, parameters)

    _add_filters(config, parameters)
    _add_mapping_file(config, parameters)
    _add_anonymizer(config, parameters)
    config["deid_pixels"] = parameters.get("deid_pixels", False)
    config["apply_default_ctp_filter_script"] = parameters.get(
        "apply_default_ctp_filter_script", True
    )
    if sc_pdf_output_dir := _sc_pdf_output_dir(project):
        config["sc_pdf_output_dir"] = sc_pdf_output_dir
    _add_deid_engine(config, parameters, settings)
    return config


def build_image_query_config(project, settings):
    config = {
        "module": "imageqr",
        "debug": settings.get("debug_logging", False),
        "pacs": project.pacs_configs,
        "application_aet": project.application_aet,
    }
    _add_query_columns(config, project.parameters)
    _add_filters(config, project.parameters)
    return config


def build_header_extract_config(project, settings):
    config = {
        "module": "headerextract",
        "debug": settings.get("debug_logging", False),
        "extract_all_headers": project.parameters.get("extract_all_headers", False),
    }
    if headers_to_extract := project.parameters.get("headers_to_extract"):
        config["headers_to_extract"] = headers_to_extract
    return config


def build_text_deid_config(project, settings):
    parameters = project.parameters
    to_keep_list = (
        parameters["text_to_keep"].split("\n") if parameters.get("text_to_keep") else []
    )
    to_remove_list = (
        parameters["text_to_remove"].split("\n")
        if parameters.get("text_to_remove")
        else []
    )

    columns_to_deid_list, columns_to_drop_list = column_actions_to_lists(parameters)

    config = {
        "module": "textdeid",
        "debug": settings.get("debug_logging", False),
        "to_keep_list": to_keep_list,
        "to_remove_list": to_remove_list,
    }
    # An empty list is meaningful (deid nothing); only skip when None.
    if columns_to_deid_list is not None:
        config["columns_to_deid"] = columns_to_deid_list
    if columns_to_drop_list:
        config["columns_to_drop"] = columns_to_drop_list
    return config


def build_image_export_config(project, settings):
    return {
        "module": "imageexport",
        "debug": settings.get("debug_logging", False),
        "sas_url": project.parameters["sas_url"],
        "project_name": project.name,
    }


def build_image_deid_export_config(project, settings):
    config = {
        "module": "imagedeidexport",
        "debug": settings.get("debug_logging", False),
        "pacs": project.pacs_configs,
        "application_aet": project.application_aet,
        "sas_url": project.parameters["sas_url"],
        "project_name": project.name,
        "deferred_delivery": settings.get("deferred_delivery", False),
        "deferred_delivery_timeout": settings.get("deferred_delivery_timeout", 172800),
        "cmove_batch_size": settings.get("cmove_batch_size", CMOVE_BATCH_SIZE),
    }
    parameters = project.parameters
    _add_query_columns(config, parameters)
    _add_filters(config, parameters)
    _add_mapping_file(config, parameters)
    _add_anonymizer(config, parameters)
    config["deid_pixels"] = parameters.get("deid_pixels", False)
    config["apply_default_ctp_filter_script"] = parameters.get(
        "apply_default_ctp_filter_script", True
    )
    if sc_pdf_output_dir := _sc_pdf_output_dir(project):
        config["sc_pdf_output_dir"] = sc_pdf_output_dir
    _add_deid_engine(config, parameters, settings)
    return config


def build_singleclickicore_config(project, settings):
    """Build the configuration for singleclickicore (image deid + text deid + export).

    Single-click iCore automatically enforces HIPAA Safe Harbor de-identification.
    """
    parameters = project.parameters
    config = {
        "module": "singleclickicore",
        "debug": settings.get("debug_logging", False),
        "pacs": project.pacs_configs,
        "application_aet": project.application_aet,
        "sas_url": parameters.get("sas_url", ""),
        "project_name": project.name,
        "deferred_delivery": settings.get("deferred_delivery", False),
        "deferred_delivery_timeout": settings.get("deferred_delivery_timeout", 172800),
        "cmove_batch_size": settings.get("cmove_batch_size", CMOVE_BATCH_SIZE),
    }

    config.update(detect_file_type_and_columns(parameters["input_file"]))

    config["ctp_filters"] = generate_filters_string(
        parameters.get("general_filters", []), parameters.get("modality_filters", {})
    )
    config["deid_pixels"] = True
    _add_mapping_file(config, parameters)

    date_shift_days = settings.get("date_shift_range", -21)
    site_id = settings.get("site_id", "SITE1")
    config["ctp_anonymizer"] = generate_hipaa_safe_harbor_script(
        site_id, date_shift_days
    )

    config["apply_default_ctp_filter_script"] = parameters.get(
        "apply_default_ctp_filter_script", True
    )
    config["skip_export"] = parameters.get("skip_export", False)

    if sc_pdf_output_dir := _sc_pdf_output_dir(project):
        config["sc_pdf_output_dir"] = sc_pdf_output_dir

    to_keep_list = (
        parameters.get("text_to_keep", "").split("\n")
        if parameters.get("text_to_keep")
        else []
    )
    to_remove_list = (
        parameters.get("text_to_remove", "").split("\n")
        if parameters.get("text_to_remove")
        else []
    )
    to_deid_list, to_drop_list = column_actions_to_lists(parameters)

    if to_keep_list:
        config["to_keep_list"] = to_keep_list
    if to_remove_list:
        config["to_remove_list"] = to_remove_list
    # An empty list is meaningful (deid nothing); only skip when None (legacy).
    if to_deid_list is not None:
        config["columns_to_deid"] = to_deid_list
    if to_drop_list:
        config["columns_to_drop"] = to_drop_list

    _add_deid_engine(config, parameters, settings)
    return config


def determine_module(config, input_dir):
    module = config.get("module")

    if module == "imagedeid":
        input_xlsx_path = os.path.join(input_dir, "input.xlsx")
        if os.path.exists(input_xlsx_path):
            return "imagedeid_pacs"
        else:
            return "imagedeid_local"

    if module == "headerextract":
        return "headerextract_local"

    return module


def _config_pacs_list(config):
    return [
        PacsConfigurationArgs(host=pacs["ip"], port=pacs["port"], aet=pacs["ae"])
        for pacs in config.get("pacs", [])
    ]


def _config_spreadsheet(config, spreadsheet_path):
    return SpreadsheetArgs(
        path=spreadsheet_path,
        acc_col=config.get("acc_col"),
        mrn_col=config.get("mrn_col"),
        date_col=config.get("date_col"),
    )


def args_for_module(
    module, config, input_dir, output_dir, appdata_dir, spreadsheet_path, run_dirs
):
    """Map a module config onto (celery task, Pydantic args).

    Port of the ``build_*_params`` functions from ``cli.py``.
    """
    if module == "imageqr":
        return icore_tasks.imageqr, ImageQrArgs(
            pacs_list=_config_pacs_list(config),
            query_spreadsheet=_config_spreadsheet(config, spreadsheet_path),
            application_aet=config.get("application_aet"),
            output_dir=output_dir,
            filter_script=config.get("ctp_filters"),
            date_window_days=config.get("date_window", 0),
            debug=config.get("debug", False),
            run_dirs=run_dirs,
            use_fallback_query=config.get("use_fallback_query", False),
            deferred_delivery=config.get("deferred_delivery", False),
            deferred_delivery_timeout=config.get("deferred_delivery_timeout", 172800),
            cmove_batch_size=config.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        )

    if module == "imagedeid_pacs":
        return icore_tasks.imagedeid_pacs, ImageDeidPacsArgs(
            pacs_list=_config_pacs_list(config),
            query_spreadsheet=_config_spreadsheet(config, spreadsheet_path),
            application_aet=config.get("application_aet"),
            output_dir=output_dir,
            filter_script=config.get("ctp_filters"),
            anonymizer_script=config.get("ctp_anonymizer"),
            lookup_table=config.get("ctp_lookup_table"),
            mapping_file_path=config.get("mapping_file_path"),
            date_window_days=config.get("date_window", 0),
            deid_pixels=config.get("deid_pixels", False),
            debug=config.get("debug", False),
            apply_default_filter_script=config.get(
                "apply_default_ctp_filter_script", True
            ),
            run_dirs=run_dirs,
            sc_pdf_output_dir=config.get("sc_pdf_output_dir"),
            use_fallback_query=config.get("use_fallback_query", False),
            deferred_delivery=config.get("deferred_delivery", False),
            deferred_delivery_timeout=config.get("deferred_delivery_timeout", 172800),
            deid_engine=config.get("deid_engine", "ctp"),
            cmove_batch_size=config.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        )

    if module == "imagedeid_local":
        return icore_tasks.imagedeid_local, ImageDeidLocalArgs(
            input_dir=input_dir,
            output_dir=output_dir,
            filter_script=config.get("ctp_filters"),
            anonymizer_script=config.get("ctp_anonymizer"),
            lookup_table=config.get("ctp_lookup_table"),
            mapping_file_path=config.get("mapping_file_path"),
            deid_pixels=config.get("deid_pixels", False),
            debug=config.get("debug", False),
            apply_default_filter_script=config.get(
                "apply_default_ctp_filter_script", True
            ),
            run_dirs=run_dirs,
            sc_pdf_output_dir=config.get("sc_pdf_output_dir"),
            deid_engine=config.get("deid_engine", "ctp"),
        )

    if module == "textdeid":
        return icore_tasks.textdeid, TextDeidArgs(
            input_file=spreadsheet_path,
            output_dir=output_dir,
            to_keep_list=config.get("to_keep_list"),
            to_remove_list=config.get("to_remove_list"),
            columns_to_drop=config.get("columns_to_drop"),
            columns_to_deid=config.get("columns_to_deid"),
            debug=config.get("debug", False),
            run_dirs=run_dirs,
        )

    if module == "imageexport":
        return icore_tasks.image_export, ImageExportArgs(
            input_dir=input_dir,
            sas_url=config.get("sas_url"),
            project_name=config.get("project_name"),
            appdata_dir=appdata_dir,
            debug=config.get("debug", False),
            run_dirs=run_dirs,
        )

    if module == "imagedeidexport":
        return icore_tasks.imagedeidexport, ImageDeidExportArgs(
            pacs_list=_config_pacs_list(config),
            query_spreadsheet=_config_spreadsheet(config, spreadsheet_path),
            application_aet=config.get("application_aet"),
            sas_url=config.get("sas_url"),
            project_name=config.get("project_name"),
            output_dir=output_dir,
            appdata_dir=appdata_dir,
            filter_script=config.get("ctp_filters"),
            anonymizer_script=config.get("ctp_anonymizer"),
            lookup_table=config.get("ctp_lookup_table"),
            mapping_file_path=config.get("mapping_file_path"),
            date_window_days=config.get("date_window", 0),
            deid_pixels=config.get("deid_pixels", False),
            debug=config.get("debug", False),
            apply_default_filter_script=config.get(
                "apply_default_ctp_filter_script", True
            ),
            run_dirs=run_dirs,
            sc_pdf_output_dir=config.get("sc_pdf_output_dir"),
            use_fallback_query=config.get("use_fallback_query", False),
            deferred_delivery=config.get("deferred_delivery", False),
            deferred_delivery_timeout=config.get("deferred_delivery_timeout", 172800),
            deid_engine=config.get("deid_engine", "ctp"),
            cmove_batch_size=config.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        )

    if module == "headerextract_local":
        return icore_tasks.headerextract_local, HeaderExtractLocalArgs(
            input_dir=input_dir,
            output_dir=output_dir,
            headers_to_extract=config.get("headers_to_extract"),
            extract_all_headers=config.get("extract_all_headers", False),
            debug=config.get("debug", False),
            run_dirs=run_dirs,
        )

    if module == "singleclickicore":
        return icore_tasks.singleclickicore, SingleClickIcoreArgs(
            pacs_list=_config_pacs_list(config),
            query_spreadsheet=_config_spreadsheet(config, spreadsheet_path),
            application_aet=config.get("application_aet"),
            sas_url=config.get("sas_url"),
            project_name=config.get("project_name"),
            output_dir=output_dir,
            input_file=spreadsheet_path,
            appdata_dir=appdata_dir,
            filter_script=config.get("ctp_filters"),
            anonymizer_script=config.get("ctp_anonymizer"),
            lookup_table=config.get("ctp_lookup_table"),
            mapping_file_path=config.get("mapping_file_path"),
            date_window_days=config.get("date_window", 0),
            deid_pixels=config.get("deid_pixels", False),
            to_keep_list=config.get("to_keep_list"),
            to_remove_list=config.get("to_remove_list"),
            columns_to_drop=config.get("columns_to_drop"),
            columns_to_deid=config.get("columns_to_deid"),
            debug=config.get("debug", False),
            apply_default_filter_script=config.get(
                "apply_default_ctp_filter_script", True
            ),
            skip_export=config.get("skip_export", False),
            run_dirs=run_dirs,
            sc_pdf_output_dir=config.get("sc_pdf_output_dir"),
            use_fallback_query=config.get("use_fallback_query", False),
            deferred_delivery=config.get("deferred_delivery", False),
            deferred_delivery_timeout=config.get("deferred_delivery_timeout", 172800),
            deid_engine=config.get("deid_engine", "ctp"),
            cmove_batch_size=config.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        )

    raise ValueError(f"Unknown module: {module}")


def build_project_task(project, run_dirs):
    """Resolve a Project row to a (celery task, Pydantic args) pair."""
    settings = load_settings()
    parameters = project.parameters
    task_type = project.task_type

    if task_type == Project.TaskType.IMAGE_DEID:
        config = build_image_deid_config(project, settings)
        if project.image_source == "PACS":
            module = "imagedeid_pacs"
            spreadsheet_path = parameters["input_file"]
        else:
            module = "imagedeid_local"
            spreadsheet_path = None
        return args_for_module(
            module,
            config,
            input_dir=os.path.abspath(project.input_folder),
            output_dir=_output_dir(project, "DeID"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=spreadsheet_path,
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.IMAGE_QUERY:
        config = build_image_query_config(project, settings)
        return args_for_module(
            "imageqr",
            config,
            input_dir=None,
            output_dir=_output_dir(project, "PHI"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=parameters["input_file"],
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.HEADER_EXTRACT:
        config = build_header_extract_config(project, settings)
        return args_for_module(
            "headerextract_local",
            config,
            input_dir=os.path.abspath(project.input_folder),
            output_dir=_output_dir(project, "PHI"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=None,
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.TEXT_DEID:
        config = build_text_deid_config(project, settings)
        return args_for_module(
            "textdeid",
            config,
            input_dir=None,
            output_dir=_output_dir(project, "DeID"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=parameters["input_file"],
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.IMAGE_EXPORT:
        config = build_image_export_config(project, settings)
        return args_for_module(
            "imageexport",
            config,
            input_dir=os.path.abspath(project.input_folder),
            output_dir=_output_dir(project, "PHI"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=None,
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.IMAGE_DEID_EXPORT:
        config = build_image_deid_export_config(project, settings)
        return args_for_module(
            "imagedeidexport",
            config,
            input_dir=None,
            output_dir=_output_dir(project, "DeID"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=parameters["input_file"],
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.SINGLE_CLICK_ICORE:
        config = build_singleclickicore_config(project, settings)
        return args_for_module(
            "singleclickicore",
            config,
            input_dir=None,
            output_dir=_output_dir(project, "DeID"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=parameters["input_file"],
            run_dirs=run_dirs,
        )

    if task_type == Project.TaskType.GENERAL_MODULE:
        yaml = YAML()
        config = yaml.load(parameters["config"]) or {}
        config["module"] = parameters["module_name"]
        config["debug"] = settings.get("debug_logging", False)
        input_dir = os.path.abspath(project.input_folder)
        module = determine_module(config, input_dir)
        return args_for_module(
            module,
            config,
            input_dir=input_dir,
            output_dir=_output_dir(project, "PHI"),
            appdata_dir=_appdata_dir(project),
            spreadsheet_path=os.path.join(input_dir, "input.xlsx"),
            run_dirs=run_dirs,
        )

    raise ValueError(f"Unknown task type: {task_type}")
