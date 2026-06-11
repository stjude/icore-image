"""Build typed Celery task invocations from task-submission requests.

Each ``build_*`` function maps a parsed request body straight onto one of the
Pydantic args models from the top-level ``tasks`` module and returns
``(celery task, args)`` — one translation step, no intermediate config dicts.
GENERAL_MODULE is the lone exception: it accepts an arbitrary YAML config, so
``args_for_module`` translates that config into typed args.

Args are built with ``run_dirs`` unset; ``home.tasks.run_project`` creates the
run directories at execution time so log/appdata timestamps reflect when the
task ran, not when it was submitted.
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
SETTINGS_PATH = os.path.join(ICORE_BASE_DIR, "config", "settings.json")
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


def _sc_pdf_output_dir(data, project):
    if sc_pdf_output_dir := data.get("sc_pdf_output_dir", ""):
        return os.path.abspath(
            os.path.join(sc_pdf_output_dir, f"PHI_{project.name}_{project.timestamp}")
        )
    return None


def _pacs_list(project):
    return [
        PacsConfigurationArgs(host=pacs["ip"], port=pacs["port"], aet=pacs["ae"])
        for pacs in project.pacs_configs
    ]


def _query_spreadsheet(data):
    """Accession/MRN/date column precedence shared by the PACS query types."""
    acc_col = mrn_col = date_col = None
    date_window_days = 0
    use_fallback_query = False
    if data["acc_col"] != "":
        acc_col = data["acc_col"]
        mrn_col = data["mrn_col"]
        if data.get("use_fallback_query", False):
            date_col = data.get("date_col", "")
            date_window_days = data.get("date_window", 0)
            use_fallback_query = True
    elif data["mrn_col"] != "" and data["date_col"] != "":
        mrn_col = data["mrn_col"]
        date_col = data["date_col"]
        date_window_days = data.get("date_window", 0)
    spreadsheet = SpreadsheetArgs(
        path=data["input_file"], acc_col=acc_col, mrn_col=mrn_col, date_col=date_col
    )
    return spreadsheet, date_window_days, use_fallback_query


def _filter_script(data):
    expression = generate_filters_string(
        data["general_filters"], data["modality_filters"]
    )
    return expression or None


def _anonymizer_script(data):
    return generate_anonymizer_script(
        data["tags_to_keep"],
        data["tags_to_dateshift"],
        data["tags_to_randomize"],
        data["date_shift_days"],
        data["site_id"],
        None,
        remove_unspecified=data.get("remove_unspecified", False),
        remove_overlays=data.get("remove_overlays", False),
        remove_curves=data.get("remove_curves", False),
        remove_private=data.get("remove_private", False),
    )


def _mapping_file_path(data):
    if data.get("use_mapping_file", False):
        return data.get("mapping_file_path", "") or None
    return None


def column_actions_to_lists(data):
    """Convert column actions into (columns_to_deid, columns_to_drop) lists."""
    column_actions = data.get("column_actions")
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


def build_image_deid(data, project, settings):
    common = dict(
        output_dir=_output_dir(project, "DeID"),
        filter_script=_filter_script(data),
        anonymizer_script=_anonymizer_script(data),
        deid_pixels=data.get("deid_pixels", False),
        apply_default_filter_script=data.get("apply_default_ctp_filter_script", True),
        mapping_file_path=_mapping_file_path(data),
        sc_pdf_output_dir=_sc_pdf_output_dir(data, project),
        deid_engine=data.get("deid_engine", "ctp"),
        debug=settings.get("debug_logging", False),
    )
    if project.image_source == "PACS":
        spreadsheet, date_window_days, use_fallback_query = _query_spreadsheet(data)
        return icore_tasks.imagedeid_pacs, ImageDeidPacsArgs(
            pacs_list=_pacs_list(project),
            query_spreadsheet=spreadsheet,
            application_aet=project.application_aet,
            cmove_batch_size=CMOVE_BATCH_SIZE,
            date_window_days=date_window_days,
            use_fallback_query=use_fallback_query,
            **common,
        )
    return icore_tasks.imagedeid_local, ImageDeidLocalArgs(
        input_dir=os.path.abspath(project.input_folder), **common
    )


def build_image_query(data, project, settings):
    spreadsheet, date_window_days, use_fallback_query = _query_spreadsheet(data)
    return icore_tasks.imageqr, ImageQrArgs(
        pacs_list=_pacs_list(project),
        query_spreadsheet=spreadsheet,
        application_aet=project.application_aet,
        output_dir=_output_dir(project, "PHI"),
        cmove_batch_size=CMOVE_BATCH_SIZE,
        filter_script=_filter_script(data),
        date_window_days=date_window_days,
        use_fallback_query=use_fallback_query,
        debug=settings.get("debug_logging", False),
    )


def build_header_extract(data, project, settings):
    headers_to_extract = data.get("headers_to_extract")
    if headers_to_extract:
        headers_to_extract = [
            h.strip() for h in headers_to_extract.split("\n") if h.strip()
        ]
    return icore_tasks.headerextract_local, HeaderExtractLocalArgs(
        input_dir=os.path.abspath(project.input_folder),
        output_dir=_output_dir(project, "PHI"),
        headers_to_extract=headers_to_extract or None,
        extract_all_headers=data.get("extract_all_headers", False),
        debug=settings.get("debug_logging", False),
    )


def build_text_deid(data, project, settings):
    columns_to_deid, columns_to_drop = column_actions_to_lists(data)
    return icore_tasks.textdeid, TextDeidArgs(
        input_file=data["input_file"],
        output_dir=_output_dir(project, "DeID"),
        to_keep_list=data["text_to_keep"].split("\n")
        if data.get("text_to_keep")
        else [],
        to_remove_list=data["text_to_remove"].split("\n")
        if data.get("text_to_remove")
        else [],
        # An empty deid list is meaningful (deid nothing); drop is optional.
        columns_to_deid=columns_to_deid,
        columns_to_drop=columns_to_drop or None,
        debug=settings.get("debug_logging", False),
    )


def build_image_export(data, project, settings):
    return icore_tasks.image_export, ImageExportArgs(
        input_dir=os.path.abspath(project.input_folder),
        sas_url=data["sas_url"],
        project_name=project.name,
        appdata_dir=_appdata_dir(project),
        debug=settings.get("debug_logging", False),
    )


def build_image_deid_export(data, project, settings):
    spreadsheet, date_window_days, use_fallback_query = _query_spreadsheet(data)
    return icore_tasks.imagedeidexport, ImageDeidExportArgs(
        pacs_list=_pacs_list(project),
        query_spreadsheet=spreadsheet,
        application_aet=project.application_aet,
        sas_url=data["sas_url"],
        project_name=project.name,
        output_dir=_output_dir(project, "DeID"),
        appdata_dir=_appdata_dir(project),
        cmove_batch_size=settings.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        deferred_delivery=settings.get("deferred_delivery", False),
        deferred_delivery_timeout=settings.get("deferred_delivery_timeout", 172800),
        filter_script=_filter_script(data),
        anonymizer_script=_anonymizer_script(data),
        date_window_days=date_window_days,
        use_fallback_query=use_fallback_query,
        deid_pixels=data.get("deid_pixels", False),
        apply_default_filter_script=data.get("apply_default_ctp_filter_script", True),
        mapping_file_path=_mapping_file_path(data),
        sc_pdf_output_dir=_sc_pdf_output_dir(data, project),
        deid_engine=data.get("deid_engine", "ctp"),
        debug=settings.get("debug_logging", False),
    )


def build_singleclickicore(data, project, settings):
    """Single-click iCore always enforces HIPAA Safe Harbor de-identification."""
    input_file = data["input_file"]
    detected_columns = detect_file_type_and_columns(input_file)
    columns_to_deid, columns_to_drop = column_actions_to_lists(data)
    to_keep_list = (
        data.get("text_to_keep", "").split("\n") if data.get("text_to_keep") else []
    )
    to_remove_list = (
        data.get("text_to_remove", "").split("\n")
        if data.get("text_to_remove")
        else []
    )
    return icore_tasks.singleclickicore, SingleClickIcoreArgs(
        pacs_list=_pacs_list(project),
        query_spreadsheet=SpreadsheetArgs(
            path=input_file, acc_col=detected_columns["acc_col"]
        ),
        application_aet=project.application_aet,
        sas_url=data.get("sas_url", ""),
        project_name=project.name,
        output_dir=_output_dir(project, "DeID"),
        input_file=input_file,
        appdata_dir=_appdata_dir(project),
        cmove_batch_size=settings.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        deferred_delivery=settings.get("deferred_delivery", False),
        deferred_delivery_timeout=settings.get("deferred_delivery_timeout", 172800),
        filter_script=generate_filters_string(
            data.get("general_filters", []), data.get("modality_filters", {})
        ),
        anonymizer_script=generate_hipaa_safe_harbor_script(
            settings.get("site_id", "SITE1"), settings.get("date_shift_range", -21)
        ),
        deid_pixels=True,
        apply_default_filter_script=data.get("apply_default_ctp_filter_script", True),
        mapping_file_path=_mapping_file_path(data),
        to_keep_list=to_keep_list or None,
        to_remove_list=to_remove_list or None,
        columns_to_deid=columns_to_deid,
        columns_to_drop=columns_to_drop or None,
        skip_export=not data.get("export_to_azure", True),
        sc_pdf_output_dir=_sc_pdf_output_dir(data, project),
        deid_engine=settings.get("deid_engine", "ctp"),
        debug=settings.get("debug_logging", False),
    )


def build_general_module(data, project, settings):
    config = YAML().load(data["config"]) or {}
    config["module"] = data["module_name"]
    config["debug"] = settings.get("debug_logging", False)
    input_dir = os.path.abspath(project.input_folder)
    return args_for_module(
        determine_module(config, input_dir),
        config,
        input_dir=input_dir,
        output_dir=_output_dir(project, "PHI"),
        appdata_dir=_appdata_dir(project),
        spreadsheet_path=os.path.join(input_dir, "input.xlsx"),
    )


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
    module, config, input_dir, output_dir, appdata_dir, spreadsheet_path
):
    """Map a general-module YAML config onto (celery task, Pydantic args)."""
    if module == "imageqr":
        return icore_tasks.imageqr, ImageQrArgs(
            pacs_list=_config_pacs_list(config),
            query_spreadsheet=_config_spreadsheet(config, spreadsheet_path),
            application_aet=config.get("application_aet"),
            output_dir=output_dir,
            filter_script=config.get("ctp_filters"),
            date_window_days=config.get("date_window", 0),
            debug=config.get("debug", False),
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
        )

    if module == "imageexport":
        return icore_tasks.image_export, ImageExportArgs(
            input_dir=input_dir,
            sas_url=config.get("sas_url"),
            project_name=config.get("project_name"),
            appdata_dir=appdata_dir,
            debug=config.get("debug", False),
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
            sc_pdf_output_dir=config.get("sc_pdf_output_dir"),
            use_fallback_query=config.get("use_fallback_query", False),
            deferred_delivery=config.get("deferred_delivery", False),
            deferred_delivery_timeout=config.get("deferred_delivery_timeout", 172800),
            deid_engine=config.get("deid_engine", "ctp"),
            cmove_batch_size=config.get("cmove_batch_size", CMOVE_BATCH_SIZE),
        )

    raise ValueError(f"Unknown module: {module}")
