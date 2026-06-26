"""Build typed Celery task invocations from task-submission requests.

Each ``build_*`` function maps a parsed request body straight onto one of the
Pydantic args models from the top-level ``tasks`` module and returns
``(celery task, args)`` — one translation step, no intermediate config dicts.

Args are built with ``run_dirs`` unset; ``home.tasks.run_project`` creates the
run directories at execution time so log/appdata timestamps reflect when the
task ran, not when it was submitted.
"""

import json
import os

import pandas as pd

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
    ImagineWorkflowArgs,
    SpreadsheetArgs,
    TextDeidArgs,
)

CMOVE_BATCH_SIZE = 50

HOME_DIR = os.path.expanduser("~")
ICORE_BASE_DIR = os.path.join(HOME_DIR, "Documents", "iCore")
SETTINGS_PATH = os.path.join(ICORE_BASE_DIR, "config", "settings.json")
APP_DATA_PATH = os.path.abspath(os.path.join(ICORE_BASE_DIR, "appdata"))


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
    # The mapping is authoritative: deid the "deid" columns (an empty list
    # means deid nothing) and drop the "drop" columns. Legacy "keep" entries
    # from older saved settings are migrated to "deid". The deid list is
    # returned even when empty so it is not treated as the legacy "deid
    # everything" default downstream.
    deid_list = [
        col for col, action in column_actions.items() if action in ("deid", "keep")
    ]
    drop_list = [col for col, action in column_actions.items() if action == "drop"]
    return (deid_list, drop_list)


# Recognized study-date column names (lowercased). Deliberately exact matches:
# a contains-"date" heuristic would pick up Date of Birth columns.
_DATE_COLUMN_NAMES = {"study date", "studydate", "exam date", "service date", "date"}


def detect_file_type_and_columns(input_file_path):
    """
    Detect file type (Primordial vs mPower) based on column names.
    Returns dict with acc_col, plus mrn_col/date_col when present (these
    enable the MRN+date fallback query).
    """
    df = pd.read_excel(input_file_path, nrows=0)
    columns: list[str] = df.columns.tolist()
    detected: dict[str, str | None] = {}

    if "Acc" in columns:
        detected["acc_col"] = "Acc"
    elif "Accession Number" in columns:
        detected["acc_col"] = "Accession Number"
    else:
        raise ValueError(
            f"Unknown file format. Expected Primordial (Acc column) or mPower (Accession Number column). "
            f"Found columns: {', '.join(columns)}"
        )

    detected["mrn_col"] = next(
        (col for col in columns if "mrn" in col.strip().lower()), None
    )
    detected["date_col"] = next(
        (col for col in columns if col.strip().lower() in _DATE_COLUMN_NAMES), None
    )
    return detected


def build_image_deid(data, project, settings):
    common = dict(
        output_dir=_output_dir(project, "DeID"),
        filter_script=_filter_script(data),
        anonymizer_script=_anonymizer_script(data),
        deid_pixels=data.get("deid_pixels", False),
        apply_default_filter_script=data.get("apply_default_ctp_filter_script", True),
        mapping_file_path=_mapping_file_path(data),
        sc_pdf_output_dir=_sc_pdf_output_dir(data, project),
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
        debug=settings.get("debug_logging", False),
    )


def build_imagineworkflow(data, project, settings):
    """IMAGINE Workflow always enforces HIPAA Safe Harbor de-identification."""
    input_file = data["input_file"]
    detected_columns = detect_file_type_and_columns(input_file)
    columns_to_deid, columns_to_drop = column_actions_to_lists(data)
    to_keep_list = (
        data.get("text_to_keep", "").split("\n") if data.get("text_to_keep") else []
    )
    to_remove_list = (
        data.get("text_to_remove", "").split("\n") if data.get("text_to_remove") else []
    )
    headers_to_extract = data.get("headers_to_extract")
    if headers_to_extract:
        headers_to_extract = [
            h.strip() for h in headers_to_extract.split("\n") if h.strip()
        ]
    return icore_tasks.imagineworkflow, ImagineWorkflowArgs(
        pacs_list=_pacs_list(project),
        query_spreadsheet=SpreadsheetArgs(path=input_file, **detected_columns),
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
        use_fallback_query=data.get("use_fallback_query", False),
        date_window_days=data.get("date_window", 0),
        headers_to_extract=headers_to_extract or None,
        extract_all_headers=data.get("extract_all_headers", False),
        debug=settings.get("debug_logging", False),
    )
