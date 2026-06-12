"""Pydantic models describing the job-creation HTTP API.

Single source of truth for the request payloads the frontend sends to the
run_* endpoints: the views validate incoming bodies against these models, and
``manage.py generate_openapi`` emits an OpenAPI document from them that the
frontend turns into TypeScript types (frontend/src/api/generated.ts). Field
names and defaults must match what ``home.builders`` reads off the payload.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError


class FilterEntry(BaseModel):
    tag: str
    action: str
    value: str = ""


class PacsConfigEntry(BaseModel):
    """An entry of settings.json's pacs_configs (forwarded by the frontend);
    extra keys are tolerated since the settings file is free-form."""

    ip: str
    port: int | str
    ae: str


class RunRequest(BaseModel):
    """Fields shared by every job-creation request. ``extra="forbid"`` so a
    frontend payload key the backend does not read fails loudly instead of
    silently drifting."""

    model_config = ConfigDict(extra="forbid")

    study_name: str


class ScheduledRunRequest(RunRequest):
    scheduled_time: str | None = None


class PacsQueryFields(BaseModel):
    """The accession/MRN/date selection produced by useQueryColumns()."""

    pacs_configs: list[PacsConfigEntry]
    application_aet: str
    input_file: str
    acc_col: str
    mrn_col: str
    date_col: str
    date_window: int = 0
    use_fallback_query: bool = False
    general_filters: list[FilterEntry]
    modality_filters: dict[str, list[FilterEntry]]


class DeidOptionFields(BaseModel):
    """The advanced-options payload produced by useDeidOptions()."""

    tags_to_keep: str
    tags_to_dateshift: str
    tags_to_randomize: str
    date_shift_days: int | str
    site_id: str
    mapping_file_path: str = ""
    use_mapping_file: bool = False
    deid_pixels: bool = False
    remove_unspecified: bool = False
    remove_overlays: bool = False
    remove_curves: bool = False
    remove_private: bool = False
    apply_default_ctp_filter_script: bool = True
    deid_engine: Literal["ctp", "rust"] = "ctp"
    sc_pdf_output_dir: str = ""


class RunHeaderExtractRequest(RunRequest):
    input_folder: str
    output_folder: str
    extract_all_headers: bool = False
    headers_to_extract: str | None = None


class RunTextDeidRequest(ScheduledRunRequest):
    input_file: str
    output_folder: str
    text_to_keep: str = ""
    text_to_remove: str = ""
    column_actions: dict[str, Literal["keep", "deid", "drop"]]
    date_shift_days: int | str = 0


class RunExportRequest(RunRequest):
    input_folder: str
    sas_url: str


class RunQueryRequest(ScheduledRunRequest, PacsQueryFields):
    output_folder: str


class RunDeidRequest(ScheduledRunRequest, PacsQueryFields, DeidOptionFields):
    image_source: Literal["LOCAL", "PACS"]
    input_folder: str
    output_folder: str


class RunImageDeidExportRequest(ScheduledRunRequest, PacsQueryFields, DeidOptionFields):
    output_folder: str
    sas_url: str


class RunSingleClickRequest(ScheduledRunRequest):
    input_file: str
    output_folder: str
    pacs_configs: list[PacsConfigEntry]
    application_aet: str
    sas_url: str = ""
    mapping_file_path: str = ""
    use_mapping_file: bool = False
    sc_pdf_output_dir: str = ""
    text_to_keep: str = ""
    text_to_remove: str = ""
    column_actions: dict[str, Literal["keep", "deid", "drop"]]
    export_to_azure: bool = True
    general_filters: list[FilterEntry] = []
    modality_filters: dict[str, list[FilterEntry]] = {}
    apply_default_ctp_filter_script: bool = True
    use_fallback_query: bool = False
    date_window: int = 0


class RunResponse(BaseModel):
    status: Literal["success", "error"]
    project_id: int
    log_path: str
    message: str | None = None


# (endpoint path, request model, operationId) for every job-creation endpoint;
# consumed by the views' validation gate and the OpenAPI generator.
RUN_ENDPOINTS = [
    ("/run_header_extract/", RunHeaderExtractRequest, "runHeaderExtract"),
    ("/run_deid/", RunDeidRequest, "runDeid"),
    ("/run_query/", RunQueryRequest, "runQuery"),
    ("/run_text_deid/", RunTextDeidRequest, "runTextDeid"),
    ("/run_export/", RunExportRequest, "runExport"),
    ("/run_imagedeidexport/", RunImageDeidExportRequest, "runImageDeidExport"),
    ("/run_singleclickicore/", RunSingleClickRequest, "runSingleClickICore"),
]


def validate_payload(model: type[BaseModel], data: object) -> str | None:
    """Validate a request body against its model; returns a compact,
    user-readable error message, or None when valid."""
    try:
        model.model_validate(data)
        return None
    except ValidationError as error:
        return "Invalid request: " + "; ".join(
            f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}"
            for err in error.errors()
        )
