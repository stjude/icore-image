"""Single typed configuration model for the iCore application.

All configuration knobs that used to be piped through the application as a bag of
keyword arguments now live on :class:`IcoreConfig`. The model is built once at the
CLI boundary (``IcoreConfig.model_validate(yaml.safe_load(...))``) and threaded
through the module functions, pipelines, and stages.

Field names match the historical python kwarg names; ``validation_alias`` maps the
YAML config keys (e.g. ``ctp_filters`` -> ``filter_script``). ``populate_by_name``
lets callers (tests) construct the model with either the python name or the alias.

Genuine runtime/IO values (the loaded ``Spreadsheet``, ``run_dirs``, and the
input/output/appdata directories) are intentionally *not* config and are passed as
separate parameters.
"""

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from utils import DeidEngine, PacsConfiguration


class IcoreConfig(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        arbitrary_types_allowed=True,
    )

    # --- routing ---
    module: str | None = None

    # --- PACS connection / query ---
    pacs: list[PacsConfiguration] = Field(default_factory=list)
    application_aet: str | None = None
    cmove_batch_size: int = 50
    storescp_port: int = 50001
    use_fallback_query: bool = False
    deferred_delivery: bool = False
    deferred_delivery_timeout: int = 172800
    date_window_days: int = Field(
        default=0,
        validation_alias=AliasChoices("date_window", "date_window_days"),
    )

    # --- spreadsheet column names (used to build the query Spreadsheet) ---
    acc_col: str | None = None
    mrn_col: str | None = None
    date_col: str | None = None

    # --- image de-identification ---
    filter_script: str | None = Field(
        default=None,
        validation_alias=AliasChoices("ctp_filters", "filter_script"),
    )
    anonymizer_script: str | None = Field(
        default=None,
        validation_alias=AliasChoices("ctp_anonymizer", "anonymizer_script"),
    )
    lookup_table: str | None = Field(
        default=None,
        validation_alias=AliasChoices("ctp_lookup_table", "lookup_table"),
    )
    apply_default_filter_script: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "apply_default_ctp_filter_script", "apply_default_filter_script"
        ),
    )
    mapping_file_path: str | None = None
    sc_pdf_output_dir: str | None = None
    deid_pixels: bool = False
    deid_engine: DeidEngine = "ctp"

    # --- text de-identification ---
    to_keep_list: list[str] | None = None
    to_remove_list: list[str] | None = None
    columns_to_drop: list[str] | None = None
    columns_to_deid: list[str] | None = None

    # --- header extract ---
    headers_to_extract: list[str] | None = None
    extract_all_headers: bool = False

    # --- export ---
    sas_url: str | None = None
    project_name: str | None = None

    # --- single-click orchestration ---
    skip_export: bool = False

    # --- misc ---
    debug: bool = False

    @field_validator("pacs", mode="before")
    @classmethod
    def _coerce_pacs(cls, value):
        """Accept the YAML ``{"ip", "port", "ae"}`` form and produce
        ``PacsConfiguration`` instances (the canonical downstream type)."""
        if value is None:
            return []
        coerced = []
        for entry in value:
            if isinstance(entry, PacsConfiguration):
                coerced.append(entry)
            elif isinstance(entry, dict):
                coerced.append(
                    PacsConfiguration(
                        host=entry.get("host", entry.get("ip")),
                        port=entry["port"],
                        aet=entry.get("aet", entry.get("ae")),
                    )
                )
            else:
                raise TypeError(f"Unsupported PACS entry: {entry!r}")
        return coerced
