import logging
import os
import time
import xml.etree.ElementTree as ET
from abc import ABC

import pandas as pd

from ctp import CTPPipeline, generate_sc_pdf_filter
from pipeline.base import PipelineStage
from pipeline.context import PipelineContext
from utils import (
    combine_filters,
    csv_string_to_xlsx,
    detect_and_validate_dates,
    format_dicom_date,
    format_number_with_commas,
    validate_dicom_tags,
)


# ---------------------------------------------------------------------------
# Metadata save / progress helpers (shared by CTP monitor loop)
# ---------------------------------------------------------------------------


def _save_metadata_files(pipeline: CTPPipeline, appdata_dir: str) -> None:
    audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
    if audit_log_csv:
        csv_string_to_xlsx(audit_log_csv, os.path.join(appdata_dir, "metadata.xlsx"))

    deid_audit_log_csv = pipeline.get_audit_log_csv("DeidAuditLog")
    if deid_audit_log_csv:
        csv_string_to_xlsx(
            deid_audit_log_csv, os.path.join(appdata_dir, "deid_metadata.xlsx")
        )

    linker_csv = pipeline.get_idmap_csv()
    if linker_csv:
        csv_string_to_xlsx(linker_csv, os.path.join(appdata_dir, "linker.xlsx"))


def _log_progress(pipeline: CTPPipeline, total_files: int | None = None) -> None:
    if not pipeline.metrics:
        return
    received = pipeline.metrics.files_received
    quarantined = pipeline.metrics.files_quarantined
    if total_files is not None:
        msg = (
            f"Processed {format_number_with_commas(received)} / "
            f"{format_number_with_commas(total_files)} files"
        )
    else:
        msg = f"Processed {format_number_with_commas(received)} files"
    if quarantined > 0:
        msg += f" ({format_number_with_commas(quarantined)} quarantined)"
    logging.info(msg)


def _collect_engine_audit_files(output_dir: str, appdata_dir: str) -> None:
    """Move engine-generated audit CSVs to appdata_dir as Excel files.

    The Rust engine writes metadata/deid_metadata/linker CSVs into the
    output directory alongside the de-identified DICOMs; we lift them into
    appdata_dir as .xlsx to match the CTP engine's layout.
    """
    for csv_name, xlsx_name in [
        ("metadata.csv", "metadata.xlsx"),
        ("deid_metadata.csv", "deid_metadata.xlsx"),
        ("linker.csv", "linker.xlsx"),
    ]:
        csv_path = os.path.join(output_dir, csv_name)
        if os.path.exists(csv_path):
            with open(csv_path, "r") as f:
                csv_string_to_xlsx(f.read(), os.path.join(appdata_dir, xlsx_name))
            os.remove(csv_path)
            logging.info(f"Converted {csv_name} -> {xlsx_name}")


# ---------------------------------------------------------------------------
# Filter composition helpers
# ---------------------------------------------------------------------------


def _apply_default_filter_script(
    filter_script: str | None, apply_default_filter_script: bool
) -> str | None:
    """Merge the Stanford device-whitelisting filter into the user's filter.

    The Stanford filter (from ``ctp/scripts/stanford-filter.script``) acts as
    a whitelist: files must match a known-safe device signature to pass.

    When *apply_default_filter_script* is ``False``, the user's filter is
    returned unchanged.

    **Note:** the SC/PDF exclusion filter (:func:`generate_sc_pdf_filter`)
    is NOT merged here.  CTP callers add it inline via
    :func:`_combine_with_sc_pdf` (a CTP-syntax boolean expression).  The
    Rust recipe format expresses this as a separate ``%filter blacklist``
    section because the recipe's filter translator cannot correctly flatten
    CTP's deeply nested boolean AND/OR.
    """
    if not apply_default_filter_script:
        return filter_script

    result = filter_script

    stanford_filter_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "ctp",
        "scripts",
        "stanford-filter.script",
    )
    if os.path.exists(stanford_filter_path):
        with open(stanford_filter_path, "r") as f:
            stanford_filter_content = f.read()
        result = combine_filters(result, stanford_filter_content)
    else:
        logging.warning(f"Stanford filter script not found at {stanford_filter_path}")

    return result


def _combine_with_sc_pdf(
    filter_script: str | None, apply_default_filter_script: bool
) -> str | None:
    """AND-merge the SC/PDF exclusion into a filter for CTP consumption."""
    if not apply_default_filter_script:
        return filter_script
    sc_pdf_exclusion = f"!({generate_sc_pdf_filter()})"
    return combine_filters(filter_script, sc_pdf_exclusion)


def _get_sc_pdf_blacklist(apply_default_filter_script: bool) -> str | None:
    """Return the SC/PDF exclusion as a standalone blacklist for Rust."""
    if not apply_default_filter_script:
        return None
    return generate_sc_pdf_filter()


# ---------------------------------------------------------------------------
# Mapping-file + anonymizer-script merge helpers
# ---------------------------------------------------------------------------


def _generate_lookup_table_content(mapping_file_path: str) -> str:
    df = pd.read_excel(mapping_file_path)

    tag_mappings = {}
    for col in df.columns:
        if col.startswith("New-"):
            original_tag = col[4:]
            if original_tag in df.columns:
                tag_mappings[original_tag] = col

    if not tag_mappings:
        raise ValueError(
            "Mapping file must have at least one New-{TagName} column with "
            "corresponding TagName column"
        )

    all_tags = list(tag_mappings.keys())
    validate_dicom_tags(all_tags)

    date_tags = {}
    for tag in all_tags:
        if detect_and_validate_dates(df, tag):
            date_tags[tag] = True

    lookup_lines = []
    for tag, new_tag_col in tag_mappings.items():
        is_date = tag in date_tags

        for _, row in df.iterrows():
            original_value = row[tag]
            new_value = row[new_tag_col]

            if pd.notna(original_value) and pd.notna(new_value):
                if is_date:
                    original_value = format_dicom_date(original_value)
                    new_value = format_dicom_date(new_value)
                else:
                    original_value = str(original_value).strip()
                    new_value = str(new_value).strip()

                lookup_lines.append(f"{tag}/{original_value} = {new_value}")

    return "\n".join(lookup_lines)


def _parse_anonymizer_script_actions(anonymizer_script: str) -> dict[str, str]:
    tag_actions: dict[str, str] = {}

    if not anonymizer_script:
        return tag_actions

    try:
        root = ET.fromstring(anonymizer_script)

        for elem in root.findall(".//e[@n]"):
            tag_name = elem.get("n")
            tag_text = elem.text

            if tag_name and tag_text:
                tag_actions[tag_name] = tag_text.strip()

        return tag_actions
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse anonymizer script XML: {e}")


def _get_tag_hex_from_keyword(tag_keyword: str) -> str | None:
    dictionary_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "resources",
        "dictionary.xml",
    )
    tree = ET.parse(dictionary_path)
    root = tree.getroot()

    for element in root.findall(".//element[@key]"):
        if element.get("key") == tag_keyword:
            tag_attr = element.get("tag")
            if tag_attr:
                tag_hex = tag_attr.replace("(", "").replace(")", "").replace(",", "")
                return tag_hex

    raise ValueError(f"Tag {tag_keyword} not found in DICOM dictionary")


def _extract_simple_action(action_string: str) -> str | None:
    simple_actions = ["@keep()", "@remove()", "@empty()"]

    for simple_action in simple_actions:
        if action_string == simple_action:
            return simple_action[1:-2]

    return "quarantine"


def _merge_mapping_with_script(mapping_file_path: str, anonymizer_script: str) -> str:
    df = pd.read_excel(mapping_file_path)

    tag_mappings = {}
    for col in df.columns:
        if col.startswith("New-"):
            original_tag = col[4:]
            if original_tag in df.columns:
                tag_mappings[original_tag] = col

    existing_actions = _parse_anonymizer_script_actions(anonymizer_script)

    try:
        root = ET.fromstring(anonymizer_script)
    except ET.ParseError:
        root = ET.Element("script")

    for tag_name in tag_mappings.keys():
        tag_hex = _get_tag_hex_from_keyword(tag_name)

        existing_elem = None
        for elem in root.findall(f".//e[@n='{tag_name}']"):
            existing_elem = elem
            break

        if existing_elem is not None:
            original_action = existing_actions.get(tag_name, "@keep()")
            simple_fallback = _extract_simple_action(original_action)
            new_action = f"@lookup(this,{tag_name},{simple_fallback})"
            existing_elem.text = new_action
        elif tag_hex is not None:
            new_elem = ET.Element("e")
            new_elem.set("en", "T")
            new_elem.set("t", tag_hex)
            new_elem.set("n", tag_name)
            new_elem.text = f"@lookup(this,{tag_name},keep)"
            root.append(new_elem)

    return ET.tostring(root, encoding="unicode")


def _process_mapping_file(
    mapping_file_path: str | None,
    anonymizer_script: str | None,
    lookup_table: str | None,
) -> tuple[str | None, str | None]:
    if lookup_table is not None:
        return lookup_table, anonymizer_script

    if not mapping_file_path:
        return None, anonymizer_script

    lookup_content = _generate_lookup_table_content(mapping_file_path)
    if anonymizer_script is None:
        return lookup_content, None
    modified_script = _merge_mapping_with_script(mapping_file_path, anonymizer_script)

    return lookup_content, modified_script


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------


class ImageDeidStage(PipelineStage, ABC):
    """Stage 2: de-identify DICOM images.

    Reads ``ctx.dicom_input_dir`` and writes to ``ctx.output_dir``. Populates
    ``ctx.images_saved`` and ``ctx.images_quarantined``.
    """


class ImageDeidExecutor(ImageDeidStage):
    """Concrete image-deid stage supporting both CTP and Rust engines.

    ``filter_script`` is the user+query filter *before* default-filter
    composition. This stage applies the Stanford whitelist and the SC/PDF
    blacklist according to ``apply_default_filter_script``, then dispatches
    to the chosen engine.
    """

    def __init__(
        self,
        engine: str,
        anonymizer_script: str | None = None,
        filter_script: str | None = None,
        lookup_table: str | None = None,
        mapping_file_path: str | None = None,
        deid_pixels: bool = False,
        apply_default_filter_script: bool = True,
        sc_pdf_output_dir: str | None = None,
        application_aet: str | None = None,
    ) -> None:
        self.engine = engine
        self.anonymizer_script = anonymizer_script
        self.filter_script = filter_script
        self.lookup_table = lookup_table
        self.mapping_file_path = mapping_file_path
        self.deid_pixels = deid_pixels
        self.apply_default_filter_script = apply_default_filter_script
        self.sc_pdf_output_dir = sc_pdf_output_dir
        self.application_aet = application_aet

    def execute(self, ctx: PipelineContext) -> None:
        input_dir = ctx.dicom_input_dir
        if input_dir is None:
            raise RuntimeError(
                "ImageDeidExecutor requires ctx.dicom_input_dir; run a "
                "GatherStage first."
            )

        engine_label = "dicom-deid-rs (Rust)" if self.engine == "rust" else "CTP (Java)"
        logging.info(f"Image deidentification using {engine_label} engine")

        quarantine_dir = os.path.join(ctx.appdata_dir, "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)

        anonymizer_script = self._resolve_default_anonymizer(self.anonymizer_script)
        processed_lookup_table, processed_anonymizer_script = _process_mapping_file(
            self.mapping_file_path, anonymizer_script, self.lookup_table
        )
        lookup_table = (
            processed_lookup_table
            if processed_lookup_table is not None
            else self.lookup_table
        )
        if processed_anonymizer_script is not None:
            anonymizer_script = processed_anonymizer_script

        # If a gather stage merged a query-derived filter into ``ctx``, use
        # that as the base filter instead of the one passed at construction.
        base_filter_script = (
            ctx.gather_filter_override
            if ctx.gather_filter_override is not None
            else self.filter_script
        )

        final_filter_script = _apply_default_filter_script(
            base_filter_script, self.apply_default_filter_script
        )

        if self.engine == "rust":
            self._run_rust(
                ctx,
                input_dir,
                anonymizer_script,
                final_filter_script,
                lookup_table,
                quarantine_dir,
            )
        else:
            self._run_ctp(
                ctx,
                input_dir,
                anonymizer_script,
                final_filter_script,
                lookup_table,
                quarantine_dir,
            )

        logging.info("Deidentification complete")
        logging.info(
            "Total files processed: "
            f"{format_number_with_commas(ctx.images_saved + ctx.images_quarantined)}"
        )
        logging.info(f"Files saved: {format_number_with_commas(ctx.images_saved)}")
        logging.info(
            f"Files quarantined: {format_number_with_commas(ctx.images_quarantined)}"
        )

    # --- engine dispatchers ---

    def _run_rust(
        self,
        ctx: PipelineContext,
        input_dir: str,
        anonymizer_script: str | None,
        final_filter_script: str | None,
        lookup_table: str | None,
        quarantine_dir: str,
    ) -> None:
        from deid_rs import DeidRsPipeline

        if final_filter_script:
            logging.info(
                "Filter script will be translated to whitelist "
                f"({len(final_filter_script)} chars)"
            )
            logging.debug(f"Filter script content:\n{final_filter_script[:500]}")

        rs_pipeline = DeidRsPipeline(
            input_dir=input_dir,
            output_dir=ctx.output_dir,
            anonymizer_script=anonymizer_script,
            filter_script=final_filter_script,
            sc_pdf_blacklist=_get_sc_pdf_blacklist(self.apply_default_filter_script),
            deid_pixels=self.deid_pixels,
            lookup_table=lookup_table,
            quarantine_dir=quarantine_dir,
        )
        result = rs_pipeline.run()

        _collect_engine_audit_files(ctx.output_dir, ctx.appdata_dir)

        ctx.images_saved = result["num_images_saved"]
        ctx.images_quarantined = result["num_images_quarantined"]

    def _run_ctp(
        self,
        ctx: PipelineContext,
        input_dir: str,
        anonymizer_script: str | None,
        final_filter_script: str | None,
        lookup_table: str | None,
        quarantine_dir: str,
    ) -> None:
        pipeline_type = self._select_pipeline_type()
        ctp_log_level = "DEBUG" if ctx.debug else None

        ctp_kwargs = {
            "pipeline_type": pipeline_type,
            "input_dir": input_dir,
            "output_dir": ctx.output_dir,
            "filter_script": _combine_with_sc_pdf(
                final_filter_script, self.apply_default_filter_script
            ),
            "anonymizer_script": anonymizer_script,
            "lookup_table": lookup_table,
            "log_path": ctx.run_dirs["ctp_log_path"],
            "log_level": ctp_log_level,
            "quarantine_dir": quarantine_dir,
            "sc_pdf_output_dir": self.sc_pdf_output_dir,
        }
        if self.application_aet is not None:
            ctp_kwargs["application_aet"] = self.application_aet

        with CTPPipeline(**ctp_kwargs) as pipeline:
            # Give the Java pipeline time to register ArchiveImportService
            # before we start polling for metrics.
            time.sleep(3)

            save_interval = 5
            last_save_time = 0.0

            while not pipeline.is_complete():
                current_time = time.time()
                if current_time - last_save_time >= save_interval:
                    _save_metadata_files(pipeline, ctx.appdata_dir)
                    _log_progress(pipeline, ctx.total_files)
                    last_save_time = current_time

                time.sleep(1)

            _save_metadata_files(pipeline, ctx.appdata_dir)

            ctx.images_saved = pipeline.metrics.files_saved if pipeline.metrics else 0
            ctx.images_quarantined = (
                pipeline.metrics.files_quarantined if pipeline.metrics else 0
            )

    # --- helpers ---

    def _select_pipeline_type(self) -> str:
        """Pick the CTP pipeline template matching our engine context.

        When we have an ``application_aet`` the caller is the PACS pipeline,
        which uses the ``imagedeid_pacs[_pixel]`` XML template; otherwise it
        is the local pipeline, which uses ``imagedeid_local[_pixel]``.
        """
        if self.application_aet is not None:
            return "imagedeid_pacs_pixel" if self.deid_pixels else "imagedeid_pacs"
        return "imagedeid_local_pixel" if self.deid_pixels else "imagedeid_local"

    def _resolve_default_anonymizer(self, anonymizer_script: str | None) -> str | None:
        """Load the default DicomAnonymizer.script when a mapping file is
        present and no anonymizer was supplied."""
        if anonymizer_script is not None or not self.mapping_file_path:
            return anonymizer_script
        default_script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "ctp",
            "scripts",
            "DicomAnonymizer.script",
        )
        if os.path.exists(default_script_path):
            with open(default_script_path, "r") as f:
                return f.read()
        raise ValueError(
            f"Default anonymizer script not found at {default_script_path}"
        )
