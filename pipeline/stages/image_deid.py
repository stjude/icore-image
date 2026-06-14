import logging
import os
import xml.etree.ElementTree as ET
from abc import ABC

import pandas as pd

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
# Metadata save helpers
# ---------------------------------------------------------------------------


def _collect_engine_audit_files(output_dir: str, appdata_dir: str) -> None:
    """Move engine-generated audit CSVs to appdata_dir as Excel files.

    The Rust engine writes metadata/deid_metadata/linker CSVs into the
    output directory alongside the de-identified DICOMs; we lift them into
    appdata_dir as .xlsx for the app to consume.
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


def generate_sc_pdf_filter() -> str:
    """Build a filter that matches SC, PDF, and other embedded-content types.

    Files matching this filter contain PHI that cannot be safely
    de-identified, so they are routed to the engine's blacklist.

    Note: some malformed DICOM files have a mismatched Transfer Syntax
    (file_meta says Explicit VR but the dataset is Implicit VR), which can
    prevent reading dataset tags like SOPClassUID. We also check
    MediaStorageSOPClassUID [0002,0002] from file_meta as a fallback.
    """
    filter_parts = [
        # SOPClassUID [0008,0016] from dataset
        '[0008,0016].equals("1.2.840.10008.5.1.4.1.1.104.1")',  # Encapsulated PDF
        '[0008,0016].equals("1.2.840.10008.5.1.4.1.1.104.2")',  # Encapsulated CDA
        '[0008,0016].startsWith("1.2.840.10008.5.1.4.1.1.7")',  # Secondary Capture
        '[0008,0016].startsWith("1.2.840.10008.5.1.4.1.1.88")',  # Structured Reports
        '[0008,0016].startsWith("1.2.840.10008.5.1.4.1.1.8")',  # Key Object Selection
        '[0008,0016].startsWith("1.2.840.10008.5.1.4.1.1.11")',  # Presentation States
        # MediaStorageSOPClassUID [0002,0002] from file_meta (malformed fallback)
        '[0002,0002].equals("1.2.840.10008.5.1.4.1.1.104.1")',
        '[0002,0002].equals("1.2.840.10008.5.1.4.1.1.104.2")',
        '[0002,0002].startsWith("1.2.840.10008.5.1.4.1.1.7")',
        '[0002,0002].startsWith("1.2.840.10008.5.1.4.1.1.88")',
        '[0002,0002].startsWith("1.2.840.10008.5.1.4.1.1.8")',
        '[0002,0002].startsWith("1.2.840.10008.5.1.4.1.1.11")',
        # Other indicators
        'BurnedInAnnotation.equalsIgnoreCase("YES")',
        "[0042,0011].exists()",  # EncapsulatedDocument tag
    ]
    return "\n+ ".join(filter_parts)


def _apply_default_filter_script(
    filter_script: str | None, apply_default_filter_script: bool
) -> str | None:
    """Merge the Stanford device-whitelisting filter into the user's filter.

    The Stanford filter (from ``recipes/stanford-filter.script``) acts as a
    whitelist: files must match a known-safe device signature to pass.

    When *apply_default_filter_script* is ``False``, the user's filter is
    returned unchanged.

    **Note:** the SC/PDF exclusion filter (:func:`generate_sc_pdf_filter`)
    is NOT merged here. It is passed to the Rust engine as a separate
    ``%filter blacklist`` section via :func:`_get_sc_pdf_blacklist`.
    """
    if not apply_default_filter_script:
        return filter_script

    result = filter_script

    stanford_filter_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "recipes",
        "stanford-filter.script",
    )
    if os.path.exists(stanford_filter_path):
        with open(stanford_filter_path, "r") as f:
            stanford_filter_content = f.read()
        result = combine_filters(result, stanford_filter_content)
    else:
        logging.warning(f"Stanford filter script not found at {stanford_filter_path}")

    return result


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


def _extract_simple_action(action_string: str) -> str:
    """Return the CTP simple-action name for ``@lookup`` fallback usage.

    Only ``keep``/``remove``/``empty`` are valid fallbacks in CTP's
    ``@lookup(this, KeyType, default)`` form (see docs/ctp-script-format.md).
    For any other action (e.g. ``@hashPtID``, ``@UID``, or a raw value),
    fall back to ``keep`` and warn, so the merged script remains valid.
    """
    simple_actions = ["@keep()", "@remove()", "@empty()"]

    for simple_action in simple_actions:
        if action_string == simple_action:
            return simple_action[1:-2]

    logging.warning(
        "Unsupported CTP action %r cannot be used as an @lookup fallback; "
        "defaulting to 'keep'.",
        action_string,
    )
    return "keep"


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
    """Concrete image-deid stage backed by the dicom-deid-rs engine.

    ``filter_script`` is the user+query filter *before* default-filter
    composition. This stage applies the Stanford whitelist and the SC/PDF
    blacklist according to ``apply_default_filter_script``, then runs the
    Rust engine.
    """

    progress_marker = ("image_deid", "De-identifying metadata and pixels")

    def __init__(
        self,
        anonymizer_script: str | None = None,
        filter_script: str | None = None,
        lookup_table: str | None = None,
        mapping_file_path: str | None = None,
        deid_pixels: bool = False,
        apply_default_filter_script: bool = True,
        sc_pdf_output_dir: str | None = None,
    ) -> None:
        self.anonymizer_script = anonymizer_script
        self.filter_script = filter_script
        self.lookup_table = lookup_table
        self.mapping_file_path = mapping_file_path
        self.deid_pixels = deid_pixels
        self.apply_default_filter_script = apply_default_filter_script
        self.sc_pdf_output_dir = sc_pdf_output_dir

    def execute(self, ctx: PipelineContext) -> None:
        from deid_rs import DeidRsPipeline

        input_dir = ctx.dicom_input_dir
        if input_dir is None:
            raise RuntimeError(
                "ImageDeidExecutor requires ctx.dicom_input_dir; run a "
                "GatherStage first."
            )

        logging.info("Image deidentification using dicom-deid-rs (Rust) engine")

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

        def on_progress(done: int, total: int) -> None:
            if not ctx.progress:
                return
            if total:
                status = (
                    f"Processing {format_number_with_commas(done)} of "
                    f"{format_number_with_commas(total)} images"
                )
                ctx.progress.update("image_deid", done / total, status)
            else:
                ctx.progress.update("image_deid", 0.0, "Processing images…")

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
            progress_callback=on_progress,
        )
        result = rs_pipeline.run()

        _collect_engine_audit_files(ctx.output_dir, ctx.appdata_dir)

        ctx.images_saved = result["num_images_saved"]
        ctx.images_quarantined = result["num_images_quarantined"]

        logging.info("Deidentification complete")
        logging.info(
            "Total files processed: "
            f"{format_number_with_commas(ctx.images_saved + ctx.images_quarantined)}"
        )
        logging.info(f"Files saved: {format_number_with_commas(ctx.images_saved)}")
        logging.info(
            f"Files quarantined: {format_number_with_commas(ctx.images_quarantined)}"
        )

    def _resolve_default_anonymizer(self, anonymizer_script: str | None) -> str | None:
        """Load the default DicomAnonymizer.script when a mapping file is
        present and no anonymizer was supplied."""
        if anonymizer_script is not None or not self.mapping_file_path:
            return anonymizer_script
        default_script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "recipes",
            "DicomAnonymizer.script",
        )
        if os.path.exists(default_script_path):
            with open(default_script_path, "r") as f:
                return f.read()
        raise ValueError(
            f"Default anonymizer script not found at {default_script_path}"
        )
