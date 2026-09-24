import logging
import os
import shutil
from collections.abc import MutableSequence

import pydicom
from pydicom.datadict import tag_for_keyword

from pipeline.base import PipelineStage
from pipeline.context import PipelineContext
from utils import format_number_with_commas

# A filter is ``{"tag": <DICOM keyword>, "action": <name>, "value": <str>}``
# exactly as the "Query Options" UI submits it. Actions mirror the CTP filter
# predicates the UI offers, optionally prefixed with ``not_``.
Filter = dict[str, str]

_ACTIONS = {
    "equals",
    "contains",
    "startsWith",
    "endsWith",
    "isLessThan",
    "isGreaterThan",
}


def validate_filters(filters: list[Filter]) -> None:
    """Raise ``ValueError`` for an unknown DICOM keyword or filter action."""
    for f in filters:
        if tag_for_keyword(f["tag"]) is None:
            raise ValueError(f"Unknown DICOM keyword in filter: {f['tag']!r}")
        action = f["action"].removeprefix("not_").removesuffix("IgnoreCase")
        if action not in _ACTIONS:
            raise ValueError(f"Unsupported filter action: {f['action']!r}")


def _element_string(ds: pydicom.Dataset, keyword: str) -> str:
    """Element value as CTP sees it: multi-values joined by ``\\``, absent → ``""``."""
    value = ds.get(keyword)
    if value is None:
        return ""
    if isinstance(value, MutableSequence):
        return "\\".join(str(v) for v in value)
    return str(value)


def _evaluate(ds: pydicom.Dataset, f: Filter) -> bool:
    action = f["action"]
    negate = action.startswith("not_")
    action = action.removeprefix("not_")
    actual = _element_string(ds, f["tag"])
    expected = f["value"]
    if action.endswith("IgnoreCase"):
        action = action.removesuffix("IgnoreCase")
        actual, expected = actual.lower(), expected.lower()

    if action in ("isLessThan", "isGreaterThan"):
        try:
            number = float(actual.split("\\")[0].strip())
            threshold = float(expected)
        except ValueError:
            result = False
        else:
            result = (
                number < threshold if action == "isLessThan" else number > threshold
            )
    elif action == "equals":
        result = actual == expected
    elif action == "contains":
        result = expected in actual
    elif action == "startsWith":
        result = actual.startswith(expected)
    else:
        result = actual.endswith(expected)
    return result != negate


def matches_filters(
    ds: pydicom.Dataset,
    general_filters: list[Filter],
    modality_filters: dict[str, list[Filter]],
) -> bool:
    """Same truth table as :func:`deid.grammar.generate_filters_string`:
    every general filter must hold, and (when any modality is selected) every
    filter of at least one selected modality must hold."""
    if not all(_evaluate(ds, f) for f in general_filters):
        return False
    if not modality_filters:
        return True
    return any(
        all(_evaluate(ds, f) for f in filters) for filters in modality_filters.values()
    )


class DicomFilterStage(PipelineStage):
    """Stage 2: quarantine retrieved files that fail the user's filters.

    Works in place on ``ctx.dicom_input_dir``: files that pass stay where
    they are, untouched; files that fail (or cannot be read as DICOM) are
    moved to ``<appdata_dir>/quarantine`` preserving their relative path.
    Populates ``ctx.images_saved`` and ``ctx.images_quarantined``.
    """

    progress_marker = ("filter", "Applying image filters")

    def __init__(
        self,
        general_filters: list[Filter],
        modality_filters: dict[str, list[Filter]],
    ) -> None:
        validate_filters(general_filters)
        for filters in modality_filters.values():
            validate_filters(filters)
        self.general_filters = general_filters
        self.modality_filters = modality_filters

    def execute(self, ctx: PipelineContext) -> None:
        input_dir = ctx.dicom_input_dir
        if input_dir is None:
            raise RuntimeError(
                "DicomFilterStage requires ctx.dicom_input_dir; run a GatherStage first."
            )
        quarantine_dir = os.path.join(ctx.appdata_dir, "quarantine")

        paths = [
            os.path.join(root, name)
            for root, _dirs, files in os.walk(input_dir)
            for name in files
        ]
        logging.info(
            f"Applying image filters to {format_number_with_commas(len(paths))} files"
        )
        kept = quarantined = 0
        for i, path in enumerate(paths):
            if ctx.progress:
                ctx.progress.update(
                    "filter", i / len(paths), f"Filtering image {i + 1} of {len(paths)}"
                )
            try:
                ds = pydicom.dcmread(path, stop_before_pixels=True)
                passed = matches_filters(
                    ds, self.general_filters, self.modality_filters
                )
            except Exception as e:
                logging.warning(f"Quarantining unreadable file {path}: {e}")
                passed = False
            if passed:
                kept += 1
                continue
            dest = os.path.join(quarantine_dir, os.path.relpath(path, input_dir))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(path, dest)
            quarantined += 1

        ctx.images_saved = kept
        ctx.images_quarantined = quarantined
        logging.info(f"Files kept: {format_number_with_commas(kept)}")
        logging.info(f"Files quarantined: {format_number_with_commas(quarantined)}")
