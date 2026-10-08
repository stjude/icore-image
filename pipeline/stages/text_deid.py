import logging
import os
import re
import string
import sys
from abc import ABC
from datetime import date, datetime, timedelta
from typing import Any, cast

import pandas as pd
from presidio_analyzer import (
    AnalyzerEngine,
    Pattern,
    PatternRecognizer,
)
from presidio_analyzer.nlp_engine import NlpEngine, NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig, RecognizerResult

from pipeline.base import PipelineStage
from pipeline.context import PipelineContext

logging.getLogger("presidio-analyzer").setLevel(logging.ERROR)
logging.getLogger("presidio-anonymizer").setLevel(logging.ERROR)


NLM_PRESERVE_MEDICAL = {
    "mediastinum",
    "ventricles",
    "paraspinal",
    "pleural",
    "calvarium",
    "conus",
    "medullaris",
    "schmorl",
    "multiplanar",
    "parenchyma",
    "md",
    "er",
    "copd",
    "ap",
    "lateral",
    "icu",
    "npo",
    "iv",
    "pneumonia",
    "pneumothorax",
    "effusion",
    "opacity",
    "consolidation",
    "mother",
    "father",
    "son",
    "daughter",
    "wife",
    "husband",
    "degrees",
    "cm",
    "mm",
    "ml",
    "mg",
    "dl",
    "kg",
    "ct",
    "mri",
    "xray",
    "x-ray",
    "pet",
    "ultrasound",
    "doppler",
    "head",
    "brain",
    "spine",
    "lumbar",
    "thoracic",
    "cervical",
    "chest",
    "abdomen",
    "pelvis",
    "cardiac",
    "heart",
    "liver",
    "kidney",
    "spleen",
    "gray",
    "grey",
    "white",
    "matter",
    "axial",
    "sagittal",
    "coronal",
    "vertex",
    "orbits",
    "globes",
    "extraocular",
    "paranasal",
    "sinuses",
    "maxillary",
    "frontal",
    "csf",
    "hydrocephalus",
    "intracranial",
    "hemorrhage",
    "infarction",
    "mass",
    "lesion",
    "fracture",
    "edema",
    "herniation",
    "stenosis",
    "disc",
    "vertebral",
    "spinal",
    "cord",
    "canal",
    "foramina",
    "pedicles",
    "sacrum",
    "coccyx",
    "iliac",
    "femur",
    "tibia",
    "fibula",
    "humerus",
    "radius",
    "ulna",
    "scaphoid",
    "aortic",
    "mitral",
    "tricuspid",
    "ventricular",
    "atrial",
    "myocardium",
    "pericardium",
    "endocardium",
    "septum",
    "chamber",
    "gallbladder",
    "bowel",
    "stomach",
    "duodenum",
    "colon",
    "appendix",
    "bladder",
    "prostate",
    "uterus",
    "ovary",
    "adnexa",
    "retroperitoneum",
    "breast",
    "mammogram",
    "angiogram",
    "venogram",
    "arthrogram",
    "myelogram",
    "cholangiogram",
    "intra-articular",
    "extra-axial",
    "patient",
    "male",
    "female",
    "year",
    "years",
    "old",
    "age",
    "exam",
    "examination",
    "study",
    "report",
    "findings",
    "impression",
    "history",
    "indication",
    "comparison",
    "technique",
    "protocol",
    "electronically",
    "signed",
    "dictated",
    "transcribed",
    "radiologist",
    "headaches",
    "headache",
    "dizziness",
    "pain",
    "symptoms",
    "soft",
    "bones",
    "joints",
    "tissues",
    "muscles",
    "tendons",
    "ligaments",
    "parenchyma",
    "gray-white",
    "grey-white",
    "gray",
    "grey",
    "white",
    "paranasal",
    "sinuses",
    "maxillary",
    "frontal",
    "ethmoid",
}

NLM_REDACT_NAMES = {"pine"}

# preserving the DICOM naming
LINKER_KEYS = (
    (
        ("AccessionNumber", "Accession Number", "Accession #", "Accession No"),
        "Original AccessionNumber",
    ),
    (("StudyInstanceUID", "Study Instance UID"), "Original StudyInstanceUID"),
)
REPORT_PATIENT_ID_COLUMNS = (
    "PatientID",
    "Patient ID",
    "MRN",
    "MedicalRecordNumber",
    "Medical Record Number",
    "Patient MRN",
)
IDENTIFIER_COLUMNS = frozenset(
    {
        "PatientName",
        "Patient Name",
        *REPORT_PATIENT_ID_COLUMNS,
        "AccessionNumber",
        "Accession Number",
        "Accession #",
        "Accession No",
    }
)

LINKER_VALUE_PAIRS = (
    ("Original PatientName", "Deidentified PatientName"),
    ("Original PatientID", "Deidentified PatientID"),
    ("Original AccessionNumber", "Deidentified AccessionNumber"),
)

_NUMERIC_DATE_RE = re.compile(
    r"(?<!\d)(?:(?P<year_first>\d{4})(?P<yf_sep>[-/.])"
    r"(?P<yf_month>\d{1,2})(?P=yf_sep)(?P<yf_day>\d{1,2})|"
    r"(?P<month_first>\d{1,2})(?P<mf_sep>[-/.])"
    r"(?P<mf_day>\d{1,2})(?P=mf_sep)(?P<mf_year>\d{2,4})|"
    r"(?P<compact>\d{8}))(?!\d)"
)
_TEXT_DATE_RE = re.compile(
    r"(?i)(?<![A-Za-z])(?P<month>January|February|March|April|May|June|"
    r"July|August|September|October|November|December|Jan|Feb|Mar|Apr|"
    r"Jun|Jul|Aug|Sept?|Oct|Nov|Dec)(?P<month_period>\.?)"
    r"(?P<space1>\s+)(?P<day>\d{1,2})(?P<ordinal>st|nd|rd|th)?(?P<comma>,?)"
    r"(?P<space2>\s+)(?P<year>\d{4})(?!\d)"
)
_DAY_FIRST_TEXT_DATE_RE = re.compile(
    r"(?i)(?<!\d)(?P<day>\d{1,2})(?P<sep1>[\s,./-]+)"
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|"
    r"Aug|Sept?|Oct|Nov|Dec)(?P<month_period>\.?)"
    r"(?P<sep2>[\s,./-]+)(?P<year>\d{4})(?!\d)"
)
_MONTH_FORMATS = {
    "january": (1, "%B"), "jan": (1, "%b"),
    "february": (2, "%B"), "feb": (2, "%b"),
    "march": (3, "%B"), "mar": (3, "%b"),
    "april": (4, "%B"), "apr": (4, "%b"),
    "may": (5, "%B"), "june": (6, "%B"), "jun": (6, "%b"),
    "july": (7, "%B"), "jul": (7, "%b"),
    "august": (8, "%B"), "aug": (8, "%b"),
    "september": (9, "%B"), "sept": (9, "%b"), "sep": (9, "%b"),
    "october": (10, "%B"), "oct": (10, "%b"),
    "november": (11, "%B"), "nov": (11, "%b"),
    "december": (12, "%B"), "dec": (12, "%b"),
}


def _clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_identifier(value: Any) -> str:
    """Normalize case and common separators for linker-key comparisons."""
    cleaned = _clean(value).casefold()
    if re.fullmatch(r"\d+\.0+", cleaned):
        cleaned = cleaned.split(".", maxsplit=1)[0]
    return "".join(char for char in cleaned if char.isalnum())


def _identifier_pattern(value: str) -> str:
    """Match an identifier despite common spaces/punctuation between chars."""
    chars = [char for char in value if char.isalnum()]
    if not chars:
        return r"(?!)"
    body = r"[\s._/-]*".join(re.escape(char) for char in chars)
    # Excel may stringify an integer identifier as e.g. 12345.0.
    return rf"(?<![A-Za-z0-9]){body}(?:\.0+)?(?![A-Za-z0-9])"


def _patient_name_variants(value: str) -> list[str]:
    """Return full-name orderings commonly used for DICOM PN in report text."""
    variants = [value.strip()]
    parts = [part.strip() for part in re.split(r"[\^,]+", value) if part.strip()]
    if len(parts) >= 2:
        family, given, *middle = parts
        given_middle = " ".join([given, *middle]).strip()
        variants.extend([f"{family} {given_middle}", f"{given_middle} {family}"])
        if middle:
            variants.extend([f"{family} {given}", f"{given} {family}"])
        variants.append(f"{family}, {given_middle}")
    return list(dict.fromkeys(item for item in variants if item))


def _replacement_patterns(value: str) -> list[str]:
    """Build conservative text patterns for an identifier or DICOM PN."""
    variants = _patient_name_variants(value) if ("^" in value or "," in value) else [value]
    patterns = []
    for variant in variants:
        if "^" in value or "," in value:
            words = [word for word in re.split(r"[\s,^]+", variant.strip()) if word]
            if not words:
                continue
            body = r"[\s,^\.]+".join(re.escape(word) for word in words)
            patterns.append(rf"(?<!\w){body}(?!\w)")
        else:
            patterns.append(_identifier_pattern(variant))
    return patterns


def _shift_report_dates(
    text: str, shift_days: int, row_index: int
) -> tuple[str, dict[str, str]]:
    """Protect recognized report dates after shifting them by the DICOM DATEINC."""
    restore: dict[str, str] = {}
    token_index = 0

    def ordinal_suffix(day_number: int) -> str:
        if 11 <= day_number % 100 <= 13:
            return "th"
        return {1: "st", 2: "nd", 3: "rd"}.get(day_number % 10, "th")

    def protect(shifted_text: str) -> str:
        nonlocal token_index
        token = f"ICOREDATEDTOKEN{row_index}_{token_index}END"
        token_index += 1
        restore[token] = shifted_text
        return token

    def shift_numeric(match: re.Match[str]) -> str:
        groups = match.groupdict()
        try:
            if groups["compact"]:
                original = datetime.strptime(groups["compact"], "%Y%m%d").date()
                shifted = original + timedelta(days=shift_days)
                replacement = shifted.strftime("%Y%m%d")
            elif groups["year_first"]:
                original = date(
                    int(groups["year_first"]),
                    int(groups["yf_month"]),
                    int(groups["yf_day"]),
                )
                shifted = original + timedelta(days=shift_days)
                replacement = (
                    f"{shifted.year:04d}{groups['yf_sep']}"
                    f"{shifted.month:0{len(groups['yf_month'])}d}{groups['yf_sep']}"
                    f"{shifted.day:0{len(groups['yf_day'])}d}"
                )
            else:
                year_text = groups["mf_year"]
                year = (
                    datetime.strptime(year_text, "%y").year
                    if len(year_text) == 2
                    else int(year_text)
                )
                original = date(year, int(groups["month_first"]), int(groups["mf_day"]))
                shifted = original + timedelta(days=shift_days)
                shifted_year = (
                    shifted.strftime("%y") if len(year_text) == 2 else f"{shifted.year:04d}"
                )
                replacement = (
                    f"{shifted.month:0{len(groups['month_first'])}d}{groups['mf_sep']}"
                    f"{shifted.day:0{len(groups['mf_day'])}d}{groups['mf_sep']}"
                    f"{shifted_year}"
                )
        except ValueError:
            return match.group(0)
        return protect(replacement)

    text = _NUMERIC_DATE_RE.sub(shift_numeric, text)

    def shift_textual(match: re.Match[str]) -> str:
        month_text = match.group("month")
        month_key = month_text.rstrip(".").casefold()
        month, month_format = _MONTH_FORMATS[month_key]
        try:
            original = date(int(match.group("year")), month, int(match.group("day")))
        except ValueError:
            return match.group(0)
        shifted = original + timedelta(days=shift_days)
        ordinal = (
            ordinal_suffix(shifted.day) if match.group("ordinal") else ""
        )
        replacement = (
            f"{shifted.strftime(month_format)}{match.group('month_period')}"
            f"{match.group('space1')}"
            f"{shifted.day:0{len(match.group('day'))}d}"
            f"{ordinal}"
            f"{match.group('comma')}{match.group('space2')}{shifted.year:04d}"
        )
        return protect(replacement)

    text = _TEXT_DATE_RE.sub(shift_textual, text)

    def shift_day_first_textual(match: re.Match[str]) -> str:
        month_text = match.group("month")
        month_key = month_text.casefold()
        month, month_format = _MONTH_FORMATS[month_key]
        try:
            original = date(int(match.group("year")), month, int(match.group("day")))
        except ValueError:
            return match.group(0)
        shifted = original + timedelta(days=shift_days)
        replacement = (
            f"{shifted.day:0{len(match.group('day'))}d}"
            f"{match.group('sep1')}{shifted.strftime(month_format)}"
            f"{match.group('month_period')}"
            f"{match.group('sep2')}{shifted.year:04d}"
        )
        return protect(replacement)

    text = _DAY_FIRST_TEXT_DATE_RE.sub(shift_day_first_textual, text)

    return text, restore


def load_dicom_linker(path: str) -> pd.DataFrame:
    linker = pd.read_excel(path, dtype=str).fillna("")
    required = {
        "Original PatientID",
        "Original PatientName",
        "Original AccessionNumber",
        "Original StudyInstanceUID",
        "Deidentified PatientID",
        "Deidentified PatientName",
        "Deidentified AccessionNumber",
    }
    missing = required - set(linker.columns)
    if missing:
        raise ValueError(f"linker.xlsx is missing columns: {sorted(missing)}")
    return linker


def _row_replacements(match: pd.Series) -> dict[str, str]:
    replacements = {}
    for old_column, new_column in LINKER_VALUE_PAIRS:
        old = _clean(match[old_column])
        new = _clean(match[new_column])
        if old and new and old != new:
            replacements[old] = new
    return replacements


def replacements_for_report(
    row: pd.Series,
    linker: pd.DataFrame,
) -> dict[str, str]:
    # Prefer study-specific keys.
    for report_columns, linker_key in LINKER_KEYS:
        for report_key in report_columns:
            if report_key not in row.index:
                continue

            original = _clean(row.get(report_key))
            if not original:
                continue

            normalized = _normalize_identifier(original)
            if not normalized:
                continue
            matches = linker[
                linker[linker_key].map(_normalize_identifier) == normalized
            ]
            if len(matches) == 1:
                return _row_replacements(matches.iloc[0])

    # PatientID is only safe when it identifies exactly one linker row.
    for report_key in REPORT_PATIENT_ID_COLUMNS:
        if report_key not in row.index:
            continue
        original = _clean(row.get(report_key))
        if original:
            normalized = _normalize_identifier(original)
            if not normalized:
                continue
            matches = linker[
                linker["Original PatientID"].map(_normalize_identifier) == normalized
            ]
            if len(matches) == 1:
                return _row_replacements(matches.iloc[0])

    return {}


def create_nlp_engine() -> NlpEngine:
    if getattr(sys, "frozen", False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        models_base_dir = os.path.join(bundle_dir, "_internal", "en_core_web_sm")
        model_subdir = None
        if os.path.isdir(models_base_dir):
            for entry in os.listdir(models_base_dir):
                full_path = os.path.join(models_base_dir, entry)
                if entry.startswith("en_core_web_sm-") and os.path.isdir(full_path):
                    model_subdir = full_path
                    break
        if model_subdir is None:
            raise FileNotFoundError(
                f"en_core_web_sm model not found in {models_base_dir}"
            )
        model_path = model_subdir
        from presidio_analyzer.nlp_engine import SpacyNlpEngine

        nlp_engine = SpacyNlpEngine(
            models=[{"lang_code": "en", "model_name": model_path}]
        )
    else:
        configuration = {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
        }
        provider = NlpEngineProvider(nlp_configuration=configuration)
        nlp_engine = provider.create_engine()
    return nlp_engine


def create_analyzer_engine() -> AnalyzerEngine:
    nlp_engine = create_nlp_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])

    age_90 = PatternRecognizer(
        supported_entity="AGE90PLUS",
        name="age90",
        patterns=[
            Pattern(
                name="a1", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*years?\s*old)", score=1.0
            ),
            Pattern(
                name="a2", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*year\s*old)", score=1.0
            ),
            Pattern(name="a3", regex=r"(9[0-9]|[1-9]\d{2,})(?=\-year\-old)", score=1.0),
            Pattern(name="a4", regex=r"(?<=age:\s)(9[0-9]|[1-9]\d{2,})\b", score=1.0),
            Pattern(name="a5", regex=r"(?<=Age:\s)(9[0-9]|[1-9]\d{2,})\b", score=1.0),
            Pattern(
                name="a6",
                regex=r"(?<=is\s)(9[0-9]|[1-9]\d{2,})(?=\s*years?\s*old)",
                score=1.0,
            ),
            Pattern(name="a7", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*y/o\b)", score=1.0),
            Pattern(name="a8", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*y\.o\.)", score=1.0),
            Pattern(name="a9", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*yo\b)", score=1.0),
        ],
    )

    date_patterns = PatternRecognizer(
        supported_entity="DATEPATTERN",
        name="dates",
        patterns=[
            Pattern(name="d1", regex=r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", score=0.95),
            Pattern(name="d2", regex=r"\b\d{1,2}-\d{1,2}-\d{4}\b", score=0.94),
            Pattern(
                name="d3",
                regex=r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
                score=0.98,
            ),
            Pattern(
                name="d4",
                regex=r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}\b",
                score=0.96,
            ),
        ],
    )

    name_extras_recognizer = PatternRecognizer(
        supported_entity="NAMEPERSON",
        name="names",
        global_regex_flags=re.DOTALL | re.MULTILINE,
        patterns=[
            Pattern(name="n1", regex=r"\b[A-Z]\.[A-Z]\.\b", score=0.85),
            Pattern(name="n2", regex=r"\b[A-Z]\.\s[A-Z]\.\s[A-Z][a-z]+\b", score=0.9),
            Pattern(
                name="n3",
                regex=r"\b(?!\d+\-year)(?!\d+\-day)(?!follow\-)(?!year\-old)[A-Z][a-z]{2,}\-[A-Z][a-z]{2,}\b",
                score=0.85,
            ),
            Pattern(name="n4", regex=r"\bPine\b", score=0.8),
            Pattern(name="n5", regex=r"\bPatient\s+[A-Z]\.[A-Z]\.", score=0.9),
            Pattern(name="n6", regex=r"\bThe(?=\s+MD\b)", score=0.75),
            Pattern(
                name="n7",
                regex=r"(?<=(?i:dr\.\s))([A-Z][A-Z\-]{2,}(?:\s[A-Z][A-Z\-]{2,})?)\b",
                score=0.85,
            ),
        ],
    )

    medical_record_number_recognizer = PatternRecognizer(
        supported_entity="ALPHANUMERICID",
        name="mrn",
        patterns=[
            Pattern(name="m1", regex=r"\b\d{7}(?!\d)\b", score=0.97),
            Pattern(name="m2", regex=r"\b\d{8}(?!\d)\b", score=0.97),
            Pattern(name="m3", regex=r"\b\d{9}\b", score=0.97),
            Pattern(name="m4", regex=r"\b\d{10}\b", score=0.97),
            Pattern(name="m5", regex=r"\b[A-Z]{1,6}-\d{6,10}\b", score=0.98),
            Pattern(name="m6", regex=r"\b[A-Z]\d{7,9}\b", score=0.97),
            Pattern(name="m7", regex=r"\b[A-Z]{2,3}\d{4,8}\b", score=0.97),
            Pattern(name="m8", regex=r"\b[A-Z0-9]+-\d+-[A-Z0-9]+-\d+\b", score=0.98),
            Pattern(name="m9", regex=r"\b\d{2}-\d{2}-\d{4}\b", score=0.98),
        ],
    )

    phone_number_recognizer = PatternRecognizer(
        supported_entity="PHONENUMBER",
        name="phone",
        patterns=[
            Pattern(name="p1", regex=r"\(\d{3}\)\s*\d{3}-\d{4}", score=0.99),
            Pattern(name="p2", regex=r"\b\d{3}-\d{3}-\d{4}\b", score=0.98),
            Pattern(name="p3", regex=r"\b\d{3}\.\d{3}\.\d{4}\b", score=0.98),
        ],
    )

    address_recognizer = PatternRecognizer(
        supported_entity="LOCATION",
        name="address",
        patterns=[
            Pattern(
                name="addr1",
                regex=r"\b\d+\s+[A-Z][a-z]+\s+(?:Ave|St|Street|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Court|Ct|Way|Circle|Cir),?\s+[A-Z][a-z]+,?\s+[A-Z]{2}(?:\s+\d{5})?\b",
                score=0.95,
            ),
            Pattern(
                name="addr2",
                regex=r"Address:\s+\d+\s+[A-Z][a-z]+\s+(?:Ave|St|Street|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd),?\s+[A-Z][a-z]+,?\s+[A-Z]{2}\b",
                score=0.96,
            ),
        ],
    )

    ssn = PatternRecognizer(
        supported_entity="SSNUMBER",
        name="ssn",
        patterns=[
            Pattern(name="s1", regex=r"\b\d{3}-\d{2}-\d{4}\b", score=0.98),
        ],
    )

    email = PatternRecognizer(
        supported_entity="EMAILADDR",
        name="email",
        patterns=[
            Pattern(
                name="e1",
                regex=r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",
                score=0.98,
            ),
        ],
    )

    ip_addr = PatternRecognizer(
        supported_entity="IPADDR",
        name="ip",
        patterns=[
            Pattern(
                name="i1", regex=r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", score=0.9
            ),
        ],
    )

    analyzer.registry.add_recognizer(age_90)
    analyzer.registry.add_recognizer(date_patterns)
    analyzer.registry.add_recognizer(name_extras_recognizer)
    analyzer.registry.add_recognizer(medical_record_number_recognizer)
    analyzer.registry.add_recognizer(phone_number_recognizer)
    analyzer.registry.add_recognizer(address_recognizer)
    analyzer.registry.add_recognizer(ssn)
    analyzer.registry.add_recognizer(email)
    analyzer.registry.add_recognizer(ip_addr)

    return analyzer


def _build_medical_preserve(whitelist: list[str]) -> set[str]:
    medical_preserve = NLM_PRESERVE_MEDICAL.copy()
    if whitelist:
        medical_preserve.update(w.lower() for w in whitelist)
    return medical_preserve


def _register_blacklist_recognizers(
    analyzer: AnalyzerEngine, blacklist: list[str]
) -> None:
    for item in blacklist:
        rec = PatternRecognizer(
            supported_entity="BLACKLIST",
            name=f"bl_{hash(item)}",
            patterns=[Pattern(name="bl", regex=re.escape(item), score=1.0)],
        )
        analyzer.registry.add_recognizer(rec)


def scrub(
    data: list[Any],
    analyzer: AnalyzerEngine,
    anonymizer: AnonymizerEngine,
    medical_preserve: set[str],
    progress_callback=None,
    row_replacements: list[dict[str, str]] | None = None,
    date_shift_days: int | None = None,
) -> list[str]:
    entities = [
        "PERSON",
        "DATE_TIME",
        "LOCATION",
        "PHONE_NUMBER",
        "EMAIL_ADDRESS",
        "US_SSN",
        "AGE90PLUS",
        "DATEPATTERN",
        "NAMEPERSON",
        "ALPHANUMERICID",
        "PHONENUMBER",
        "SSNUMBER",
        "EMAILADDR",
        "IPADDR",
        "BLACKLIST",
    ]

    operators = {
        "PERSON": OperatorConfig("replace", {"new_value": "[PERSONALNAME]"}),
        "NAMEPERSON": OperatorConfig("replace", {"new_value": "[PERSONALNAME]"}),
        "DATE_TIME": OperatorConfig("replace", {"new_value": "[DATE]"}),
        "DATEPATTERN": OperatorConfig("replace", {"new_value": "[DATE]"}),
        "LOCATION": OperatorConfig("replace", {"new_value": "[ADDRESS]"}),
        "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "US_SSN": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "AGE90PLUS": OperatorConfig("replace", {"new_value": "[AGE90+]"}),
        "ALPHANUMERICID": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "PHONENUMBER": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "SSNUMBER": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "EMAILADDR": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "IPADDR": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "BLACKLIST": OperatorConfig("replace", {"new_value": "[REDACTED]"}),
    }

    age_under_90 = re.compile(r"\b([1-8]?\d)\s*[-\s]*(year|yr)s?[-\s]*old\b", re.I)
    duration = re.compile(
        r"\b\d+\s+(weeks?|months?|days?|hours?|minutes?|seconds?)\b", re.I
    )
    time_12h = re.compile(r"\b\d{1,2}(am|pm)\b", re.I)
    time_colon = re.compile(r"\b\d{1,2}:\d{2}(\s*[AP]M)?\b", re.I)
    gestational = re.compile(r"\b\d{1,2}\s*weeks?\s*\d*\s*days?\b", re.I)
    measurement = re.compile(
        r"\b\d+(?:\.\d+)?\s*(cm|mm|ml|mg|kg|lb|oz|degrees?|dl|percent|%)\b", re.I
    )
    blood_pressure = re.compile(r"\b\d{2,3}/\d{2,3}\b")
    year_only = re.compile(r"\b(19|20)\d{2}\b")
    relative_date = re.compile(r"\b(yesterday|today|tomorrow)\b", re.I)
    date_adjective = re.compile(
        r"\b(daily|weekly|monthly|annually|annual|nightly|biweekly|overnight)\b",
        re.I,
    )
    all_zeros = re.compile(r"\b0{7,}\b")
    hospital = re.compile(
        r"\b(Hospital|Medical Center|Clinic|Healthcare|Health System)\b", re.I
    )
    url = re.compile(r"\bhttps?://[^\s]+\b")


    if row_replacements is None:
        row_replacements = [{} for _ in data]
    elif len(row_replacements) != len(data):
        raise ValueError("row_replacements must have one mapping per report")
    results = []
    for index, text_item in enumerate(data):
        if progress_callback:
            progress_callback()
        text = str(text_item) if text_item is not None else ""
        restore_tokens: dict[str, str] = {}
        replacements = row_replacements[index]

        for token_index, old in enumerate(
            sorted(replacements, key=len, reverse=True)
        ):
            token = f"ICOREPHIPROTECTEDTOKEN{index}_{token_index}END"
            matched = False
            for pattern in _replacement_patterns(old):
                text, count = re.subn(pattern, token, text, flags=re.IGNORECASE)
                matched = matched or count > 0
            if matched:
                restore_tokens[token] = replacements[old]

        # Preserve legacy cleanup after matching so Unicode names can still
        # match the source value stored in DICOM.
        text = "".join(c for c in text if c in string.printable)

        if date_shift_days is not None:
            text, shifted_dates = _shift_report_dates(text, date_shift_days, index)
            restore_tokens.update(shifted_dates)

        results_analysis = analyzer.analyze(
            text=text, entities=entities, language="en", score_threshold=0.5
        )

        protected_spans = [
            (match.start(), match.end())
            for token in restore_tokens
            for match in re.finditer(re.escape(token), text)
        ]
        filtered = []
        for result in results_analysis:
            # Linker replacements and shifted dates are already de-identified.
            # Presidio can misclassify their temporary tokens as names, dates,
            # or addresses; preserve any result that overlaps one of them so
            # the exact DICOM-linked value can be restored below.
            if any(
                result.start < protected_end and protected_start < result.end
                for protected_start, protected_end in protected_spans
            ):
                continue

            detected = text[result.start : result.end]
            lower = detected.lower()

            if lower in NLM_REDACT_NAMES and result.entity_type == "PERSON":
                filtered.append(result)
                continue

            if lower in medical_preserve:
                continue

            if result.entity_type == "PERSON":
                detected_lower = detected.lower()
                if any(
                    term in detected_lower
                    for term in [
                        "parenchyma",
                        "gray-white",
                        "grey-white",
                        "paranasal",
                        "sinuses",
                        "brain",
                        "ct head",
                        "mri brain",
                    ]
                ):
                    continue
                if detected_lower in [
                    "ct",
                    "mri",
                    "x-ray",
                    "xray",
                    "head",
                    "brain",
                    "spine",
                    "chest",
                ]:
                    continue

            if re.match(
                r"\d+-year-old", text[max(0, result.start - 10) : result.end + 1]
            ):
                continue

            detected_words_list = detected.split()
            if result.entity_type == "PERSON":
                if any(
                    word.lower() in ["record", "chart", "visit", "case"]
                    for word in detected_words_list
                ):
                    if len(detected_words_list) == 1:
                        continue

            if all_zeros.match(detected):
                continue
            if re.match(r"^(\d)\1{6,}$", detected):
                continue
            if re.search(r"(\d)\1{3,}$", detected) and len(detected) == 7:
                continue

            if result.entity_type == "LOCATION" and hospital.search(detected):
                continue

            if url.match(detected):
                continue

            if result.entity_type == "DATE_TIME":
                if re.search(r"\b(9[0-9]|[1-9]\d{2,})\s*years?\s*old", detected):
                    continue
                if age_under_90.search(detected):
                    continue
                if duration.search(detected):
                    continue
                if time_12h.match(detected):
                    continue
                if time_colon.match(detected):
                    continue
                if year_only.fullmatch(detected):
                    continue
                if relative_date.search(detected):
                    continue
                if gestational.match(detected):
                    continue
                if date_adjective.search(detected):
                    continue

            if blood_pressure.fullmatch(detected):
                continue

            if measurement.search(detected):
                continue

            if re.match(r"^\d{1,2}\s+years?$", detected):
                continue

            if result.entity_type in ["ALPHANUMERICID", "IPADDR"] and re.match(
                r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", detected
            ):
                parts = [int(p) for p in detected.split(".")]
                if all(p < 20 for p in parts):
                    continue

            filtered.append(result)

        filtered = sorted(filtered, key=lambda x: x.start)
        anonymized = anonymizer.anonymize(
            text=text,
            analyzer_results=cast(list[RecognizerResult], filtered),
            operators=operators,
        )
        result = anonymized.text
        for token, dicom_value in restore_tokens.items():
            result = result.replace(token, dicom_value)
        results.append(result)

    return results


class TextDeidStage(PipelineStage, ABC):
    """Stage 3: scrub an Excel file of PHI."""


class PresidioTextDeid(TextDeidStage):
    """Concrete text-deid stage backed by Presidio + custom recognizers."""

    progress_marker = ("text_deid", "De-identifying reports")

    def __init__(
        self,
        to_keep_list: list[str] | None = None,
        to_remove_list: list[str] | None = None,
        columns_to_drop: list[str] | None = None,
        columns_to_deid: list[str] | None = None,
    ) -> None:
        self.to_keep_list = to_keep_list
        self.to_remove_list = to_remove_list
        self.columns_to_drop = columns_to_drop
        self.columns_to_deid = columns_to_deid

    def execute(self, ctx: PipelineContext) -> None:
        input_file = ctx.text_input_file
        if input_file is None:
            raise RuntimeError(
                "PresidioTextDeid requires ctx.text_input_file to be set before "
                "the stage runs."
            )

        logging.info(f"NLM-Matched Deidentification: {input_file}")

        df = pd.read_excel(input_file, header=0)

        linker = None
        if ctx.images_saved and not ctx.deid_linker_path:
            raise RuntimeError(
                "DICOM files were de-identified, but no DICOM linker was "
                "available for consistent report identifier replacement."
            )
        if ctx.deid_linker_path:
            if not os.path.isfile(ctx.deid_linker_path):
                raise FileNotFoundError(
                    f"DICOM linker file not found: {ctx.deid_linker_path}"
                )
            linker = load_dicom_linker(ctx.deid_linker_path)

        row_replacements = (
            [replacements_for_report(row, linker) for _, row in df.iterrows()]
            if linker is not None
            else [{} for _ in range(len(df))]
        )
        if linker is not None:
            unmatched_rows = sum(not replacements for replacements in row_replacements)
            if unmatched_rows:
                logging.warning(
                    "DICOM linker did not match %d of %d report rows; "
                    "those rows will receive generic text de-identification only.",
                    unmatched_rows,
                    len(df),
                )
        if ctx.images_saved and ctx.date_shift_days is None:
            raise RuntimeError(
                "DICOM files were de-identified, but their DATEINC offset "
                "could not be read for consistent report date shifting."
            )

        if self.columns_to_drop:
            df = df.drop(columns=self.columns_to_drop, errors="ignore")

        # An explicit list (including an empty one) is authoritative; only a
        # None columns_to_deid falls back to deidentifying every column.
        columns_to_process = (
            self.columns_to_deid
            if self.columns_to_deid is not None
            else list(df.columns)
        )
        columns_to_process = [c for c in columns_to_process if c in df.columns]

        analyzer = create_analyzer_engine()
        anonymizer = AnonymizerEngine()
        _register_blacklist_recognizers(analyzer, self.to_remove_list or [])
        medical_preserve = _build_medical_preserve(self.to_keep_list or [])

        # Processing is column-major (all rows of a column, then the next), so
        # we report the running cell count across rows × columns. With a single
        # text column this is exactly "report N of total".
        total_cells = len(df) * len(columns_to_process)
        cells_done = 0

        def on_cell() -> None:
            nonlocal cells_done
            cells_done += 1
            if ctx.progress and total_cells:
                ctx.progress.update(
                    "text_deid",
                    cells_done / total_cells,
                    f"Processing {cells_done} of {total_cells} reports",
                )

        result_df = df.copy()
        # Identifier columns may be excluded from the user-selected free-text
        # columns. Replace those cells directly so the workbook itself keeps
        # the same DICOM pseudonyms.
        for column in IDENTIFIER_COLUMNS.intersection(result_df.columns):
            if column in columns_to_process:
                continue
            for row_index, replacements in enumerate(row_replacements):
                current = _normalize_identifier(result_df.at[row_index, column])
                if not current:
                    continue
                targets = set()
                for original, target in replacements.items():
                    variants = (
                        _patient_name_variants(original)
                        if column in {"PatientName", "Patient Name"}
                        else [original]
                    )
                    if any(_normalize_identifier(variant) == current for variant in variants):
                        targets.add(target)
                if len(targets) == 1:
                    result_df.at[row_index, column] = targets.pop()

        for column in columns_to_process:
            logging.info(f"Processing: {column}")
            result_df[column] = scrub(
                df[column].tolist(),
                analyzer,
                anonymizer,
                medical_preserve,
                progress_callback=on_cell,
                row_replacements=row_replacements,
                date_shift_days=ctx.date_shift_days,
            )

        os.makedirs(ctx.output_dir, exist_ok=True)
        output_file = os.path.join(ctx.output_dir, "output.xlsx")
        result_df.to_excel(output_file, index=False, header=True)

        logging.info(f"Complete: {output_file}")
        ctx.text_output_file = output_file
        ctx.text_rows_processed = len(result_df)
