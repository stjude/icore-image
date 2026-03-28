"""Python-based audit log and linker table extraction for the Rust deid engine.

Replaces CTP's DicomAuditLogger and IDMap when using dicom-deid-rs.
Reads DICOM headers from input (pre-deid) and output (post-deid) directories
to produce metadata.xlsx, deid_metadata.xlsx, and linker.xlsx.
"""

import logging
import os

import pandas as pd
import pydicom

from utils import csv_string_to_xlsx

# Tags extracted by CTP's DicomAuditLogger (study-level)
AUDIT_TAGS = [
    "AccessionNumber",
    "StudyInstanceUID",
    "PatientName",
    "PatientID",
    "PatientSex",
    "Manufacturer",
    "ManufacturerModelName",
    "StudyDescription",
    "StudyDate",
    "SeriesInstanceUID",
    "SOPClassUID",
    "Modality",
    "SeriesDescription",
    "Rows",
    "Columns",
    "InstitutionName",
    "StudyTime",
]


def _find_dicom_files(directory: str) -> list[str]:
    """Recursively find DICOM files by checking for the DICM preamble."""
    dicom_files = []
    for root, _, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            try:
                with open(filepath, "rb") as f:
                    f.seek(128)
                    if f.read(4) == b"DICM":
                        dicom_files.append(filepath)
            except (OSError, IOError):
                continue
    return dicom_files


def _get_tag_value(ds: pydicom.Dataset, tag_name: str) -> str:
    """Safely extract a tag value as a string."""
    try:
        elem = ds[tag_name]
        if elem.value is None:
            return ""
        return str(elem.value)
    except (KeyError, AttributeError):
        return ""


def extract_audit_log(
    dicom_dir: str, level: str = "study"
) -> pd.DataFrame:
    """Read all DICOMs in dir, extract AUDIT_TAGS, return DataFrame.

    When level="study", deduplicates by StudyInstanceUID to match CTP's
    study-level audit logging behavior.
    """
    dicom_files = _find_dicom_files(dicom_dir)

    rows = []
    seen_studies = set()

    for filepath in dicom_files:
        try:
            ds = pydicom.dcmread(filepath, stop_before_pixels=True)
        except Exception:
            continue

        study_uid = _get_tag_value(ds, "StudyInstanceUID")

        if level == "study" and study_uid in seen_studies:
            continue
        seen_studies.add(study_uid)

        row = {}
        for tag_name in AUDIT_TAGS:
            row[tag_name] = _get_tag_value(ds, tag_name)
        rows.append(row)

    return pd.DataFrame(rows, columns=AUDIT_TAGS)


def build_linker_table(
    pre_audit: pd.DataFrame, post_audit: pd.DataFrame
) -> pd.DataFrame:
    """Build an ID mapping table from pre/post audit logs.

    Maps original PatientID/PatientName to de-identified values,
    keyed by StudyInstanceUID (which may itself be hashed).
    """
    linker_cols = [
        "Original PatientID",
        "Original PatientName",
        "Original AccessionNumber",
        "Original StudyInstanceUID",
        "Deidentified PatientID",
        "Deidentified PatientName",
        "Deidentified AccessionNumber",
        "Deidentified StudyInstanceUID",
    ]

    if pre_audit.empty or post_audit.empty:
        return pd.DataFrame(columns=linker_cols)

    # Match by row order (input and output should correspond 1:1 for
    # non-blacklisted files). If sizes differ, match what we can.
    rows = []
    for i in range(min(len(pre_audit), len(post_audit))):
        pre = pre_audit.iloc[i]
        post = post_audit.iloc[i]
        rows.append(
            {
                "Original PatientID": pre.get("PatientID", ""),
                "Original PatientName": pre.get("PatientName", ""),
                "Original AccessionNumber": pre.get("AccessionNumber", ""),
                "Original StudyInstanceUID": pre.get("StudyInstanceUID", ""),
                "Deidentified PatientID": post.get("PatientID", ""),
                "Deidentified PatientName": post.get("PatientName", ""),
                "Deidentified AccessionNumber": post.get("AccessionNumber", ""),
                "Deidentified StudyInstanceUID": post.get("StudyInstanceUID", ""),
            }
        )

    return pd.DataFrame(rows, columns=linker_cols)


def save_audit_files(
    input_dir: str, output_dir: str, appdata_dir: str
) -> None:
    """Full audit extraction workflow.

    Extracts pre-deid and post-deid audit logs from DICOM headers,
    builds a linker table, and saves all as Excel files.
    """
    logging.info("Extracting audit logs from DICOM headers...")

    pre_audit = extract_audit_log(input_dir)
    if not pre_audit.empty:
        pre_csv = pre_audit.to_csv(index=False)
        csv_string_to_xlsx(pre_csv, os.path.join(appdata_dir, "metadata.xlsx"))
        logging.info(f"Pre-deid audit log: {len(pre_audit)} studies")

    post_audit = extract_audit_log(output_dir)
    if not post_audit.empty:
        post_csv = post_audit.to_csv(index=False)
        csv_string_to_xlsx(post_csv, os.path.join(appdata_dir, "deid_metadata.xlsx"))
        logging.info(f"Post-deid audit log: {len(post_audit)} studies")

    linker = build_linker_table(pre_audit, post_audit)
    if not linker.empty:
        linker_csv = linker.to_csv(index=False)
        csv_string_to_xlsx(linker_csv, os.path.join(appdata_dir, "linker.xlsx"))
        logging.info(f"Linker table: {len(linker)} entries")

    logging.info("Audit extraction complete")
