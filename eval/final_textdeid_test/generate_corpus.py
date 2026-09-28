"""Generate the HELD-OUT synthetic validation corpus for
eval/final_textdeid_test/run_eval.py.

Sibling of eval/generate_corpus.py, not an extension of it: draws from
fixtures.py in this same folder (held-out name/hospital/date/MRN/hyphenated-
term pools, disjoint from eval/fixtures.py) so this corpus was never used to
write or tune any of the five fixes in pipeline/stages/text_deid.py. The
point is to measure generalization, not re-confirm the tuning set.

Every PHI value is fabricated by this script, never real patient data.

Usage:
    python3 eval/final_textdeid_test/generate_corpus.py
"""

from __future__ import annotations

import json
import random
from pathlib import Path

from fixtures import (
    AGES_90PLUS,
    AGES_UNDER_90,
    DATES,
    DOCTOR_LAST_NAMES,
    EMAILS,
    FIRST_NAMES,
    HOSPITALS,
    HYPHENATED_LAST_NAMES,
    LAST_NAMES,
    MEDICAL_FILLER,
    MRNS,
    PHONES,
)

CORPUS_DIR = Path(__file__).resolve().parent / "corpus"
OUT_FILENAME = "synthetic_validation.jsonl"
SEED = 20260806  # different from eval/generate_corpus.py's SEED on purpose
NUM_REPORTS = 50

EXAM_TYPES = [
    "a CT of the abdomen",
    "an MRI of the cervical spine",
    "a screening mammogram",
    "a renal ultrasound",
]

NARRATIVE_TEMPLATE = (
    "{age}-year-old {sex} presented to {hospital} for {exam_type}.\n"
    "Patient: {name}, MRN {mrn}, seen on {date}.\n"
    "Contact: {phone}, {email}.\n"
    "History: {history}\n"
    "Findings: {findings}\n"
    "Impression: {impression}\n"
    "Electronically signed by Dr. {doc_last}, Radiologist."
)

SECTIONED_TEMPLATE = (
    "PATIENT: {name}\n"
    "MRN: {mrn}\n"
    "DATE OF SERVICE: {date}\n"
    "AGE: {age}\n"
    "EXAM: {exam_type_cap} AT {hospital_cap}\n"
    "CONTACT: {phone} / {email}\n"
    "FINDINGS: {findings}\n"
    "IMPRESSION: {impression}\n"
    "ELECTRONICALLY SIGNED: DR. {doc_last_cap}"
)


def build_standard_report(rng: random.Random, idx: int) -> dict:
    use_hyphenated_name = rng.random() < 0.35
    if use_hyphenated_name:
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(HYPHENATED_LAST_NAMES)
        name_category = "name_hyphenated"
    else:
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        name_category = "name"
    name = f"{first} {last}"

    doc_last = rng.choice(DOCTOR_LAST_NAMES)
    hospital = rng.choice(HOSPITALS)
    date = rng.choice(DATES)
    mrn = rng.choice(MRNS)
    phone = rng.choice(PHONES)
    email = rng.choice(EMAILS)
    sex = rng.choice(["male", "female"])
    exam_type = rng.choice(EXAM_TYPES)
    history, findings, impression = rng.sample(MEDICAL_FILLER, 3)

    is_elderly = rng.random() < 0.5
    age = rng.choice(AGES_90PLUS) if is_elderly else rng.choice(AGES_UNDER_90)

    use_sectioned_template = rng.random() < 0.5
    if use_sectioned_template:
        text = SECTIONED_TEMPLATE.format(
            name=name,
            mrn=mrn,
            date=date,
            age=age,
            exam_type_cap=exam_type.upper(),
            hospital_cap=hospital.upper(),
            phone=phone,
            email=email,
            findings=findings,
            impression=impression,
            doc_last_cap=doc_last.upper(),
        )
        hospital_value = hospital.upper()
        doc_last_value = doc_last.upper()
        age_value = f"AGE: {age}"
        template_name = "sectioned"
    else:
        text = NARRATIVE_TEMPLATE.format(
            age=age,
            sex=sex,
            hospital=hospital,
            exam_type=exam_type,
            name=name,
            mrn=mrn,
            date=date,
            phone=phone,
            email=email,
            history=history,
            findings=findings,
            impression=impression,
            doc_last=doc_last,
        )
        hospital_value = hospital
        doc_last_value = doc_last
        age_value = f"{age}-year-old"
        template_name = "narrative"

    phi = [
        {"category": name_category, "value": name},
        {"category": "name", "value": doc_last_value},
        {"category": "date", "value": date},
        {"category": "mrn", "value": mrn},
        {"category": "phone", "value": phone},
        {"category": "email", "value": email},
    ]
    if is_elderly:
        phi.append({"category": "age90", "value": age_value})

    preserve = [
        {"category": "hospital", "value": hospital_value},
    ]
    if template_name == "narrative":
        preserve.append({"category": "medical_term", "value": history})
    preserve.append({"category": "medical_term", "value": findings})
    preserve.append({"category": "medical_term", "value": impression})
    if not is_elderly:
        preserve.append({"category": "age_under_90", "value": age_value})

    return {
        "id": f"validation-{idx:03d}",
        "description": f"held-out randomly generated {template_name}-style report",
        "text": text,
        "phi": phi,
        "preserve": preserve,
    }


def main() -> None:
    rng = random.Random(SEED)
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CORPUS_DIR / OUT_FILENAME

    with out_path.open("w") as f:
        for i in range(NUM_REPORTS):
            record = build_standard_report(rng, i)
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {NUM_REPORTS} held-out synthetic reports -> {out_path}")


if __name__ == "__main__":
    main()
