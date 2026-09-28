"""Generate a synthetic corpus of fake radiology reports for eval/run_eval.py.

Every PHI value in the generated reports is fabricated by this script, never
real patient data. Ground truth is exact because every substitution is
recorded at generation time: each record lists PHI values that must be
redacted ("phi") and values that must survive verbatim ("preserve").

This script is pure stdlib (no presidio/spacy dependency), so it can be run
with any python3 to regenerate or extend the corpus.

Usage:
    python3 eval/generate_corpus.py

Regenerating overwrites eval/corpus/synthetic_v1.jsonl. If you want to compare
old vs. new corpora side by side, copy the old file aside (or bump the
OUT_FILENAME below to synthetic_v2.jsonl) rather than overwriting it.

Diversity notes (expanded from the original version): fixtures.py now draws
from a much broader name pool (not just ~10 Anglo-American names repeated),
reports alternate between two structurally different templates (a narrative
style and a labeled ALL-CAPS sectioned style some real RIS/dictation systems
use), and roughly a third of reports use a hyphenated surname so
name_hyphenated recall is exercised at more than n=1.
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
OUT_FILENAME = "synthetic_v1.jsonl"
SEED = 20260727  # fixed so the corpus is reproducible across machines
NUM_REPORTS = 60

EXAM_TYPES = [
    "a CT of the head",
    "an MRI of the lumbar spine",
    "a chest X-ray",
    "an abdominal ultrasound",
]

# --- Template A: narrative style (original) ---------------------------------

NARRATIVE_TEMPLATE = (
    "{age}-year-old {sex} presented to {hospital} for {exam_type}.\n"
    "Patient: {name}, MRN {mrn}, seen on {date}.\n"
    "Contact: {phone}, {email}.\n"
    "History: {history}\n"
    "Findings: {findings}\n"
    "Impression: {impression}\n"
    "Electronically signed by Dr. {doc_last}, Radiologist."
)

# --- Template B: labeled ALL-CAPS sectioned style ---------------------------
# Structurally different on purpose: colon-delimited key/value lines, no
# narrative prose, no "History" section at all, section labels in caps. Some
# real RIS/dictation exports look like this rather than the narrative style
# above -- a recognizer that's implicitly learned to expect prose punctuation
# around names/dates should get exercised differently here.

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
    """Assemble one randomized report + its exact ground truth."""
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
        # Deliberately "AGE: 93" (raw digits, no "-year-old" suffix) rather
        # than reusing the narrative template's phrasing -- this exercises
        # the a4/a5 colon-lookbehind AGE90PLUS patterns instead of the a3
        # "-year-old" lookahead pattern the narrative template already
        # covers, so the two templates test different regex branches rather
        # than the same one twice under different formatting.
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
        # The sectioned template upper-cases the hospital name and doctor
        # surname to mimic real ALL-CAPS section styling -- ground truth
        # must match the case actually present in the generated text.
        hospital_value = hospital.upper()
        doc_last_value = doc_last.upper()
        # Check "AGE: <n>" as a whole, not the bare digits: for age == 90 the
        # bare string "90" is trivially still "present" inside the
        # anonymizer's own fixed placeholder token "[AGE90+]", which would
        # be a false-positive leak, not a real one (same reasoning as the
        # narrative template's "<n>-year-old" check).
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
        # Check the full "<age>-year-old" phrase, not the bare digits: when
        # age == 90 the bare string "90" is trivially still "present" inside
        # the anonymizer's own fixed placeholder token "[AGE90+]", which
        # would be a false-positive leak, not a real one.
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
        "id": f"synthetic-{idx:03d}",
        "description": f"randomly generated {template_name}-style report",
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

    print(f"Wrote {NUM_REPORTS} synthetic reports -> {out_path}")


if __name__ == "__main__":
    main()
