"""Fixture value pools for synthetic report generation (eval/generate_corpus.py).

Every value below is fabricated for testing purposes only. Never add real
patient data to this file — the whole point of a synthetic corpus is that
ground truth is known exactly because *we* injected the values.

Values are chosen (not just "realistic-looking") to land squarely inside the
regex patterns defined in pipeline/stages/text_deid.py, so a passing eval
result means the real code path actually caught them — not that we got lucky
with formatting.
"""

# --- People ----------------------------------------------------------------
FIRST_NAMES = [
    "John",
    "Maria",
    "David",
    "Aisha",
    "Wei",
    "Fatima",
    "Robert",
    "Elena",
    "Marcus",
    "Priya",
    "Mohammed",
    "Yuki",
    "Olamide",
    "Sofia",
    "Dmitri",
    "Ngozi",
    "Hiroshi",
    "Esperanza",
    "Kwame",
    "Anjali",
]

LAST_NAMES = [
    "Smith",
    "Nguyen",
    "Garcia",
    "Johnson",
    "Patel",
    "Kowalski",
    "Anderson",
    "Okafor",
    "Ivanov",
    "Delacroix",
    "Nakamura",
    "Abdullah",
    "Kimathi",
    "Oyelaran",
    "Petrov",
    "Villanueva",
    "Choudhury",
    "Novak",
    "Adeyemi",
    "Fernandez",
]

# A second surname pool of already-hyphenated names, used both to broaden
# name_hyphenated recall coverage in the synthetic generator (beyond the one
# static "Wilson-Cook" edge case) and as raw material for future stress
# cases.
HYPHENATED_LAST_NAMES = [
    "Wilson-Cook",
    "Reyes-Martinez",
    "Baker-Fitzgerald",
    "Osei-Mensah",
    "Alvarez-Torres",
    "Singh-Kaur",
]

DOCTOR_LAST_NAMES = [
    "Rossi",
    "Chen",
    "Okoye",
    "Fischer",
    "Whitfield",
    "Al-Rashid",
    "Tanaka",
    "Kowalczyk",
]

# --- Places -------------------------------------------------------------------

HOSPITALS = [
    "Cedar Rapids Medical Center",
    "Northgate Regional Hospital",
    "Lakeside Healthcare Clinic",
    "Overlook Health System",
    "Riverside Community Hospital",
    "St. Augustine Medical Center",
    "Pinehurst Diagnostic Imaging",
    "Metro West Radiology Associates",
]

# --- Dates: one literal example per regex branch in text_deid.py's
# date_patterns recognizer (d1-d4), so every format actually gets exercised.

DATES = [
    "01/05/2024",  # d1: \d{1,2}/\d{1,2}/\d{2,4}
    "11/23/2023",
    "01-05-2024",  # d2: \d{1,2}-\d{1,2}-\d{4}
    "November 23, 2023",  # d3: full month name
    "Jan. 5, 2024",  # d4: abbreviated month
]

# --- Medical record numbers: one literal example per regex branch (m1-m8).

MRNS = [
    "4521873",  # m1: 7 digits
    "88213456",  # m2: 8 digits
    "902345671",  # m3: 9 digits
    "1029384756",  # m4: 10 digits
    "MRN-102938",  # m5: [A-Z]{1,6}-\d{6,10}
    "A1234567",  # m6: [A-Z]\d{7,9}
    "AB123456",  # m7: [A-Z]{2,3}\d{4,8}
]

PHONES = [
    "(555) 123-4567",  # p1
    "555-123-4567",  # p2
    "555.123.4567",  # p3
]

EMAILS = [
    "jane.doe@hospital.org",
    "j.smith123@clinicmail.com",
    "priya.patel@radnet.example.com",
]

# --- Ages: values that must be flagged (>=90, via the "-year-old" lookahead
# pattern a3) vs values that must NOT be flagged (<90, safe-harbor allows
# these to remain per the code's design intent).

AGES_90PLUS = ["90", "93", "101"]
AGES_UNDER_90 = ["45", "67", "12", "34"]

# --- Medical filler text drawn conceptually from NLM_PRESERVE_MEDICAL so the
# whitelist is actually being exercised, not just "text with no PHI-shaped
# tokens in it".

MEDICAL_FILLER = [
    "There is no evidence of mediastinum widening or paraspinal soft tissue swelling.",
    "Ventricles are normal in size and configuration without hydrocephalus.",
    "No acute intracranial hemorrhage, mass, or infarction is identified.",
    "The visualized lung parenchyma shows no consolidation or pneumothorax.",
    "Vertebral body heights and disc spaces are preserved without stenosis.",
    "Cardiac silhouette and mediastinal contours are within normal limits.",
    "No pleural effusion or pericardial effusion is seen.",
    "Gray-white matter differentiation is preserved throughout the brain.",
    "The fracture appears non-displaced with well-corticated margins.",
    "Findings are most consistent with post-traumatic changes, low-grade in severity.",
    "T2-weighted images demonstrate no abnormal signal within the visualized cord.",
    "A well-defined, non-obstructive lesion is noted without further complication.",
]

# --- Known bug-class stress cases -------------------------------------------
# Words that must NEVER be redacted, grouped by the failure mode they
# target. All three groups were found by manually reviewing real eval output
# (see eval/findings_over_redaction.md) -- these pools exist to quantify how
# wide each bug's blast radius is, not just confirm the one example we
# happened to notice.

# Case-insensitivity bug in the custom hyphenated-name regex (n3 in
# create_analyzer_engine): any two hyphen-joined words of 3+ letters each get
# matched regardless of case, so ordinary hyphenated clinical adjectives look
# like "Wilson-Cook" to the pattern.
HYPHENATED_MEDICAL_TERMS = [
    "well-defined",
    "ill-defined",
    "non-displaced",
    "post-traumatic",
    "low-grade",
    "high-grade",
    "T2-weighted",
    "well-corticated",
    "non-obstructive",
    "well-healed",
]

# Presidio's built-in spaCy-based date recognizer noise: standalone
# date-adjectives that aren't dates at all, similar to the "daily" case in
# "activities of daily living".
DATE_ADJECTIVE_WORDS = [
    "daily",
    "weekly",
    "monthly",
    "annually",
    "nightly",
    "biweekly",
]

# spaCy en_core_web_sm NER noise on technical/PACS jargon it has no training
# exposure to, similar to the "node" case.
TECHNICAL_JARGON_WORDS = [
    "workstation",
    "console",
    "modality",
    "server",
    "gateway",
    "portal",
]

# Common clinical shorthand that should never be mistaken for an identifier.
CLINICAL_ABBREVIATIONS = ["h/o", "s/p", "c/o", "w/o", "f/u"]
