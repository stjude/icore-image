"""Fixture value pools for the HELD-OUT validation corpus
(eval/final_textdeid_test/generate_corpus.py).

This is a sibling of eval/fixtures.py, not an import of it. Every value here
is deliberately DIFFERENT from eval/fixtures.py's pools -- different names,
hospitals, hyphenated terms, MRNs, dates -- because the tuning corpus
(eval/corpus/) is what the five fixes in pipeline/stages/text_deid.py were
written and iterated against. Re-running that same corpus and getting 100%
mostly proves the fixes still match the exact strings they were shaped
around. This corpus exists to check whether the fixes generalize to PHI-
shaped text they've never seen, using the same category taxonomy.

All values are fabricated for testing purposes only -- never real patient
data.
"""

# --- People ------------------------------------------------------------------
FIRST_NAMES = [
    "Amara",
    "Diego",
    "Freya",
    "Tariq",
    "Ingrid",
    "Kenji",
    "Lucia",
    "Nnamdi",
    "Saoirse",
    "Rashid",
    "Bianca",
    "Ivan",
    "Chidinma",
    "Magnus",
    "Rosa",
    "Zainab",
    "Felix",
    "Aditi",
    "Boris",
    "Yasmin",
]

LAST_NAMES = [
    "Osei",
    "Moreno",
    "Larsen",
    "Haddad",
    "Sorensen",
    "Watanabe",
    "Fernandes",
    "Chukwu",
    "Byrne",
    "Malik",
    "Rocha",
    "Petrenko",
    "Eze",
    "Andersen",
    "Silva",
    "Farouk",
    "Keller",
    "Rao",
    "Volkov",
    "Hassan",
]

# Fresh hyphenated surnames, disjoint from eval/fixtures.py's HYPHENATED_LAST_NAMES.
HYPHENATED_LAST_NAMES = [
    "Cross-Bennett",
    "Delgado-Ruiz",
    "Adeyinka-Bello",
    "Larsen-Voss",
    "Marsh-Whitaker",
    "Cohen-Levy",
]

DOCTOR_LAST_NAMES = [
    "Bianchi",
    "Park",
    "Adeyemo",
    "Kessler",
    "Okonkwo",
    "Nishimura",
    "Wojcik",
    "Delacroix",  # note: same surname reused deliberately once, see edge cases
]

# --- Places -------------------------------------------------------------------
HOSPITALS = [
    "Brightwater Medical Center",
    "Summit Ridge Hospital",
    "Fairview Imaging Partners",
    "Harborview Diagnostic Center",
    "Union Square Radiology",
    "Cascade Regional Health",
    "Sunnyvale Clinic",
    "Ashford Imaging Associates",
]

# --- Dates: same regex branches (d1-d4) as eval/fixtures.py, new literal values.
DATES = [
    "03/14/2025",  # d1
    "07/09/2024",
    "03-14-2025",  # d2
    "March 14, 2025",  # d3
    "Feb. 9, 2025",  # d4
]

# --- MRNs: same regex branches (m1-m7), new literal values.
MRNS = [
    "6637284",  # m1: 7 digits
    "71904523",  # m2: 8 digits
    "804512399",  # m3: 9 digits
    "2093847561",  # m4: 10 digits
    "MRN-778104",  # m5
    "B7654321",  # m6
    "CDX98765",  # m7
]

PHONES = [
    "(415) 555-0192",
    "415-555-0192",
    "415.555.0192",
]

EMAILS = [
    "d.moreno@clinicnet.org",
    "f.larsen22@radgroup.example.com",
    "imaging.dept@harborview.example.org",
]

AGES_90PLUS = ["91", "96", "104"]
AGES_UNDER_90 = ["28", "52", "8", "41"]

# Filler sentences, none copied from eval/fixtures.py's MEDICAL_FILLER. A few
# deliberately contain a fresh hyphenated term so the hyphenated-name fix gets
# exercised incidentally too, not just via the dedicated edge cases.
MEDICAL_FILLER = [
    "The soft tissues of the neck are unremarkable without lymphadenopathy.",
    "No interval change is seen in the previously described nodular opacity.",
    "The visualized osseous structures demonstrate no acute fracture.",
    "There is mild degenerative change at the L4-L5 disc level, unchanged.",
    "The kidneys are normal in size with no hydronephrosis identified.",
    "A poorly-defined hypodensity is noted in the right hepatic lobe.",
    "Findings are most consistent with a well-circumscribed nodule.",
    "The exam was performed using a low-dose, single-phase protocol.",
    "Post-surgical changes are noted at the prior resection site.",
    "The airway is patent without evidence of any narrowing.",
    "No free fluid or free air is identified within the abdomen.",
    "The thyroid gland is normal in size and echotexture bilaterally.",
]

# --- Known bug-class stress values, all disjoint from eval/fixtures.py -------

# Fresh hyphenated clinical terms (fix #1) -- none overlap
# eval/fixtures.py's HYPHENATED_MEDICAL_TERMS list.
HYPHENATED_MEDICAL_TERMS = [
    "poorly-defined",
    "well-circumscribed",
    "single-phase",
    "low-dose",
    "post-surgical",
    "well-marginated",
    "ill-fitting",
    "high-resolution",
    "three-dimensional",
    "long-standing",
]

# Date-adjective words: split into two groups on purpose.
# COVERED words are already in text_deid.py's date_adjective regex (fix #2)
# but in phrasing this exact set never appeared in the tuning corpus.
DATE_ADJECTIVE_WORDS_COVERED = [
    "daily",
    "weekly",
    "monthly",
    "nightly",
    "overnight",
]
# UNCOVERED words are a genuine generalization probe: the fix is a fixed word
# list, not a general date-adjective detector, so these are expected to
# still fail unless the underlying model happens to not flag them as DATE_TIME
# in the first place. A failure here is not a regression -- it's the fixed
# word list's known boundary, worth documenting either way.
DATE_ADJECTIVE_WORDS_UNCOVERED = [
    "hourly",
    "quarterly",
    "fortnightly",
    "biannually",
]

TECHNICAL_JARGON_WORDS = [
    "console",
    "throughput",
    "workstation",
    "firmware",
    "bandwidth",
    "uplink",
]

CLINICAL_ABBREVIATIONS = ["h/o", "s/p", "c/o", "w/o", "f/u"]
