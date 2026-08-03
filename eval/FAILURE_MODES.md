# Text de-id: failure modes, edge cases, and proposed solutions

Consolidated from `findings_over_redaction.md` and `findings_recall_gaps.md`
(both still kept as the dated, narrative record of when/how each was found).
This file is the single reference: every known failure mode, the corpus edge
case(s) that test it, its root cause, and a proposed fix — organized by
priority rather than by when it was discovered.

Everything here was confirmed directly against `analyzer.analyze()` /
`scrub()` in `pipeline/stages/text_deid.py`, not inferred from the eval
report alone. **No code has been changed** — this is findings and proposed
solutions only. Numbers are from the current baseline
(`eval/baseline_results.json`, corpus of 60 synthetic + 49 handwritten
records).

---

## At a glance

| # | Failure mode | Type | Current rate | Fix complexity |
|---|---|---|---|---|
| 1 | Hyphenated compound words flagged as names | over-redaction, regex bug | 10% correct (9/10 stress cases fail) | low — scoped regex-flag fix |
| 2 | Date-adjectives flagged as dates | over-redaction, model noise | 0% correct (6/6 fail) | low — one new exclusion filter |
| 3 | "y/o" age shorthand not recognized | **missed PHI (leak)** | 0% caught | low — one new regex pattern |
| 4 | Names in ALL CAPS not recognized | **missed PHI (leak)** | 0% caught (tested) | medium — needs a normalization or fallback pass |
| 5 | Cross-line span bleeding into next label | over-redaction, model noise | 88% correct (3/24 fail) | medium — format-shape sensitive, needs triage |
| 6 | MRN repeated-tail-digit exclusion unreliable | over-redaction, narrow bug | 0% correct (1 case) | low — broaden the exclusion's entity-type guard |
| 7 | Inconsistent misses on specific name combos | **missed PHI (leak)**, model noise | non-zero, unpredictable | high — model/training limitation |
| 8 | Technical/PACS jargon misflagged | over-redaction, model noise | mostly fine; sporadic misses | high — model limitation, not systemic |

Priority order for fixing, if/when this gets picked up: **3 and 1/2 first**
(clean, high-confidence, low-effort fixes for real leaks and categorical
over-redaction), **then 4** (real leak, but the fix is less certain), **then
6**, with **5, 7, 8 as tracked/monitored** rather than urgent — they're real
but diffuse, model-driven, and don't have a clean bounded fix.

---

## 1. Hyphenated compound words flagged as names

**What happens:** `"There is a well-defined lesion..."` → `"There is a
[PERSONALNAME] lesion..."`. Ordinary hyphenated clinical adjectives
(well-defined, non-displaced, post-traumatic, low-grade, T2-weighted, etc.)
get redacted as if they were a name like "Wilson-Cook".

**Root cause:** the custom `name_extras_recognizer` pattern `n3` —
`[A-Z][a-z]{2,}-[A-Z][a-z]{2,}` — was written assuming case-sensitive
matching (both halves capitalized, a reasonable proper-noun heuristic). But
`presidio_analyzer.PatternRecognizer` defaults `global_regex_flags` to
`re.DOTALL | re.MULTILINE | re.IGNORECASE`, and `create_analyzer_engine()`
never overrides it. So the pattern actually matches *any* hyphen-joined
word pair of 3+ letters each, regardless of case.

**Scope confirmed:** not a one-off — 9 of 10 dedicated stress cases fail, and
it also corrupts *ordinary* generated filler text: the `medical_term`
category (plain "Findings:"/"Impression:" sentences with no deliberate bug
target) sits at 77% correct purely because a few of the stock filler
sentences happen to contain a hyphenated term.

**Edge cases:** `edge-name-hyphenated` (positive control — a real hyphenated
surname must still be caught), `edge-hyphen-well-defined`,
`edge-hyphen-ill-defined`, `edge-hyphen-non-displaced`,
`edge-hyphen-post-traumatic`, `edge-hyphen-low-grade`,
`edge-hyphen-high-grade`, `edge-hyphen-t2-weighted`,
`edge-hyphen-well-corticated`, `edge-hyphen-non-obstructive`,
`edge-hyphen-well-healed`.

**Proposed solution:** override `global_regex_flags` for
`name_extras_recognizer` to drop `IGNORECASE`:

```python
name_extras_recognizer = PatternRecognizer(
    supported_entity="NAMEPERSON",
    name="names",
    global_regex_flags=re.DOTALL | re.MULTILINE,  # no IGNORECASE
    patterns=[...],
)
```

The other four patterns in that recognizer (initials, "F. M. Last", "Pine",
"Patient F.M.", "The" before "MD") already require literal capitals in their
regex, so this should only tighten `n3` — but verify against
`edge-name-initials` and `edge-name-hyphenated` (and a full eval run) before
shipping. Other recognizers (dates, MRNs, address) inherit the same
IGNORECASE default and weren't individually audited; the same fix pattern
would apply if similar bugs turn up there, but some (e.g. date month names)
may have legitimate reasons to stay case-insensitive — check each on its own.

---

## 2. Date-adjectives flagged as dates

**What happens:** `"...activities of daily living."` → `"...activities of
[DATE] living."`. Same for weekly, monthly, annually, nightly, biweekly —
**all 6 of 6** tested stress cases fail, not occasionally.

**Root cause:** Presidio's built-in spaCy-based date recognizer flags these
words as `DATE_TIME` on their own, independent of context. None of `scrub()`'s
existing exclusion filters cover them: `duration` requires a number before
the unit ("3 weeks"), `relative_date` only covers yesterday/today/tomorrow,
`year_only` and `gestational` don't apply.

**Edge cases:** `edge-date-adj-daily`, `edge-date-adj-weekly`,
`edge-date-adj-monthly`, `edge-date-adj-annually`, `edge-date-adj-nightly`,
`edge-date-adj-biweekly`.

**Proposed solution:** add a new exclusion filter to `scrub()`'s `DATE_TIME`
branch, mirroring the existing `duration`/`relative_date` pattern — a small
compiled regex for common date-adjectives (`daily`, `weekly`, `monthly`,
`annual(ly)?`, `nightly`, `biweekly`, ...) plus `continue`. This is the same
shape of fix already used elsewhere in the file, low risk.

---

## 3. "y/o" age shorthand is not recognized at all

**What happens:** `"93 y/o male presented with acute confusion."` produces
**zero entities of any kind.** This is a real leak, not over-redaction — an
age of 90+ written this way passes through completely unredacted.

**Root cause:** none of the `AGE90PLUS` patterns (`a1`-`a6` in
`create_analyzer_engine()`) match "y/o" — they all require literal "year(s)
old", "-year-old", or an "age:" label. "y/o" is extremely common shorthand in
real dictated/EHR radiology text.

**This is the single most concerning finding in the whole set** — a direct
compliance-relevant miss, not a readability problem.

**Edge case:** `edge-recall-yo-age-format`.

**Proposed solution:** add new `AGE90PLUS` pattern(s) for the "y/o" (and
likely "yo", "y.o.") shorthand, following the same lookahead/lookbehind shape
as the existing `a1`-`a6` patterns.

---

## 4. Names in ALL CAPS are not recognized

**What happens:** `Dr. Chen` is caught cleanly (`PERSON`, score 0.85);
`DR. ROSSI` / `DR. TANAKA` / other all-caps names are **not detected at any
confidence threshold down to 0.3.** Reproduced on every all-caps name tested
— not a one-off.

**Root cause:** spaCy's NER relies heavily on capitalization as a proper-noun
signal; ALL-CAPS text removes that signal entirely. This is a property of
the underlying `en_core_web_sm` model, not a custom regex.

**Edge cases:** not yet a dedicated handwritten edge case — currently only
surfaced incidentally through the "sectioned" synthetic report template
(`generate_corpus.py`, ~half of the 60 synthetic reports), where doctor
surnames are rendered in caps. **Recommended next step:** add a standalone
`edge-name-allcaps` case so this doesn't depend on which synthetic reports
happen to get generated.

**Proposed solutions:**
1. Run NER against a case-normalized copy of the text (e.g. title-cased) and
   map detected spans back to the original offsets — catches this without
   touching the custom regex patterns, but adds a preprocessing step and
   needs care with span-offset mapping.
2. A targeted fallback recognizer for signature-line-shaped text (e.g.
   `(?:DR\.|ELECTRONICALLY SIGNED)[:\s]+([A-Z-]+)`) — narrower, cheaper, but
   only covers the signature-line case, not any all-caps name anywhere.
3. Evaluate a larger spaCy model (see #8) — may incidentally help here too,
   unconfirmed.

---

## 5. Entity spans can bleed across a newline into the next line's label

**What happens:** in the labeled/sectioned report style (colon-delimited,
one field per line), a detected span sometimes extends past the newline and
swallows the next line's label word. Confirmed for two different entity
types:

```python
'PATIENT: Ngozi Anderson\nMRN: 902345671\n...'
# PERSON span: 'Ngozi Anderson\nMRN'  -- "MRN" swallowed into the name redaction

'...DATE OF SERVICE: Jan. 5, 2024\nAGE: 45\n...'
# output: '...DATE OF SERVICE: [DATE]: 45\n...'  -- "AGE" swallowed into the date redaction
```

The redacted *value* itself is correct in both cases — it's the adjacent
*label word* that disappears. Only reproduces in the sectioned template
(no separating punctuation between a value and the next line's label); the
narrative template's comma/prose structure doesn't trigger it.

**Edge cases:** not yet isolated as a dedicated handwritten case — currently
observed via `age_under_90` category failures in the synthetic corpus
(e.g. `synthetic-010`, `synthetic-039`, `synthetic-040`). **Recommended next
step:** add a minimal dedicated case isolating just this shape (a name or
date immediately followed by `\nLABEL:`) so it doesn't depend on which
synthetic reports happen to be generated.

**Proposed solution:** needs more triage before proposing a specific fix —
first confirm whether this is spaCy's own span-merging behavior or an
artifact of how `analyzer.analyze()` combines overlapping/adjacent results,
since the fix differs (a spaCy pipeline component vs. a result-merging
adjustment in `create_analyzer_engine()`/`scrub()`). Flagging as a known,
reproducible issue rather than proposing an unverified fix.

---

## 6. MRN repeated-tail-digit exclusion doesn't reliably apply

**What happens:** `"Legacy chart MRN 1230000 predates..."` — a 7-digit MRN
ending in 4+ repeated digits is supposed to be excluded from redaction (per
the code's own carve-out), but gets redacted as `[DATE]` anyway.

**Root cause:** the exclusion —
`re.search(r"(\d)\1{3,}$", detected) and len(detected) == 7` — only runs
inside the `if result.entity_type == "ALPHANUMERICID":` branch of `scrub()`'s
filter loop. In practice, Presidio sometimes tags this exact digit string as
`DATE_TIME` instead of `ALPHANUMERICID`, bypassing the carve-out entirely
since it's guarded by an entity-type check that doesn't fire.

**Edge case:** `edge-mrn-repeated-tail`.

**Proposed solution:** apply the repeated-tail (and all-zeros) checks
independent of `result.entity_type`, or apply them earlier in the filter
loop before the entity-type-specific branches, so the exclusion holds
regardless of which recognizer happened to claim the match.

---

## 7. Inconsistent misses on specific name combinations

**What happens:** most names across a broadened 20×20 first/last name pool
are recognized correctly, including most non-Western names individually. But
a few specific *combinations* are missed: Olamide Kimathi, Sofia Oyelaran,
Esperanza Novak, Olamide Smith. Pairing the same surnames with "John" instead
works fine — this is not a clean "fails on all non-Western names" story, it's
a non-zero, somewhat unpredictable miss rate concentrated on specific
first+last pairings.

**Root cause:** inherent limitation of `en_core_web_sm`'s training data
coverage — not a bug in custom logic, and not deterministic enough to
target with a regex without an unbounded whack-a-mole list.

**Edge cases:** none yet — these were found via ad hoc pairwise testing, not
committed to the corpus. **Recommended next step:** add the four confirmed
failing combinations above as dedicated recall-check cases so they're
tracked (not fixed, just monitored for regression/improvement if the model
ever changes).

**Proposed solution:** no clean regex fix. Same tradeoff as #8 — evaluate a
larger spaCy model, or accept as a documented model limitation and monitor.

---

## 8. Technical/PACS jargon occasionally misflagged

**What happens:** the original finding was `"node"` → `[ADDRESS]`. Testing a
broader set of similar jargon (workstation, console, modality, server,
gateway, portal) found all 6 actually **pass** — this is not a systemic
"jargon class" failure. But sporadic individual words still misfire
unpredictably (e.g. "overnight" got swallowed in the same sentence as the
"server" test, caught only by the word-level catch-all check, not the
jargon category itself).

**Root cause:** general spaCy NER noise on domain-specific / technical text
it has no training exposure to — same underlying cause as #7, just
manifesting as occasional false positives instead of false negatives.

**Edge cases:** `edge-jargon-workstation`, `edge-jargon-console`,
`edge-jargon-modality`, `edge-jargon-server`, `edge-jargon-gateway`,
`edge-jargon-portal` (all currently pass — kept as regression guards).

**Proposed solutions:** same as #7/#2 in the original over-redaction doc —
add specific known-bad words to `NLM_PRESERVE_MEDICAL` reactively as they're
found (cheap, whack-a-mole), or evaluate a larger spaCy model for better
general precision (needs a side-by-side comparison via this same eval
harness before committing).

---

## Verified working — no action needed

For completeness, these were tested and hold up cleanly, so they're not
failure modes: IP address redaction (`edge-ip-public`) and the all-octets-<20
IP exclusion (`edge-ip-internal-excluded`, aside from "node" above), SSN
(`edge-ssn`), street address (`edge-address`), blood pressure
(`edge-blood-pressure`), measurements (`edge-measurement`), symptom
durations (`edge-duration`), relative dates (`edge-relative-date`), bare
years (`edge-year-only`), gestational age phrasing (`edge-gestational-age`),
all-zero MRNs (`edge-mrn-all-zeros`), hospital/clinic name exclusion
(`edge-hospital-name-preserved`), the custom blacklist/whitelist override
(`edge-blacklist-whitelist-override`), initials-style names
(`edge-name-initials`), the "Pine" force-redact carve-out
(`edge-name-pine-special-case`), the age-90 boundary conditions
(`edge-age90-boundary`, `edge-age-under-90-boundary`, `edge-age90-colon-form`,
`edge-age90-is-form`), common clinical abbreviations
(`edge-clinical-abbreviations`), and the formatting-robustness checks
(`edge-recall-lowercase-name`, `edge-recall-nospace-mrn-label`,
`edge-recall-nospace-date-label`, `edge-recall-extra-whitespace-name`).
