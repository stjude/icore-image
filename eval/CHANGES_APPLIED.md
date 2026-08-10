# Text de-id: changes applied to `pipeline/stages/text_deid.py`

This document records the code changes made to fix the failure modes
identified in `eval/pre_changes/FAILURE_MODES.md`, plus the before/after eval
numbers proving each fix. Every change is also commented in-line in
`text_deid.py` with a `FAILURE_MODES.md #N fix` marker so the code and this
document stay traceable to each other.

Before numbers: `eval/baseline_results.json` (pre-fix, 60 synthetic + 49
handwritten records).
After numbers: `eval/post_test_folder/latest.json` (same corpus, post-fix,
generated via `python eval/run_eval.py --results-dir post_test_folder`).

---

## 1. Hyphenated compound words no longer flagged as names (FAILURE_MODES #1)

**Where:** `name_extras_recognizer` in `create_analyzer_engine()`.

**Change:** added `global_regex_flags=re.DOTALL | re.MULTILINE` to the
recognizer, dropping the `IGNORECASE` that `PatternRecognizer` applies by
default. The `n3` pattern (`[A-Z][a-z]{2,}-[A-Z][a-z]{2,}`) was written
assuming case-sensitive matching but was silently running case-insensitive,
so any hyphenated word pair — "well-defined", "non-displaced",
"T2-weighted" — matched like a hyphenated surname.

**Result:**

| Category | Before | After |
|---|---|---|
| `hyphenated_medical_term` (preserve rate) | 10.0% (1/10) | **100.0%** (10/10) |
| `medical_term` (preserve rate) | 77.2% (115/149) | **100.0%** (149/149) |

The `medical_term` improvement is a side effect: several stock filler
sentences (used across many synthetic reports) happened to contain
hyphenated clinical terms, so this single fix cleared a second category too.
`name_hyphenated` (real hyphenated surnames, the positive control) stayed at
100% throughout — confirms the fix only removed the false matches, not the
true ones.

---

## 2. Date-adjectives no longer flagged as dates (FAILURE_MODES #2)

**Where:** `scrub()`, `DATE_TIME` branch.

**Change:** added a new compiled exclusion regex, `date_adjective`, matching
`daily|weekly|monthly|annually|annual|nightly|biweekly|overnight`, and a
`continue` when it matches — the same shape as the existing
`duration`/`relative_date`/`gestational` exclusions already in that branch.

`overnight` was not in the original FAILURE_MODES.md list — it turned up
during the post-fix eval run (see "Corpus bugs found along the way" below)
and was added once confirmed as the same failure mode.

**Result:**

| Category | Before | After |
|---|---|---|
| `date_adjective` (preserve rate) | 0.0% (0/6) | **100.0%** (6/6) |

---

## 3. "y/o" age shorthand now recognized (FAILURE_MODES #3)

**Where:** `age_90` recognizer in `create_analyzer_engine()`.

**Change:** added three new patterns (`a7`, `a8`, `a9`) covering `y/o`,
`y.o.`, and `yo` as suffixes on a 90+ age, mirroring the lookahead shape of
the existing `a1`-`a6` patterns (which only covered "year(s) old",
"-year-old", and "age:" labels).

This was flagged in FAILURE_MODES.md as the single most concerning finding:
a real compliance-relevant leak, not just an over-redaction/readability
issue — "93 y/o male" previously produced zero redactions at all.

**Result:**

| Category | Before | After |
|---|---|---|
| `age90` (recall) | 97.6% (40/41) | **100.0%** (41/41) |

---

## 4. ALL-CAPS doctor names in signature lines (FAILURE_MODES #4, partial fix)

**Where:** `name_extras_recognizer`, new pattern `n7`.

**Change:** added a targeted pattern —
`(?<=(?i:dr\.\s))([A-Z][A-Z\-]{2,}(?:\s[A-Z][A-Z\-]{2,})?)\b` — that looks
behind for "Dr. " (case-insensitive only on that lookbehind, via the scoped
`(?i:...)` inline flag) and then matches an ALL-CAPS surname. This is
solution #2 from FAILURE_MODES.md ("targeted fallback recognizer for
signature-line-shaped text"), not solution #1 (case-normalization +
span-remapping) or #3 (larger spaCy model) — both of those are broader,
riskier changes that FAILURE_MODES.md flagged as needing more validation, so
this narrower fix was chosen. It only catches "DR. X" style ALL-CAPS names,
not ALL-CAPS names in other contexts.

**Result:**

| Category | Before | After |
|---|---|---|
| `name` (recall) | 77.6% (83/107) | **97.2%** (104/107) |

3 name leaks remain — these are a different, unrelated failure mode
(FAILURE_MODES #7, inconsistent name-combination misses — see "Not fixed"
below), not ALL-CAPS related.

---

## 5. MRN repeated-tail-digit exclusion broadened (FAILURE_MODES #6)

**Where:** `scrub()`, the digit-exclusion checks.

**Change:** the repeated-tail-digit check (`re.search(r"(\d)\1{3,}$",
detected) and len(detected) == 7`) and the all-zeros check were previously
both scoped inside `if result.entity_type == "ALPHANUMERICID":`. Presidio
sometimes tags these digit strings as `DATE_TIME` instead, so the exclusion
never fired. Moved both checks to run unconditionally, independent of
`result.entity_type`, right after the pattern match. Removed the now-empty
`if result.entity_type == "ALPHANUMERICID": pass` block.

**Result:**

| Category | Before | After |
|---|---|---|
| `mrn_repeated_tail` (preserve rate) | 0.0% (0/1) | **100.0%** (1/1) |

---

## Overall eval summary

| Category | Metric | Before | After |
|---|---|---|---|
| hyphenated_medical_term | preserve | 10.0% | **100.0%** |
| date_adjective | preserve | 0.0% | **100.0%** |
| age90 | recall | 97.6% | **100.0%** |
| medical_term | preserve | 77.2% | **100.0%** |
| mrn_repeated_tail | preserve | 0.0% | **100.0%** |
| name | recall | 77.6% | **97.2%** |
| word_level (whole-sentence diff) | preserve | 97.4% (3268/3354) | **99.97%** (3353/3354) |
| age_under_90 | preserve | 87.5% (21/24) | 87.5% (21/24) — unchanged, deferred |

Every other category (dates, MRNs, phones, emails, SSN, address, hospital
names, blacklist/whitelist, clinical abbreviations, technical jargon, etc.)
was already at 100% before and after, and stayed there — no regressions.

---

## Corpus bugs found and fixed along the way

Two issues in `eval/corpus/edge_cases.jsonl` were discovered while validating
the post-fix run (not bugs in `text_deid.py` itself):

1. **`edge-age90-colon-form`** was missing "Whitfield" (the doctor's surname)
   from its declared `phi` list, which made the new word-level diff check
   flag a *correct* redaction as an unexpected one. Fixed by adding it to the
   record's `phi` list.
2. **`edge-date-adj-nightly`** and **`edge-date-adj-biweekly`** had lowercase
   check values ("nightly"/"biweekly") but the actual generated sentences
   capitalize those words sentence-initially ("Nightly"/"Biweekly"), so the
   exact-substring check could never pass regardless of whether the real
   redaction behavior was correct. Confirmed via direct `scrub()` calls that
   the actual code was already handling these correctly; fixed the corpus
   check values to match actual capitalization.

Both were caught precisely because the eval now diffs whole sentences
word-by-word rather than just checking for a fixed set of anticipated
strings — this is the mechanism working as intended, not a sign of a
lingering code bug.

---

## Not fixed (deliberately deferred)

Three issues from FAILURE_MODES.md were left as-is; the post-fix eval numbers
for these categories are unchanged from baseline by design:

- **#5 — Cross-line span bleeding** (`age_under_90` still at 87.5%, 3
  failures: `synthetic-010`, `synthetic-039`, `synthetic-040`, all
  "AGE: nn" label-swallowing). FAILURE_MODES.md explicitly calls for more
  triage before proposing a fix — it's unclear whether this is spaCy's own
  span-merging or an artifact of Presidio's result-merging, and the fix
  differs depending on the answer.
- **#7 — Inconsistent name-combination misses** (3 of the `name` category's
  remaining leaks: `synthetic-010` "Olamide Kimathi", `synthetic-013` and
  `synthetic-022` "Anjali Oyelaran"). This is an `en_core_web_sm` training
  data coverage gap on specific first+last pairings, not a deterministic
  pattern a regex could target without an unbounded whack-a-mole list.
- **#8 — Technical/PACS jargon noise** ("node" still misflagged as
  `[ADDRESS]` in `edge-ip-internal-excluded`, caught only by the word-level
  catch-all check). Same underlying cause as #7 — general NER noise on
  domain text outside the model's training exposure — with no clean regex
  fix; FAILURE_MODES.md's proposed path is either reactive whitelist
  additions as specific words are found, or evaluating a larger spaCy model.

All three remain visible in the eval output as known, tracked gaps rather
than being silently absorbed — that's what the word-level diff and the
per-category breakdown are for.

---

## How to reproduce this comparison

```bash
# pre-fix baseline (already captured in eval/baseline_results.json)
python eval/run_eval.py --update-baseline

# post-fix run, written to a separate folder so it doesn't overwrite the baseline run
python eval/run_eval.py --results-dir post_test_folder --verbose
```

`baseline_results.json` is a fixed reference point and was intentionally
**not** overwritten by the post-fix run, so it continues to represent the
pre-fix state for any future comparison.
