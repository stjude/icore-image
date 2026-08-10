# Held-out validation results — `final_textdeid_test/`

This corpus is independent of `eval/corpus/` (the tuning set the five fixes
in `pipeline/stages/text_deid.py` were written and iterated against — see
`eval/CHANGES_APPLIED.md`). Every name, hospital, MRN, date, hyphenated term,
and edge-case sentence in `fixtures.py` / `generate_corpus.py` /
`corpus/edge_cases.jsonl` here is new — none of it was used while writing or
debugging those fixes. 106 rows total (50 generated synthetic reports + 56
hand-written edge cases). No code was changed for this task; this is a
measurement only.

Full numbers: `results/latest.json` (also saved as this folder's own
`baseline_results.json` via `--update-baseline`, so future runs here can
track regressions independently of the tuning corpus's baseline).

---

## The five fixes generalize

| Fix | Category | Result on held-out data |
|---|---|---|
| #1 hyphenated terms | `hyphenated_medical_term` | **100%** (10/10) — 10 brand-new terms (poorly-defined, well-circumscribed, single-phase, etc.) |
| #1 side effect | `medical_term` | **100%** (126/126) |
| #2 date-adjectives (known words) | `date_adjective` | 5/5 of the *originally covered* words (daily, weekly, monthly, nightly, overnight) passed in new sentences |
| #3 y/o shorthand | `age90` | **100%** (25/25) — including new formats never tested during tuning: `y/o`, `y.o.` with periods, and `104yo` with no space |
| #3 negative control | `age_under_90` (the `84 y/o` case specifically) | correct — not falsely redacted |
| #4 ALL-CAPS "Dr. X" names | `name` | 3/3 new probes passed (`DR. KESSLER`, `DR. OKONKWO`, `DR. VAN DYKE`) |
| #5 MRN repeated-tail | `mrn_repeated_tail` | **100%** (1/1) — new digit string |

Also unchanged/clean on the new data: dates, MRNs, phones, emails, SSN,
address, IP (public + internal-excluded), hospital names, blacklist-style
categories not otherwise exercised, clinical abbreviations, measurements,
vitals, gestational age, relative dates, bare years, name initials, the
"Pine" special case, and 5 of 6 new technical-jargon words.

---

## Known limitations confirmed to generalize (not new bugs)

These reproduce on entirely new data, which is expected — they were always
documented as model-level limitations rather than something the fixes
targeted:

- **ALL-CAPS names without "Dr."** — `DR NISHIMURA` (no period) was missed,
  exactly as predicted: fix #4's pattern requires the literal "Dr." token.
  This is the fix's documented scope boundary, not a regression.
- **Issue #7, inconsistent name-combination misses** — on this held-out
  corpus, `name` recall came in at **93.75%** (90/96), with **5 new leaked
  pairings** never seen during tuning: Ingrid Farouk, Aditi Chukwu, Magnus
  Osei, Saoirse Volkov, Saoirse Farouk. This confirms Issue #7 isn't specific
  to the four pairings originally found (Olamide Kimathi, etc.) — it's a
  broader, unpredictable gap in `en_core_web_sm`'s name coverage that shows
  up on fresh names too, consistent with FAILURE_MODES.md's assessment that
  this has no clean regex fix.
- **Issue #5, cross-line span bleeding** — `age_under_90` came in at **76.7%**
  (23/30), and the failures reproduce the same shape on a different label
  pair than originally documented: not just `DATE OF SERVICE: ...\nAGE: nn`,
  but also `PATIENT: Name\nMRN: ...` in a couple of rows, where the name span
  swallowed the word "MRN" from the next line. This is new evidence that the
  bleed isn't limited to the one label pair in FAILURE_MODES.md #5 — it's a
  general property of the sectioned template's lack of punctuation between
  a value and the next line's label.

---

## Two new findings from testing broader phrasing

Not previously documented, found only because this corpus used different
wording than the tuning set:

1. **`date_adjective` word list is exact-match, not general.** Of 4 new
   probe words deliberately chosen to be *outside* the fixed list added in
   fix #2 (`hourly`, `quarterly`, `fortnightly`, `biannually`), 2 still got
   redacted as dates (`hourly`, `quarterly`) while 2 happened not to
   (`fortnightly`, `biannually` weren't tagged `DATE_TIME` by the underlying
   model in these sentences at all). Net: `date_adjective` preserve rate is
   **77.8%** (7/9) on this corpus. This confirms fix #2 is, as designed, a
   fixed word-list patch rather than a general "date-adjective" detector —
   worth knowing if new adjectives keep turning up in real reports.
2. **The `duration` exclusion doesn't handle spelled-out numbers.** "Symptoms
   have persisted for three weeks" got redacted to "...for [DATE]." The
   existing exclusion (per FAILURE_MODES.md's description) appears to require
   a literal digit before the unit ("3 weeks"), so "three weeks" isn't
   recognized as a duration and falls through to date redaction.
   `duration` preserve rate: **0%** (0/1) on this corpus — a new, narrow
   over-redaction case, not related to any of the five fixes. Flagging for
   awareness; not fixed here per the scope of this task (build a test set,
   not change code).

---

## How to re-run

```bash
python eval/final_textdeid_test/generate_corpus.py   # regenerate the synthetic half (fixed seed, reproducible)
python eval/final_textdeid_test/run_eval.py --verbose
python eval/final_textdeid_test/run_eval.py --update-baseline   # after intentional future changes
```

This folder's `baseline_results.json` is independent of `eval/baseline_results.json`
(the tuning set's baseline) — comparisons here track regressions against this
held-out corpus specifically.
