# Text de-id eval framework

Automated scoring for `pipeline/stages/text_deid.py`: builds a real `.xlsx`
input, runs it through the actual `TextDeidPipeline`, and checks the real
`output.xlsx` cell-by-cell against a corpus of records with known PHI and
known non-PHI. There is no mocking of Presidio, spaCy, or the pipeline —
this exercises the exact code path a production run does (`pd.read_excel()`
-> de-id -> `pd.to_excel()`), just with synthetic data.

It answers two questions any change to `text_deid.py` should be checked
against:

- **Recall** — did any real PHI (a name, MRN, date, etc.) survive
  unredacted? This is a leak.
- **Preserve rate** — did any legitimate clinical text (a medical term, a
  hospital name, an address deliberately excluded by policy) get redacted
  when it shouldn't have been? This is an over-redaction.

There are two independent corpora in this repo, scored by the same script:

- `eval/corpus/` — the main corpus, used to tune and regression-test
  `text_deid.py` day to day.
- `eval/final_textdeid_test/corpus/` — a held-out corpus, written
  independently of the main one (different names, MRNs, hospitals,
  phrasing), used to check that a fix generalizes rather than just matching
  the exact strings it was written against. Separate `fixtures.py`/
  `generate_corpus.py`, and (since it's scored via `--corpus-dir`) its own
  separate baseline/results/workdir too.

Everything under both folders is fabricated. No real patient data is used
anywhere in this framework.

## Layout

```
eval/
  run_eval.py             # the one script -- scores whichever corpus you point it at
  fixtures.py             # name/date/MRN/etc. pools used to build synthetic reports
  generate_corpus.py      # generates eval/corpus/synthetic_v1.jsonl (pure stdlib, no presidio/spacy needed)
  corpus/
    synthetic_v1.jsonl    # generated synthetic reports
    edge_cases.jsonl       # hand-written, one-off regression cases

eval/final_textdeid_test/  # held-out corpus, same shape, no run_eval.py of its own
  fixtures.py
  generate_corpus.py
  corpus/
    synthetic_validation.jsonl
    edge_cases.jsonl
```

`run_eval.py` also writes `results/` (timestamped JSON + a side-by-side
input/output `.xlsx`), a `_workdir/` scratch directory, and
`baseline_results.json` when asked — all created next to whichever corpus
directory you pointed it at (so `eval/final_textdeid_test/` gets its own
copies of these when you run against that corpus). None of these are
checked in — see "Generated output" at the bottom.

## Quick start

From the repo root, using the project's virtualenv (it already has
presidio + spaCy installed):

```bash
.venv/bin/python eval/run_eval.py
```

First run, with no baseline yet, ends with:

```
No baseline found at eval/baseline_results.json. Once these results look
correct, re-run with --update-baseline.
```

Read the report (see "Interpreting results" below), and once it looks
right, lock it in as the baseline everything else compares against:

```bash
.venv/bin/python eval/run_eval.py --update-baseline
```

## Running it

```bash
# main (tuning) corpus -- default when --corpus-dir is omitted
.venv/bin/python eval/run_eval.py
.venv/bin/python eval/run_eval.py --verbose             # print each failure as it's found
.venv/bin/python eval/run_eval.py --update-baseline      # write current results as the new baseline
.venv/bin/python eval/run_eval.py --results-dir foo       # write results/ to eval/foo instead of eval/results

# held-out validation corpus -- same script, pointed at the other corpus
.venv/bin/python eval/run_eval.py --corpus-dir eval/final_textdeid_test/corpus
.venv/bin/python eval/run_eval.py --corpus-dir eval/final_textdeid_test/corpus --verbose
```

Exit code is `0` if there are no PHI leaks and nothing regressed against the
baseline, `1` otherwise — usable as a CI gate, not just a manual report.

If you've changed `fixtures.py` or want to regenerate the synthetic half of
either corpus (fixed seed, reproducible):

```bash
python3 eval/generate_corpus.py
python3 eval/final_textdeid_test/generate_corpus.py
```

(`generate_corpus.py` is pure stdlib — no virtualenv needed. `run_eval.py`
needs the project's real dependencies, since it runs the actual pipeline.)

## Validating a change to text_deid.py

This is the workflow the framework is built around:

1. **Capture a baseline before touching any code**, on the branch/commit you're
   about to change:

   ```bash
   .venv/bin/python eval/run_eval.py --update-baseline
   ```

2. **Make your change** to `pipeline/stages/text_deid.py`.

3. **Re-run without `--update-baseline`** and read the report:

   ```bash
   .venv/bin/python eval/run_eval.py --verbose
   ```

   A `=== REGRESSIONS vs baseline ===` section means some category's
   `recall` or `preserve_rate` dropped relative to the baseline you just
   captured — i.e. your change broke something that used to work. No section
   printed (plus "No regressions vs baseline.") means nothing got worse.

4. **Check generalization against the held-out corpus** — a fix that only
   works on the strings it was tuned against isn't done yet:

   ```bash
   .venv/bin/python eval/run_eval.py --corpus-dir eval/final_textdeid_test/corpus --update-baseline
   # ... make no further code changes, or re-run without --update-baseline
   # to compare a later change against this same held-out baseline ...
   ```

5. Once you're happy with the result on both corpora, update both baselines
   (`--update-baseline` on each) so future changes compare against the new,
   correct behavior.

### Comparing two branches directly

Baselines are not checked in (see "Generated output"), so to compare a
branch against `main` instead of against a previously-saved baseline:

```bash
git checkout main
.venv/bin/python eval/run_eval.py --update-baseline
git checkout <your-branch>
.venv/bin/python eval/run_eval.py
```

### Adding a test case

Append a line to the relevant `corpus/*.jsonl` file:

```json
{"id": "edge-my-case", "description": "what this checks", "text": "...", "phi": [{"category": "name", "value": "..."}], "preserve": [{"category": "medical_term", "value": "..."}]}
```

- `phi`: values that must be redacted. If any survive in the output, that's
  a leak.
- `preserve`: values that must survive verbatim. If any get redacted, that's
  an over-redaction.
- `category` is a free-text label used to group results in the report (e.g.
  `name`, `date_adjective`, `mrn_repeated_tail`) — reuse an existing category
  where it fits, or add a new one; the summary just buckets by whatever
  string you use.

You don't need to touch `run_eval.py` to add a case — it's data-driven.

## Interpreting results

A run prints a per-category console table, e.g.:

```
category                  recall   preserve_rate   phi n  preserve n
--------------------------------------------------------------------
age90                       100%             n/a      41           0
date_adjective                n/a            83%       0           6
medical_term                  n/a           100%       0         126
name                         94%             n/a      96           0
```

...followed by every individual failure, tagged by what kind of failure it
is:

```
[LEAK] synthetic-014 / name: 'Ingrid Farouk'
       output: 'Patient Ingrid Farouk was seen for...'

[OVER-REDACTED] edge-date-adj-hourly / date_adjective: 'hourly'
       output: 'Vitals checked [DATE].'
```

- **`[LEAK]`** — a `phi` value that should have been redacted but wasn't.
  Real PHI left in the output.
- **`[OVER-REDACTED]`** — a `preserve` value that should have survived but
  got blacked out. Legitimate text lost.

The same data is written as JSON (`results/latest.json`). Each category
looks like:

```json
"age90": {
  "recall": 1.0,
  "preserve_rate": null,
  "phi_total": 25,
  "phi_pass": 25,
  "preserve_total": 0,
  "preserve_pass": 0
}
```

- **`recall`** — of the values in this category that *should* be redacted
  (`phi_total`), the fraction that actually were (`phi_pass`). This is the
  leak-detection number: lower recall means real PHI is slipping through
  unredacted. `null` if the category has no `phi`-type checks.
- **`preserve_rate`** — of the values that should *survive* untouched
  (`preserve_total`), the fraction that did (`preserve_pass`). This is the
  over-redaction number: lower preserve_rate means legitimate clinical text
  is being needlessly blacked out. `null` if the category has no
  `preserve`-type checks.
- A category is normally either recall-only or preserve_rate-only, not
  both — e.g. `age90` only has PHI values (ages that must be redacted),
  `medical_term` only has preserve values (jargon that must survive).

One category is special: **`word_level`**. Explicit `phi`/`preserve` entries
only catch words someone thought to list in the corpus ahead of time.
`word_level` supplements that by reconstructing what each `Report` cell
*should* look like if only the declared `phi` values were redacted, then
diffing that word-by-word against the real output. Any placeholder
(`[NAME]`, `[DATE]`, etc.) that shows up somewhere the expected text didn't
have one is an **unanticipated over-redaction** — a false positive nobody
explicitly wrote a test for. This is what catches surprises like ordinary
words ("node", "Post-operative") getting swallowed, without having to
hand-list every non-PHI word in every sentence.

The generated `.xlsx` in `results/` (`latest_input_output.xlsx`) puts input
and output columns side by side per record, plus two summary columns:
**Incorrect Redactions** (every value wrongly redacted in that record, or
"Correct") and **Missed PHI** (every real PHI value that slipped through, or
"None missed") — useful for scanning every failure at a glance without
re-running with `--verbose`.

A **regression** is a category whose `recall` or `preserve_rate` in the
current run is lower than the same category in `baseline_results.json`.
An *improvement* (a number going up) is never flagged — only drops are
regressions. `run_eval.py` exits non-zero if there are regressions or any
PHI leak at all.

## Generated output (not checked in)

`results/`, `_workdir/`, and `baseline_results.json` (wherever `run_eval.py`
writes them — `eval/` by default, `eval/final_textdeid_test/` when run with
`--corpus-dir eval/final_textdeid_test/corpus`) are gitignored. They're
fully reproducible by re-running the commands above, and by nature represent
a single point-in-time run rather than the framework itself — see the
repo's `.gitignore` for the exact patterns. If you use a custom
`--results-dir`, that folder is also meant to be temporary/local; don't
commit it either.

Point-in-time findings from a specific tuning or validation run (what was
broken, what was fixed, before/after numbers) belong in the PR description
for the change that prompted the run, not in this directory — see the
repo's PR history for examples.
