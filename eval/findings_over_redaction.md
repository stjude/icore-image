# Text de-id: over-redaction findings (2026-07-27)

Source: manual review of `input.xlsx` / `output.xlsx` (uploaded, 50-row batch
produced by `eval/run_eval.py`'s default batch), diffed row-by-row and
confirmed against direct `analyzer.analyze()` calls against
`pipeline/stages/text_deid.py::create_analyzer_engine()`. No code was changed
as part of this pass — findings and proposed fixes only.

Of the 50 rows, every redaction corresponds to real injected PHI (names,
MRNs, emails, dates, ages 90+) **except** the three cases below, which are
words that are not identifiers being redacted anyway.

---

## Issue 1 — Hyphenated compound words flagged as names (regex bug, has a clean fix)

**Example (row 12):**
`"Post-operative note reviewed with patient Wilson-Cook regarding follow-up."`
→ `"[PERSONALNAME] note reviewed with patient [PERSONALNAME] regarding follow-up."`

"Post-operative" is not a name, but it matched the custom hyphenated-name
pattern in `create_analyzer_engine()`:

```python
Pattern(
    name="n3",
    regex=r"\b(?!\d+\-year)(?!\d+\-day)(?!follow\-)(?!year\-old)[A-Z][a-z]{2,}\-[A-Z][a-z]{2,}\b",
    score=0.85,
),
```

**Root cause:** this pattern was clearly written assuming case-sensitive
matching — both halves must start with a capital letter, which is a
reasonable proper-noun heuristic ("Wilson-Cook" looks like a name;
"post-operative" doesn't). But `presidio_analyzer.PatternRecognizer` defaults
its `global_regex_flags` parameter to `re.DOTALL | re.MULTILINE |
re.IGNORECASE`, and `create_analyzer_engine()` never overrides it for the
`name_extras_recognizer`. So in practice the pattern matches *any* two
hyphen-joined words of 3+ letters each, regardless of case.

Confirmed directly (via `analyzer.analyze()` and by testing the raw regex
with/without `re.IGNORECASE`):

| word | flagged as NAMEPERSON today | should be flagged |
|---|---|---|
| `Wilson-Cook` | yes | yes |
| `Post-operative` | yes | **no** |
| `well-documented` | yes | **no** |
| `chart-based` | yes | **no** |
| `well-known` | yes | **no** |

In real radiology dictation this generalizes to a lot of legitimate clinical
vocabulary: well-defined, ill-defined, non-displaced, post-traumatic,
low-grade, high-grade, T2-weighted, and so on — anything of the shape
`word-word` where each half is 3+ letters.

**Proposed fix (not applied):** override `global_regex_flags` for just this
recognizer (or just this one pattern) to drop `re.IGNORECASE`, e.g.:

```python
name_extras_recognizer = PatternRecognizer(
    supported_entity="NAMEPERSON",
    name="names",
    global_regex_flags=re.DOTALL | re.MULTILINE,  # no IGNORECASE
    patterns=[...],
)
```

**Caveat before applying:** `PatternRecognizer.global_regex_flags` applies to
every pattern in that recognizer, not just n3. The other four patterns in
`name_extras_recognizer` (n1 initials, n2 "F. M. Last", n4 "Pine", n5
"Patient F.M.", n6 "The" before "MD") all already require literal capital
letters in their regex, so removing IGNORECASE for the whole recognizer
should be safe and *only* tightens n3 — but this should be verified against
the existing `edge-name-initials` and `edge-name-hyphenated` corpus cases
(and a run of the full eval) before shipping, to confirm real names are
still caught. Other recognizers in `create_analyzer_engine()` (dates, MRNs,
address) also inherit the same IGNORECASE default and were **not** audited
here — same fix pattern would apply to any of them if similar bugs turn up,
but each should be checked independently since some (e.g. date month names)
may have legitimate reasons to stay case-insensitive.

---

## Issue 2 — "node" flagged as an address (NLP model noise, no regex to fix)

**Example (row 1):**
`"Internal PACS node 10.2.5.1 processed the study."`
→ `"Internal PACS [ADDRESS] 10.2.5.1 processed the study."`

Confirmed via direct `analyzer.analyze()` call: spaCy's `en_core_web_sm`
model itself tags "node" as a `LOCATION` entity (score 0.85) — this is not
one of the custom regex recognizers, it's the underlying general-purpose NER
model. `en_core_web_sm` is a small, general-English model with no exposure to
PACS/radiology jargon during training, so it occasionally misreads technical
nouns as place names. There's no single regex fix for this class of error —
it's inherent to using a lightweight general-purpose NER model on
domain-specific text.

**Proposed solutions (not applied), roughly cheapest to most involved:**

1. Add specific known false-positive words (`node`, etc.) to
   `NLM_PRESERVE_MEDICAL` as they're discovered — cheap, but reactive/
   whack-a-mole; only catches words a human has already seen misfire.
2. Evaluate a larger spaCy model (`en_core_web_md` or `en_core_web_lg`) for
   better NER precision on this kind of text — larger bundle/slower
   inference, needs a side-by-side precision/recall comparison (the eval
   harness in `eval/` is exactly the tool for that comparison) before
   committing.
3. Longer-term: a domain-tuned clinical NER model (e.g. scispaCy or a
   med-specific NER model) would likely reduce this whole class of error,
   but is a real engineering investment, not a config change.

---

## Issue 3 — "daily" flagged as a date (NLP model noise, no regex to fix)

**Example (row 16):**
`"...independent with activities of daily living."`
→ `"...independent with activities of [DATE] living."`

Confirmed via direct `analyzer.analyze()` call: Presidio's built-in
(spaCy-based) date recognizer tags "daily" as `DATE_TIME` (score 0.85) on its
own, independent of surrounding context. None of the existing `scrub()`
exclusion filters catch it — `duration` requires a number before the unit
(e.g. "3 weeks"), `relative_date` only covers yesterday/today/tomorrow,
`year_only` and `gestational` don't apply either. So "daily" slips through
every existing carve-out.

**Proposed solutions (not applied):**

1. Add a new exclusion filter to `scrub()`, mirroring the existing
   `duration`/`relative_date` pattern, for common date-adjectives that aren't
   actual dates: `daily`, `weekly`, `monthly`, `annual`/`annually`, `nightly`,
   etc. This is the most surgical fix and matches the code's existing style
   (a small compiled regex + `continue` in the `DATE_TIME` branch of the
   filter loop).
2. Same "add to the eval corpus as a preserve case" tracking as issue 2, so
   any fix (or regression) is caught automatically going forward.

---

## Suggested path forward

Issue 1 has a clear, scoped, low-risk fix (drop `IGNORECASE` for the name
recognizer) once verified against the eval corpus. Issues 2 and 3 are model
noise rather than bugs in the custom logic — the cheapest mitigation for
both is the same pattern already used for the `mrn_repeated_tail` quirk
found earlier: add the specific known-bad words as documented edge cases in
`eval/corpus/edge_cases.jsonl` so they're tracked as known, non-regressing
issues, and revisit the "bigger model" question if this class of error shows
up often enough in real reports to be worth the size/speed tradeoff.

No changes have been made to `pipeline/stages/text_deid.py`,
`eval/run_eval.py`, or `eval/corpus/*.jsonl` as part of this pass, per
request — this document is findings + proposed fixes only.
