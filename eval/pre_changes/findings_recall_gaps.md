# Text de-id: recall gaps found by expanding the corpus (2026-07-27)

Following up on `eval/findings_over_redaction.md` (which covered
over-redaction / false positives), the corpus was expanded per request to
stress-test known bug classes more broadly, broaden name/report diversity,
and add realistic formatting noise. That expansion surfaced something more
serious than over-redaction: **actual missed PHI (false negatives)**. All of
these were confirmed directly against `analyzer.analyze()`, not inferred from
the eval report alone. No code was changed — findings and evidence only.

Corpus is now 60 synthetic + 49 handwritten records (up from 30 + 21).
Baseline has been updated to reflect current (imperfect) reality — these are
tracked as known issues, not silently normalized away.

---

## Finding 1 — "y/o" age shorthand is not recognized at all (confirmed gap, most severe)

`"93 y/o male presented with acute confusion."` produces **zero** entities of
any kind. None of the `AGE90PLUS` patterns (`a1`-`a6` in
`create_analyzer_engine()`) match "y/o" — they all require literal "year(s)
old", "-year-old", or an "age:" label. "y/o" is extremely common shorthand in
real dictated/EHR radiology text. This is a straightforward miss: an age of
90+ written this way passes through completely unredacted.

**This is the single most concerning finding in this pass** — it's a direct
compliance-relevant leak, not a readability problem like the over-redaction
issues.

---

## Finding 2 — Names in ALL CAPS are not recognized (confirmed gap, high impact)

Tested a new "sectioned" report template (colon-delimited, ALL-CAPS section
labels — a real style some RIS/EHR exports use) alongside the original
narrative style. Doctor surnames rendered in caps in that template
(`DR. ROSSI`, `DR. TANAKA`, `DR. AL-RASHID`'s caps form, etc.) are **not**
detected as `PERSON` at any confidence threshold down to 0.3, even though the
identical name in Title Case (`Dr. Chen`) is caught cleanly at 0.85.
Confirmed directly:

```
'Electronically signed by Dr. Chen, Radiologist.'         -> PERSON 'Chen' (0.85)
'Electronically signed by Dr. AL-RASHID, Radiologist.'     -> PERSON 'AL-RASHID' (0.85)
'ELECTRONICALLY SIGNED: DR. ROSSI'                         -> nothing detected
'ELECTRONICALLY SIGNED: DR. TANAKA'                        -> nothing detected
```

Root cause: spaCy's NER relies heavily on capitalization as a signal for
proper nouns; ALL-CAPS text removes that signal entirely. This isn't
specific to one name — it reproduced on every all-caps name tested. Any real
report or export that renders names in all caps (common in some EHR raw-text
dumps, header blocks, or signature lines) is a real leak risk today.

---

## Finding 3 — A handful of specific name combinations are missed, inconsistently

Broadened the name pool from ~10 Western names to 20 first + 20 last names
spanning more naming conventions, and paired them systematically. Most
combinations — including most non-Western names individually — are caught
fine. But a few specific combinations are missed:

| combination | result |
|---|---|
| Olamide Kimathi | **missed** |
| Sofia Oyelaran | **missed** |
| Esperanza Novak | **missed** |
| John Kimathi / John Oyelaran / John Novak | caught |
| Olamide Smith | **missed** |
| (18 other first-name + Smith pairings) | caught |

This is **not** a clean "the model fails on all non-Western names" story —
pairing the same surnames with "John" instead worked fine, and most
individual names from the broadened pool were recognized correctly. What's
real: the miss rate is non-zero and somewhat unpredictable, concentrated on
specific first+last combinations rather than a single clean rule, and it
skews toward name patterns less common in the model's training data. Worth
tracking rather than dismissing, but characterize it accurately as "an
elevated, inconsistent miss rate on certain combinations" rather than a
uniform bias — overstating it would be its own kind of inaccuracy.

---

## Finding 4 — Entity spans can bleed across a newline and swallow the next line's label

Confirmed directly, and confirmed to affect **two different entity types**,
not just one:

```python
text = 'PATIENT: Ngozi Anderson\nMRN: 902345671\n...'
# PERSON span returned: (9, 27) -> 'Ngozi Anderson\nMRN'
```

```
'PATIENT: Olamide Kimathi\nMRN: 88213456\nDATE OF SERVICE: Jan. 5, 2024\nAGE: 45\n...'
# output: '...\nDATE OF SERVICE: [DATE]: 45\n...'
```

In the first case, spaCy's PERSON span extends across the newline and
absorbs the literal word "MRN" from the following line into the same entity.
In the second, whatever matched the date span absorbs the newline plus the
word "AGE" from the line after it — `DATE OF SERVICE: Jan. 5, 2024\nAGE: 45`
collapses to `DATE OF SERVICE: [DATE]: 45`, and the label "AGE" itself
vanishes. This is what's behind the `age_under_90` category failures below —
the age *number* isn't the problem, the *label describing it* disappears.
Both only reproduce in the new sectioned/labeled template style, where a
value is immediately followed by a newline and another capitalized label
with no separating punctuation — the original narrative template's
comma/prose structure doesn't trigger it. A real formatting-shape
sensitivity worth knowing about if labeled-style reports (colon-delimited,
one field per line) are ever a real input format, not just narrative dictation.

---

## Full accounting — every category latest.json flagged, by severity

Going category-by-category through `eval/results/latest.json` rather than
just the standout examples above:

| category | rate | n | severity |
|---|---|---|---|
| `date_adjective` | **0%** preserved | 0/6 | **total failure** — every single date-adjective stress case (daily/weekly/monthly/annually/nightly/biweekly) got redacted as a date. Not "sometimes reproduces" — 100% of the time in this corpus. |
| `hyphenated_medical_term` | **10%** preserved | 1/10 | **near-total failure** — 9 of 10 hyphenated clinical-adjective stress cases got redacted as a name. |
| `name` (recall) | 78% | 83/107 | 24 real names missed — ALL-CAPS names (Finding 2) plus the inconsistent combinations in Finding 3. |
| `medical_term` | 77% preserved | 115/149 | 34 ordinary "Findings:"/"Impression:" sentences corrupted — this is the hyphenated-term bug leaking into *ordinary* generated filler text, not just the dedicated edge cases, because 4 of the 12 filler sentences happen to contain a hyphenated term. |
| `age_under_90` | 88% preserved | 21/24 | 3 failures, all the "AGE: nn" label-swallowing bug above (Finding 4), not the age value itself. |
| `age90` (recall) | 98% | 40/41 | 1 miss — the "y/o" gap (Finding 1). |
| `word_level` (catch-all) | 97.4% | 3268/3354 | 86 individual words incorrectly swallowed across the whole corpus — this is the aggregate signal, and it dropped from 99.79% on the smaller/narrower corpus to 97.4% here specifically *because* the broader, more realistic corpus surfaces more of the above. |
| `mrn_repeated_tail` | 0% preserved | 0/1 | previously known, unchanged. |

What held up clean: `technical_jargon` (6/6 — "workstation," "console," etc.
were not the problem; a *different* word, "overnight," in that same sentence
was, caught only by the word-level catch-all, not the category I'd
specifically targeted), the no-space label robustness checks, and the
irregular-whitespace name check — all 100%.

**The corrected picture:** this isn't a handful of anecdotal words slipping
through. `date_adjective` and `hyphenated_medical_term` are not edge cases in
the "rare" sense — they're categorical failures that, once you account for
how often that kind of phrasing shows up in ordinary radiology language (see
`medical_term` dropping to 77% purely from incidental exposure), likely
affect a meaningful fraction of real reports, not a handful of contrived
sentences.

---

## Suggested priority if/when this gets addressed

Re-ranking with the full numbers in view: **Finding 1 ("y/o", a genuine
leak)** and the **hyphenated-term / date-adjective categorical failures**
(90% and 100% failure rates, respectively — these are the two custom-regex
bugs from `findings_over_redaction.md`, now confirmed to be systematic rather
than occasional) are the highest priority. **Finding 2 (ALL CAPS names)** is
next — also a clean, fully-reproducible leak. Finding 4 (cross-line label
swallowing) and Finding 3 (inconsistent name-combination misses) are real but
lower-severity/more diffuse, and belong on the same list without being
first in line.
