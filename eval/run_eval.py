"""Evaluate text-deid the way the system actually runs it: .xlsx in, .xlsx out.

Run with the project's own virtualenv (it already has presidio + spaCy
installed):

    .venv/bin/python eval/run_eval.py
    .venv/bin/python eval/run_eval.py --verbose
    .venv/bin/python eval/run_eval.py --update-baseline

Earlier versions of this script called scrub() directly with a raw Python
string, which only tests the PHI-detection engine in isolation. That skips
the layer that is the *only* way this code is ever actually invoked in
production:
an uploaded .xlsx goes through pd.read_excel(), a column_actions mapping
selects which columns get de-identified vs. dropped vs. left alone
(columns_to_deid / columns_to_drop), and the result is written back out with
pd.to_excel(). There is no .dcm involved in this stage at all.

So this script builds a real multi-row .xlsx per run — shaped like a
Primordial-style RIS export (Acc / MRN / Study Date / Report columns) — and
calls TextDeidPipeline(...).run() for real, exactly like test_textdeid.py
does, then scores the actual output.xlsx cell-by-cell. Every corpus record
becomes one spreadsheet row instead of a bare string passed to scrub().

Column layout and actions are illustrative defaults (Acc/Study Date kept,
MRN/Report de-identified) — adjust COLUMN_ACTIONS below to match whatever
column_actions configuration your real projects actually use.
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.pipelines import TextDeidPipeline  # noqa: E402
from utils import RunDirs  # noqa: E402

CORPUS_DIR = Path(__file__).resolve().parent / "corpus"
BASELINE_PATH = Path(__file__).resolve().parent / "baseline_results.json"
RESULTS_DIR = Path(__file__).resolve().parent / "results"
WORKDIR = Path(__file__).resolve().parent / "_workdir"

# --- Spreadsheet shape -------------------------------------------------------
# Mirrors detect_file_type_and_columns()'s "Primordial" layout (Acc column,
# an MRN-named column, a recognized study-date column name) plus a free-text
# report column. "Report" holds each corpus record's report text.
COLUMN_ACTIONS = {"Acc": "keep", "MRN": "deid", "Study Date": "keep", "Report": "deid"}
DEID_COLUMNS = [c for c, a in COLUMN_ACTIONS.items() if a == "deid"]
DROP_COLUMNS = [c for c, a in COLUMN_ACTIONS.items() if a == "drop"] or None

# Deliberately "boring" MRN/date values for the structural MRN/Study Date
# columns — cycled per row, distinct from any values embedded in a record's
# "Report" text via fixtures.py. None of these trip the code's narrow
# preserve-carve-outs (all-zero, repeated-tail, etc.); that's covered
# separately by the dedicated edge cases in corpus/edge_cases.jsonl.
SAFE_MRN_POOL = [
    "4521873",
    "88213456",
    "902345671",
    "1029384756",
    "MRN-102938",
    "A1234567",
    "AB123456",
]
SAFE_DATE_POOL = [
    "01/05/2024",
    "11/23/2023",
    "01-05-2024",
    "November 23, 2023",
    "Jan. 5, 2024",
]

# --- Whole-sentence, word-level false-positive detection --------------------
# The phi/preserve checks above only catch words we explicitly anticipated
# and listed in the corpus. Anything we didn't think to list (e.g. "node" and
# "Post-operative" in real output) went uncaught. Since every corpus record
# is synthetic, and we know exactly which substrings are the real injected
# PHI, everything else in that sentence is, by construction, non-PHI. So
# instead of hand-listing individual preserve values, we reconstruct what the
# Report cell *should* look like if only the known PHI got redacted, and diff
# that word-by-word against what the pipeline actually produced. Any
# placeholder in the real output that isn't accounted for in the expected
# text is a word we never told the eval to check an automatically
# discovered false positive.
PLACEHOLDER_RE = re.compile(r"^\[[A-Z0-9+]+\]$")

# Maps each corpus "category" label to the placeholder scrub() would use for
# it, per the `operators` dict in pipeline/stages/text_deid.py. Only phi
# categories need an entry, preserve categories are never redacted so
# there's nothing to reconstruct.
CATEGORY_PLACEHOLDER = {
    "name": "[PERSONALNAME]",
    "name_hyphenated": "[PERSONALNAME]",
    "name_initials": "[PERSONALNAME]",
    "name_special_pine": "[PERSONALNAME]",
    "date": "[DATE]",
    "mrn": "[ALPHANUMERICID]",
    "mrn_column": "[ALPHANUMERICID]",
    "phone": "[ALPHANUMERICID]",
    "email": "[ALPHANUMERICID]",
    "ssn": "[ALPHANUMERICID]",
    "address": "[ADDRESS]",
    "ip": "[ALPHANUMERICID]",
    "age90": "[AGE90+]",
    "blacklist": "[REDACTED]",
}


def build_expected_text(text: str, phi_items: list[dict]) -> str:
    """Reconstruct what the Report cell *should* look like if only the known
    PHI values were redacted and every other word were left untouched.
    """
    spans = []
    for item in phi_items:
        placeholder = CATEGORY_PLACEHOLDER.get(item.get("category"))
        if placeholder is None:
            continue  # unmapped category -- skip rather than guess
        start = text.find(item["value"])
        if start == -1:
            continue  # value isn't literally in this text -- skip
        spans.append((start, start + len(item["value"]), placeholder))
    spans.sort(key=lambda s: s[0])

    pieces = []
    cursor = 0
    for start, end, placeholder in spans:
        if start < cursor:
            continue  # overlapping span, already covered
        pieces.append(text[cursor:start])
        pieces.append(placeholder)
        cursor = end
    pieces.append(text[cursor:])
    return "".join(pieces)


def word_level_checks(
    record_id: str, expected_text: str, actual_text: str
) -> list["ItemResult"]:
    """Word-by-word diff of expected vs. actual Report output.

    Every non-PHI word becomes its own "word_level" preserve check: passed if
    it survived, failed if it got swallowed into a placeholder the eval never
    told the code to produce there. Known-PHI spans (already scored by the
    phi/preserve checks above) are skipped so they aren't double-counted.
    """
    expected_tokens = re.findall(r"\S+", expected_text)
    actual_tokens = re.findall(r"\S+", actual_text)
    sm = difflib.SequenceMatcher(None, expected_tokens, actual_tokens)

    results: list[ItemResult] = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        expected_slice = expected_tokens[i1:i2]
        actual_slice = actual_tokens[j1:j2]

        if tag == "equal":
            for word in expected_slice:
                if PLACEHOLDER_RE.match(word):
                    continue  # a known PHI placeholder, already scored elsewhere
                results.append(
                    ItemResult(
                        record_id,
                        "word_level",
                        word,
                        "preserve",
                        True,
                        actual_text[:200],
                    )
                )
            continue

        if any(PLACEHOLDER_RE.match(t) for t in expected_slice):
            # This span is a known PHI value: either correctly redacted
            # (won't reach here, it'd be "equal") or a leak -- already
            # scored by the explicit phi check for this record.
            continue

        placeholders_here = [t for t in actual_slice if PLACEHOLDER_RE.match(t)]
        passed = (
            not placeholders_here
        )  # a placeholder here means an unanticipated redaction
        for word in expected_slice:
            results.append(
                ItemResult(
                    record_id, "word_level", word, "preserve", passed, actual_text[:200]
                )
            )
    return results


@dataclass
class ItemResult:
    record_id: str
    category: str
    value: str
    kind: str  # "phi" or "preserve"
    passed: bool
    output_snippet: str = ""


def load_corpus() -> list[dict]:
    records: list[dict] = []
    for path in sorted(CORPUS_DIR.glob("*.jsonl")):
        with path.open() as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"{path}:{line_no}: invalid JSON — {e}") from e
                record["_source"] = path.name
                records.append(record)
    return records


def group_into_batches(records: list[dict]) -> dict[tuple, list[tuple[int, dict]]]:
    """Group rows by (to_keep_list, to_remove_list).

    TextDeidPipeline takes these lists once per run, for the whole
    spreadsheet — they aren't a per-row concept in production. Records with
    custom keep/remove lists (rare — e.g. the blacklist/whitelist edge case)
    get their own batch/spreadsheet; everything else shares one default
    batch, matching how a single real project run works.
    """
    batches: dict[tuple, list[tuple[int, dict]]] = defaultdict(list)
    for i, record in enumerate(records):
        key = (
            tuple(record.get("to_keep_list") or []),
            tuple(record.get("to_remove_list") or []),
        )
        batches[key].append((i, record))
    return batches


def build_dataframe_and_checks(
    batch_records: list[tuple[int, dict]],
) -> tuple[
    pd.DataFrame, list[tuple[str, list[tuple[str, str, str, str]], str, list[dict]]]
]:
    """Build one spreadsheet's rows + the per-row checks to run against output.

    Each entry in checks_per_row is (record_id, checks, report_text, phi_items)
    -- report_text/phi_items are carried along so evaluate() can also run the
    whole-sentence word-level diff against the real Report text.
    """
    rows = []
    checks_per_row = []
    for global_idx, record in batch_records:
        acc_value = f"ACC-{global_idx:04d}"
        mrn_value = SAFE_MRN_POOL[global_idx % len(SAFE_MRN_POOL)]
        date_value = SAFE_DATE_POOL[global_idx % len(SAFE_DATE_POOL)]

        rows.append(
            {
                "Acc": acc_value,
                "MRN": mrn_value,
                "Study Date": date_value,
                "Report": record["text"],
            }
        )

        checks: list[tuple[str, str, str, str]] = [
            ("Acc", "preserve", "structural_keep_acc", acc_value),
            ("Study Date", "preserve", "structural_keep_date", date_value),
            ("MRN", "phi", "mrn_column", mrn_value),
        ]
        for item in record.get("phi", []):
            checks.append(
                ("Report", "phi", item.get("category", "uncategorized"), item["value"])
            )
        for item in record.get("preserve", []):
            checks.append(
                (
                    "Report",
                    "preserve",
                    item.get("category", "uncategorized"),
                    item["value"],
                )
            )

        checks_per_row.append(
            (
                record.get("id", record["_source"]),
                checks,
                record["text"],
                record.get("phi", []),
            )
        )

    return pd.DataFrame(rows), checks_per_row


def run_batch(
    df: pd.DataFrame, to_keep_list: tuple, to_remove_list: tuple, workdir: Path
) -> pd.DataFrame:
    """Write a real .xlsx, run the real TextDeidPipeline, read the real output."""
    workdir.mkdir(parents=True, exist_ok=True)
    input_path = workdir / "input.xlsx"
    output_dir = workdir / "output"
    log_dir = workdir / "logs"
    appdata_dir = workdir / "appdata"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    appdata_dir.mkdir(parents=True, exist_ok=True)

    df.to_excel(input_path, index=False)

    # Explicit, isolated run_dirs so this eval never writes into the real
    # ~/Documents/iCore/logs (setup_run_directories()'s default) on whoever's
    # machine runs it.
    run_dirs = RunDirs(
        log_dir=str(log_dir),
        run_log_path=str(log_dir / "run.txt"),
        appdata_dir=str(appdata_dir),
    )

    TextDeidPipeline(
        input_file=str(input_path),
        output_dir=str(output_dir),
        to_keep_list=list(to_keep_list) or None,
        to_remove_list=list(to_remove_list) or None,
        columns_to_deid=DEID_COLUMNS,
        columns_to_drop=DROP_COLUMNS,
        run_dirs=run_dirs,
        appdata_dir=str(appdata_dir),
    ).run()

    return pd.read_excel(output_dir / "output.xlsx")


def build_incorrect_redactions_column(
    record_ids: list[str], batch_results: list["ItemResult"]
) -> list[str]:
    """Per-record summary of every value that got redacted but shouldn't
    have been -- the failed "preserve" checks (explicit categories like
    "hospital"/"ip_internal" plus the catch-all "word_level" checks),
    deduplicated per record. "Correct" when nothing was wrongly redacted.
    This is what lets "what issues are persistent" be read straight off the
    spreadsheet instead of re-deriving it from --verbose output each time.
    """
    by_record: dict[str, list[str]] = defaultdict(list)
    for r in batch_results:
        if (
            r.kind == "preserve"
            and not r.passed
            and r.value not in by_record[r.record_id]
        ):
            by_record[r.record_id].append(r.value)
    return [
        "; ".join(by_record[rid]) if by_record.get(rid) else "Correct"
        for rid in record_ids
    ]


def build_missed_phi_column(
    record_ids: list[str], batch_results: list["ItemResult"]
) -> list[str]:
    """Per-record summary of every known-PHI value that did NOT get
    redacted -- the opposite failure mode from "Incorrect Redactions": real
    PHI (name, date, MRN, age90, etc.) still sitting in the output. "None"
    when everything expected to be redacted actually was.
    """
    by_record: dict[str, list[str]] = defaultdict(list)
    for r in batch_results:
        if r.kind == "phi" and not r.passed and r.value not in by_record[r.record_id]:
            by_record[r.record_id].append(r.value)
    # NOTE: the literal string "None" is one of pandas' default NA sentinels
    # -- read_excel() silently turns it back into a blank/NaN cell on the
    # next read. "None missed" isn't a recognized sentinel, so it round-trips.
    return [
        "; ".join(by_record[rid]) if by_record.get(rid) else "None missed"
        for rid in record_ids
    ]


def build_side_by_side(
    df: pd.DataFrame,
    result_df: pd.DataFrame,
    record_ids: list[str],
    batch_label: str,
    incorrect_redactions: list[str],
    missed_phi: list[str],
) -> pd.DataFrame:
    """One row per record, input and output columns interleaved side by side
    (e.g. "Report (input)" next to "Report (output)") so a human can scan a
    single sheet instead of flipping between two files.
    """
    combined = pd.DataFrame({"batch": [batch_label] * len(df), "record_id": record_ids})
    for col in df.columns:
        combined[f"{col} (input)"] = df[col].values
        combined[f"{col} (output)"] = result_df[col].values
    combined["Incorrect Redactions"] = incorrect_redactions
    combined["Missed PHI"] = missed_phi
    return combined


def evaluate(
    records: list[dict], verbose: bool = False
) -> tuple[list[ItemResult], pd.DataFrame]:
    results: list[ItemResult] = []
    combined_frames: list[pd.DataFrame] = []
    batches = group_into_batches(records)

    for batch_num, (batch_key, batch_records) in enumerate(
        sorted(batches.items(), key=str)
    ):
        to_keep_list, to_remove_list = batch_key
        df, checks_per_row = build_dataframe_and_checks(batch_records)
        result_df = run_batch(
            df, to_keep_list, to_remove_list, WORKDIR / f"batch_{batch_num}"
        )

        batch_results: list[ItemResult] = []
        for (record_id, checks, report_text, phi_items), (_, out_row) in zip(
            checks_per_row, result_df.iterrows()
        ):
            for column, kind, category, value in checks:
                cell = str(out_row[column])
                passed = (value not in cell) if kind == "phi" else (value in cell)
                batch_results.append(
                    ItemResult(
                        record_id=record_id,
                        category=category,
                        value=value,
                        kind=kind,
                        passed=passed,
                        output_snippet=cell[:200],
                    )
                )
                if verbose and not passed:
                    tag = "LEAK" if kind == "phi" else "OVER-REDACTED"
                    print(f"[{tag}] {record_id} / {column} / {category}: {value!r}")

            # Whole-sentence check: catches words never explicitly listed as
            # phi or preserve (e.g. "node", "Post-operative") by diffing the
            # entire Report cell against what it should look like if only the
            # known PHI were redacted.
            actual_report = str(out_row["Report"])
            expected_report = build_expected_text(report_text, phi_items)
            word_results = word_level_checks(record_id, expected_report, actual_report)
            batch_results.extend(word_results)
            if verbose:
                for r in word_results:
                    if not r.passed:
                        print(
                            f"[UNEXPECTED REDACTION] {record_id}: {r.value!r} swallowed"
                        )

        results.extend(batch_results)
        record_ids = [record_id for record_id, *_ in checks_per_row]
        incorrect_redactions = build_incorrect_redactions_column(
            record_ids, batch_results
        )
        missed_phi = build_missed_phi_column(record_ids, batch_results)
        combined_frames.append(
            build_side_by_side(
                df,
                result_df,
                record_ids,
                f"batch_{batch_num}",
                incorrect_redactions,
                missed_phi,
            )
        )

    combined_df = (
        pd.concat(combined_frames, ignore_index=True)
        if combined_frames
        else pd.DataFrame()
    )
    return results, combined_df


def summarize(results: list[ItemResult]) -> dict:
    by_cat: dict[str, dict[str, int]] = defaultdict(
        lambda: {"phi_total": 0, "phi_pass": 0, "preserve_total": 0, "preserve_pass": 0}
    )
    for r in results:
        bucket = by_cat[r.category]
        if r.kind == "phi":
            bucket["phi_total"] += 1
            bucket["phi_pass"] += int(r.passed)
        else:
            bucket["preserve_total"] += 1
            bucket["preserve_pass"] += int(r.passed)

    summary = {}
    for cat, b in sorted(by_cat.items()):
        recall = b["phi_pass"] / b["phi_total"] if b["phi_total"] else None
        preserve_rate = (
            b["preserve_pass"] / b["preserve_total"] if b["preserve_total"] else None
        )
        summary[cat] = {"recall": recall, "preserve_rate": preserve_rate, **b}
    return summary


def print_report(results: list[ItemResult], summary: dict) -> None:
    print(
        "\n=== Text De-id Eval Report (real .xlsx -> TextDeidPipeline -> .xlsx) ===\n"
    )
    header = (
        f"{'category':<22}{'recall':>10}{'preserve_rate':>16}"
        f"{'phi n':>8}{'preserve n':>12}"
    )
    print(header)
    print("-" * len(header))
    for cat, s in summary.items():
        recall = f"{s['recall']:.0%}" if s["recall"] is not None else "n/a"
        preserve = (
            f"{s['preserve_rate']:.0%}" if s["preserve_rate"] is not None else "n/a"
        )
        print(
            f"{cat:<22}{recall:>10}{preserve:>16}"
            f"{s['phi_total']:>8}{s['preserve_total']:>12}"
        )

    leaks = [r for r in results if r.kind == "phi" and not r.passed]
    over_redactions = [r for r in results if r.kind == "preserve" and not r.passed]

    print(f"\nTotal PHI leaks: {len(leaks)}")
    for r in leaks:
        print(f"  [LEAK] {r.record_id} / {r.category}: {r.value!r}")
        print(f"         output: {r.output_snippet!r}")

    print(f"\nTotal over-redactions: {len(over_redactions)}")
    for r in over_redactions:
        print(f"  [OVER-REDACTED] {r.record_id} / {r.category}: {r.value!r}")
        print(f"         output: {r.output_snippet!r}")


def check_regressions(summary: dict, baseline: dict) -> list[str]:
    problems = []
    for cat, s in summary.items():
        base = baseline.get(cat)
        if base is None:
            continue
        if s["recall"] is not None and base.get("recall") is not None:
            if s["recall"] < base["recall"]:
                problems.append(
                    f"{cat}: recall dropped {base['recall']:.0%} -> {s['recall']:.0%}"
                )
        if s["preserve_rate"] is not None and base.get("preserve_rate") is not None:
            if s["preserve_rate"] < base["preserve_rate"]:
                problems.append(
                    f"{cat}: preserve_rate dropped "
                    f"{base['preserve_rate']:.0%} -> {s['preserve_rate']:.0%}"
                )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Write current results as the new baseline instead of comparing against it.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print each failure as it's found."
    )
    args = parser.parse_args()

    records = load_corpus()
    if not records:
        print(
            f"No corpus records found under {CORPUS_DIR}. Run generate_corpus.py first."
        )
        return 1

    results, combined_df = evaluate(records, verbose=args.verbose)
    summary = summarize(results)
    print_report(results, summary)

    RESULTS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with (RESULTS_DIR / f"{stamp}.json").open("w") as f:
        json.dump(summary, f, indent=2)
    with (RESULTS_DIR / "latest.json").open("w") as f:
        json.dump(summary, f, indent=2)

    combined_path_stamped = RESULTS_DIR / f"{stamp}_input_output.xlsx"
    combined_path_latest = RESULTS_DIR / "latest_input_output.xlsx"
    combined_df.to_excel(combined_path_stamped, index=False)
    combined_df.to_excel(combined_path_latest, index=False)
    print(f"\nInput/output side-by-side workbook -> {combined_path_latest}")

    if args.update_baseline:
        with BASELINE_PATH.open("w") as f:
            json.dump(summary, f, indent=2)
        print(f"\nBaseline updated -> {BASELINE_PATH}")
        return 0

    leaks = sum(1 for r in results if r.kind == "phi" and not r.passed)

    if BASELINE_PATH.exists():
        with BASELINE_PATH.open() as f:
            baseline = json.load(f)
        problems = check_regressions(summary, baseline)
        if problems:
            print("\n=== REGRESSIONS vs baseline ===")
            for p in problems:
                print(f"  - {p}")
        elif leaks == 0:
            print("\nNo regressions vs baseline.")
        return 1 if (problems or leaks) else 0

    print(
        f"\nNo baseline found at {BASELINE_PATH}. "
        "Once these results look correct, re-run with --update-baseline."
    )
    return 1 if leaks else 0


if __name__ == "__main__":
    raise SystemExit(main())
