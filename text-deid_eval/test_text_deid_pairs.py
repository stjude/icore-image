"""Text de-id checked against (report_input, expected_output) pairs.
    uv run pytest -m textdeid_eval text-deid_eval -n0 --type basic
    uv run pytest -m textdeid_eval text-deid_eval -n0 --type comp
comp also prints a detailed accuracy report after the run (basic is the default).
"""

from __future__ import annotations

import difflib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest

from pipeline.pipelines import TextDeidPipeline
from utils import RunDirs

pytestmark = pytest.mark.textdeid_eval

CASES_DIR = Path(__file__).resolve().parent / "cases"

COLUMN_ACTIONS = {"Acc": "keep", "MRN": "deid", "Study Date": "keep", "Report": "deid"}
DEID_COLUMNS = [c for c, a in COLUMN_ACTIONS.items() if a == "deid"]
PLACEHOLDER_MRN = "4521873"
PLACEHOLDER_DATE = "01/05/2024"


def load_cases() -> list[dict]:
    cases: list[dict] = []
    for path in sorted(CASES_DIR.glob("*.jsonl")):
        with path.open() as f:
            for line_no, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                try:
                    case = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"{path}:{line_no}: invalid JSON: {e}") from e
                missing = {"id", "report_input", "expected_output"} - case.keys()
                if missing:
                    raise ValueError(
                        f"{path}:{line_no}: missing keys {sorted(missing)}"
                    )
                cases.append(case)
    ids = [c["id"] for c in cases]
    duplicates = {i for i in ids if ids.count(i) > 1}
    if duplicates:
        raise ValueError(f"Duplicate case ids: {sorted(duplicates)}")
    return cases


def build_input_frame(cases: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Acc": [c["id"] for c in cases],
            "MRN": [PLACEHOLDER_MRN] * len(cases),
            "Study Date": [PLACEHOLDER_DATE] * len(cases),
            "Report": [c["report_input"] for c in cases],
        }
    )


def run_text_deid(
    cases: list[dict],
    workdir: Path,
    to_keep_list: tuple = (),
    to_remove_list: tuple = (),
) -> dict[str, str]:
    input_path = workdir / "input.xlsx"
    output_dir = workdir / "output"
    log_dir = workdir / "logs"
    appdata_dir = workdir / "appdata"
    for d in (output_dir, log_dir, appdata_dir):
        d.mkdir(parents=True, exist_ok=True)

    build_input_frame(cases).to_excel(input_path, index=False)

    TextDeidPipeline(
        input_file=str(input_path),
        output_dir=str(output_dir),
        to_keep_list=list(to_keep_list) or None,
        to_remove_list=list(to_remove_list) or None,
        columns_to_deid=DEID_COLUMNS,
        run_dirs=RunDirs(
            log_dir=str(log_dir),
            run_log_path=str(log_dir / "run.txt"),
            appdata_dir=str(appdata_dir),
        ),
        appdata_dir=str(appdata_dir),
    ).run()
    out = pd.read_excel(output_dir / "output.xlsx", keep_default_na=False, dtype=str)
    return dict(zip(out["Acc"], out["Report"]))


PLACEHOLDER_RE = re.compile(r"\[[A-Z0-9+]+\]")


def _word_diff(expected: str, actual: str) -> tuple[list, list, list]:
    """Word-level diff -> (leaks, unexpected redactions, other differences)."""
    exp, act = expected.split(), actual.split()
    leaks, over, other = [], [], []
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, exp, act).get_opcodes():
        if tag == "equal":
            continue
        e, a = " ".join(exp[i1:i2]), " ".join(act[j1:j2])
        e_ph, a_ph = bool(PLACEHOLDER_RE.search(e)), bool(PLACEHOLDER_RE.search(a))
        if e_ph and not a_ph:
            leaks.append((e, a))
        elif a_ph and not e_ph:
            over.append((e, a))
        else:
            other.append((e, a))
    return leaks, over, other


def _comprehensive_report(
    cases: list[dict], outputs: dict[str, str], detail: bool = False
) -> list[str]:
    def passed(c):
        return outputs[c["id"]] == c["expected_output"]

    def pct(n, d):
        return f"{n / d:.1%}" if d else "n/a"

    lines = [f"Pairs: {len(cases)}"]
    settled = [c for c in cases if "review" not in c]
    review = [c for c in cases if "review" in c]
    lines += [
        f"Settled pairs passing: {sum(map(passed, settled))}/{len(settled)} "
        f"({pct(sum(map(passed, settled)), len(settled))})",
        f"Needs-review pairs matching expected: {sum(map(passed, review))}/{len(review)}",
        "",
        "By id prefix (all pairs, exact match):",
    ]
    by_prefix = defaultdict(list)
    for c in cases:
        by_prefix[c["id"].split("-")[0]].append(c)
    for prefix, group in sorted(by_prefix.items()):
        n_ok = sum(map(passed, group))
        lines.append(
            f"  {prefix:<12}{n_ok:>4}/{len(group):<4}{pct(n_ok, len(group)):>8}"
        )
    exp_count, act_count, matched = Counter(), Counter(), Counter()
    for c in cases:
        e = Counter(PLACEHOLDER_RE.findall(c["expected_output"]))
        a = Counter(PLACEHOLDER_RE.findall(outputs[c["id"]]))
        exp_count.update(e)
        act_count.update(a)
        matched.update(e & a)
    lines += [
        "",
        f"{'placeholder':<20}{'expected':>9}{'produced':>9}{'recall':>9}{'precision':>11}",
    ]
    for ph in sorted(set(exp_count) | set(act_count)):
        lines.append(
            f"{ph:<20}{exp_count[ph]:>9}{act_count[ph]:>9}"
            f"{pct(matched[ph], exp_count[ph]):>9}{pct(matched[ph], act_count[ph]):>11}"
        )
    lines.append("(counts per pair, so a redaction in the wrong place can still count)")

    # Word-level diff of expected vs. actual output.
    total_words = sum(
        1
        for c in cases
        for w in c["expected_output"].split()
        if not PLACEHOLDER_RE.search(w)
    )
    mismatches = []
    for c in cases:
        if not passed(c):
            mismatches.append((c, *_word_diff(c["expected_output"], outputs[c["id"]])))
    n_leak = sum(len(m[1]) for m in mismatches)
    n_over = sum(len(m[2]) for m in mismatches)
    n_other = sum(len(m[3]) for m in mismatches)
    lines += [
        "",
        f"Word-level: {n_over} unexpected redactions in {total_words} "
        f"non-redacted words ({pct(n_over, total_words)}), "
        f"{n_leak} leaks, {n_other} other differences",
        "",
        f"Mismatched cases: {len(mismatches)} of {len(cases)} "
        "(FAIL first, then review cases with leaks, then the rest)",
    ]
    mismatches.sort(key=lambda m: ("review" in m[0], not m[1], not m[2], m[0]["id"]))
    for c, leaks, over, other in mismatches:
        status = f"review: {c['review']}" if "review" in c else "FAIL"
        lines += ["", f"{c['id']}  [{status}]"]
        for label, items in (
            ("LEAK (expected redacted, output kept it)", leaks),
            ("UNEXPECTED REDACTION (output redacted, expected kept it)", over),
            ("OTHER", other),
        ):
            lines += [f"  {label}: expected {e!r} -> got {a!r}" for e, a in items]
        if not (leaks or over or other):
            lines.append("  differs only in whitespace or line breaks")
        if detail:
            for label, text in (
                ("Input", c["report_input"]),
                ("Expected", c["expected_output"]),
                ("Actual", outputs[c["id"]]),
            ):
                lines += [f"  {label}:"] + [f"    | {row}" for row in text.split("\n")]
    return lines


@pytest.fixture(scope="module")
def deid_outputs(tmp_path_factory, request) -> Iterator[dict[str, str]]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for case in load_cases():
        key = (
            tuple(case.get("to_keep_list", [])),
            tuple(case.get("to_remove_list", [])),
        )
        groups[key].append(case)

    outputs: dict[str, str] = {}
    for n, ((keep, remove), group) in enumerate(sorted(groups.items(), key=str)):
        outputs.update(
            run_text_deid(group, tmp_path_factory.mktemp(f"batch_{n}"), keep, remove)
        )
    yield outputs
    if request.config.getoption("deid_type") == "comp":
        reporter = request.config.pluginmanager.get_plugin("terminalreporter")
        reporter.write_sep("=", "text-deid comprehensive report")
        for line in _comprehensive_report(load_cases(), outputs):
            reporter.write_line(line)
        full_report = _comprehensive_report(load_cases(), outputs, detail=True)
        report_path = Path(__file__).resolve().parent / "comprehensive_report.md"
        report_path.write_text(
            "# Text de-id comprehensive report\n\n```\n"
            + "\n".join(full_report)
            + "\n```\n"
        )
        reporter.write_line(f"Report written to {report_path}")


def _as_param(case: dict):
    marks = []
    if "review" in case:
        marks.append(pytest.mark.xfail(reason=f"needs review: {case['review']}"))
    return pytest.param(case, id=case["id"], marks=marks)


@pytest.mark.parametrize("case", [_as_param(c) for c in load_cases()])
def test_report_deid(case: dict, deid_outputs: dict[str, str]) -> None:
    assert deid_outputs[case["id"]] == case["expected_output"]
