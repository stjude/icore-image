"""Text de-id checked against (report_input, expected_output) pairs.
    uv run pytest -m textdeid_eval text-deid_eval -n0 --type basic
    uv run pytest -m textdeid_eval text-deid_eval -n0 --type comp
comp also prints a detailed accuracy report after the run (basic is the default).

Cases are grouped by PHI category via the id convention
"sh-NN_<category>-NN" (e.g. "sh-01_names-03"). Pass --category <name> to
    uv run pytest -m textdeid_eval text-deid_eval -n0 --category <category>
"""

from __future__ import annotations
import difflib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
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
CASE_ID_RE = re.compile(r"^sh-\d+_(?P<category>[a-z_]+)-\d+$")

# test_full_report joins every selected case into one radiology report, placing
# each case's line under its "section" header in this order.
FULL_REPORT_ID = "full_report"
SECTION_ORDER = [
    "PATIENT INFORMATION",
    "CLINICAL HISTORY",
    "TECHNIQUE",
    "COMPARISON",
    "FINDINGS",
    "IMPRESSION",
    "ADDENDUM",
    "SIGNATURE",
]


def case_category(case_id: str) -> str | None:
    m = CASE_ID_RE.match(case_id)
    return m.group("category") if m else None


def select_cases(cases: list[dict], category: str | None) -> list[dict]:
    if not category:
        return cases
    selected = [c for c in cases if case_category(c["id"]) == category]
    if not selected:
        known = sorted({cat for c in cases if (cat := case_category(c["id"]))})
        raise pytest.UsageError(
            f"--category {category!r} matched no cases. Known categories: "
            f"{', '.join(known)}"
        )
    return selected


def load_cases() -> list[dict]:
    cases: list[dict] = []
    for path in sorted(CASES_DIR.glob("*.json")):
        with path.open() as f:
            try:
                case_list = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}: invalid JSON: {e}") from e
            if not isinstance(case_list, list):
                raise ValueError(f"{path}: expected a JSON array of cases")
            for i, case in enumerate(case_list):
                missing = {"id", "report_input", "expected_output"} - case.keys()
                if missing:
                    raise ValueError(
                        f"{path}: case {i}: missing keys {sorted(missing)}"
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
TOKEN_RE = re.compile(r"\[[A-Z0-9+]+\]|[^\W_]+(?:[-'/.][^\W_]+)*")


@dataclass
class CaseScore:
    expected_redactions: int = 0
    missed_redactions: int = 0
    non_phi_words: int = 0
    leaks: list[tuple[str, str]] = field(default_factory=list)
    over_redacted: list[tuple[str, str]] = field(default_factory=list)
    over_redacted_words: int = 0
    label_mismatches: list[tuple[str, str]] = field(default_factory=list)
    other: list[tuple[str, str]] = field(default_factory=list)
    missed_by_label: Counter = field(default_factory=Counter)
    # Input-token positions of every leaked PHI word and over-redacted word;
    # test_full_report maps these back to the case each token came from.
    problem_idx: set[int] = field(default_factory=set)


def phi_token_map(
    report_input: str, expected: str
) -> tuple[dict[int, list[int]], dict[int, set[str]]]:
    inp, exp = TOKEN_RE.findall(report_input), TOKEN_RE.findall(expected)
    phi: dict[int, list[int]] = {}
    kept: dict[int, set[str]] = {}
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(
        None, inp, exp, autojunk=False
    ).get_opcodes():
        ph = [j for j in range(j1, j2) if PLACEHOLDER_RE.fullmatch(exp[j])]
        if tag == "equal" or not ph:
            continue
        words = {exp[j] for j in range(j1, j2) if j not in ph}
        for k, i in enumerate(range(i1, i2)):
            phi[i] = [ph[k]] if len(ph) == i2 - i1 else ph
            kept[i] = words
    return phi, kept


def score_case(report_input: str, expected: str, actual: str) -> CaseScore:
    inp, exp, act = (TOKEN_RE.findall(t) for t in (report_input, expected, actual))
    is_ph = PLACEHOLDER_RE.fullmatch
    phi, kept = phi_token_map(report_input, expected)
    s = CaseScore(
        expected_redactions=sum(1 for t in exp if is_ph(t)),
        non_phi_words=len(inp) - len(phi),
    )
    leaked_ph: set[int] = set()

    def exp_context(ph_idx: set[int]) -> str:
        return " ".join(exp[max(min(ph_idx) - 1, 0) : max(ph_idx) + 2])

    def add_leak(input_idx: list[int], act_lo: int, act_hi: int) -> None:
        ph_idx = {j for i in input_idx for j in phi[i]}
        leaked_ph.update(ph_idx)
        s.problem_idx.update(input_idx)
        s.leaks.append(
            (exp_context(ph_idx), " ".join(act[max(act_lo - 1, 0) : act_hi + 1]))
        )

    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(
        None, inp, act, autojunk=False
    ).get_opcodes():
        if tag == "equal":
            run: list[int] = []
            for i in [*range(i1, i2), None]:
                if i is not None and i in phi:
                    run.append(i)
                elif run:
                    lo, hi = j1 + run[0] - i1, j1 + run[-1] - i1
                    add_leak(run, lo, hi + 1)
                    run = []
            continue
        removed_phi = [i for i in range(i1, i2) if i in phi]
        lost = [i for i in range(i1, i2) if i not in phi]
        a_ph = [t for t in act[j1:j2] if is_ph(t)]
        a_words = [t for t in act[j1:j2] if not is_ph(t)]
        leaked = [
            w
            for w in a_words
            if any(w in inp[i] and w not in kept[i] for i in removed_phi)
        ]
        pair = (
            " ".join(inp[max(i1 - 1, 0) : i2 + 1]),
            " ".join(act[max(j1 - 1, 0) : j2 + 1]),
        )
        if leaked:
            add_leak(removed_phi, j1, j2)
        elif removed_phi:
            ph_idx = {j for i in removed_phi for j in phi[i]}
            if Counter(exp[j] for j in ph_idx) != Counter(a_ph):
                s.label_mismatches.append((exp_context(ph_idx), pair[1]))
        if lost:
            s.over_redacted_words += len(lost)
            s.over_redacted.append(pair)
            s.problem_idx.update(lost)
        elif a_words and not leaked:
            s.other.append(pair)
    s.missed_redactions = len(leaked_ph)
    s.missed_by_label.update(exp[j] for j in leaked_ph)
    return s


def score_output(case: dict, outputs: dict[str, str]) -> CaseScore:
    return score_case(
        case["report_input"], case["expected_output"], outputs[case["id"]]
    )


def build_full_report(cases: list[dict]) -> tuple[str, str, list[str | None]]:
    by_section: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        by_section[c.get("section", "FINDINGS")].append(c)
    extra = sorted(set(by_section) - set(SECTION_ORDER))
    inputs: list[str] = []
    expected: list[str] = []
    owners: list[str | None] = []
    for section in [s for s in SECTION_ORDER + extra if s in by_section]:
        inputs.append(f"{section}:")
        expected.append(f"{section}:")
        owners.append(None)
        for c in by_section[section]:
            in_lines = c["report_input"].split("\n")
            inputs += in_lines
            expected += c["expected_output"].split("\n")
            owners += [c["id"]] * len(in_lines)
    return "\n".join(inputs), "\n".join(expected), owners


def score_full_report(
    cases: list[dict], outputs: dict[str, str]
) -> tuple[CaseScore, list[str]]:
    report_input, expected, line_owners = build_full_report(cases)
    score = score_case(report_input, expected, outputs[FULL_REPORT_ID])
    token_owners = [
        owner
        for owner, line in zip(line_owners, report_input.split("\n"))
        for _ in TOKEN_RE.findall(line)
    ]
    failing = {o for i in score.problem_idx if (o := token_owners[i])}
    by_id = {c["id"]: c for c in cases}
    only_in_full = [
        cid
        for cid in sorted(failing)
        if not ((s := score_output(by_id[cid], outputs)).leaks or s.over_redacted)
    ]
    return score, only_in_full


def _comprehensive_report(
    cases: list[dict], outputs: dict[str, str], detail: bool = False
) -> list[str]:
    def pct(n, d):
        return f"{n / d:.1%}" if d else "n/a"

    scores = {c["id"]: score_output(c, outputs) for c in cases}
    total = CaseScore()
    for s in scores.values():
        total.expected_redactions += s.expected_redactions
        total.missed_redactions += s.missed_redactions
        total.non_phi_words += s.non_phi_words
        total.over_redacted_words += s.over_redacted_words
        total.missed_by_label.update(s.missed_by_label)
    correct = total.expected_redactions - total.missed_redactions
    leak_free = sum(1 for s in scores.values() if not s.leaks)
    over_free = sum(1 for s in scores.values() if not s.over_redacted)
    n_labels = sum(len(s.label_mismatches) for s in scores.values())
    exact = sum(1 for c in cases if outputs[c["id"]] == c["expected_output"])
    n = len(cases)

    lines = [
        f"Pairs: {n}",
        "",
        f"1. Redactions: {correct}/{total.expected_redactions} correct "
        f"({pct(correct, total.expected_redactions)}); "
        f"cases with every redaction correct: {leak_free}/{n} ({pct(leak_free, n)})",
        f"   Different placeholder label (PHI still removed): {n_labels}",
        f"2. Over-redaction: {total.over_redacted_words} of {total.non_phi_words} "
        f"non-PHI words redacted ({pct(total.over_redacted_words, total.non_phi_words)}); "
        f"cases with no over-redaction: {over_free}/{n} ({pct(over_free, n)})",
        f"Exact matches: {exact}/{n}",
        "",
    ]
    full_cases = full_report_cases(cases)
    full, only_in_full = score_full_report(full_cases, outputs)
    full_correct = full.expected_redactions - full.missed_redactions
    lines += [
        f"3. Full report ({len(full_cases)} cases joined): "
        f"{full_correct}/{full.expected_redactions} redactions correct, "
        f"{full.over_redacted_words} of {full.non_phi_words} non-PHI words over-redacted",
        "   Cases that pass alone but fail in the full report: "
        + (", ".join(only_in_full) or "none"),
        "",
        f"{'category':<14}{'cases':>6}{'redactions ok':>15}{'over-redacted words':>21}",
    ]
    by_category: dict[str, list[str]] = defaultdict(list)
    for c in cases:
        by_category[case_category(c["id"]) or c["id"].split("-")[0]].append(c["id"])
    for category, ids in sorted(by_category.items()):
        ok = sum(1 for i in ids if not scores[i].leaks)
        over = sum(scores[i].over_redacted_words for i in ids)
        lines.append(f"{category:<14}{len(ids):>6}{f'{ok}/{len(ids)}':>15}{over:>21}")

    lines += ["", f"{'placeholder':<20}{'expected':>9}{'missed':>8}{'recall':>9}"]
    expected_by_label = Counter(
        p for c in cases for p in PLACEHOLDER_RE.findall(c["expected_output"])
    )
    for ph, count in sorted(expected_by_label.items()):
        missed = total.missed_by_label[ph]
        lines.append(f"{ph:<20}{count:>9}{missed:>8}{pct(count - missed, count):>9}")

    flagged = [
        c for c in cases if scores[c["id"]].leaks or scores[c["id"]].over_redacted
    ]
    flagged.sort(
        key=lambda c: (
            "review" in c,
            not scores[c["id"]].leaks,
            -scores[c["id"]].over_redacted_words,
            c["id"],
        )
    )
    lines += [
        "",
        f"Cases with leaks or over-redaction: {len(flagged)} of {n} "
        "(leaks first, then most over-redacted words)",
    ]
    for c in flagged:
        s = scores[c["id"]]
        status = (
            f"review: {c['review']}"
            if "review" in c
            else ("LEAK" if s.leaks else "OVER-REDACTED")
        )
        lines += ["", f"{c['id']}  [{status}]"]
        for label, src, items in (
            ("LEAK (PHI left in output)", "expected", s.leaks),
            (
                "OVER-REDACTED (non-PHI text removed or changed)",
                "input",
                s.over_redacted,
            ),
            (
                "LABEL (redacted with a different placeholder)",
                "expected",
                s.label_mismatches,
            ),
            ("OTHER (text added to output)", "input", s.other),
        ):
            lines += [f"  {label}: {src} {e!r} -> got {a!r}" for e, a in items]
        if detail:
            for label, text in (
                ("Input", c["report_input"]),
                ("Expected", c["expected_output"]),
                ("Actual", outputs[c["id"]]),
            ):
                lines += [f"  {label}:"] + [f"    | {row}" for row in text.split("\n")]
    if detail:
        full_input, full_expected, _ = build_full_report(full_cases)
        lines += ["", "FULL REPORT"]
        lines += [f"  LEAK: expected {e!r} -> got {a!r}" for e, a in full.leaks]
        lines += [
            f"  OVER-REDACTED: input {e!r} -> got {a!r}" for e, a in full.over_redacted
        ]
        for label, text in (
            ("Input", full_input),
            ("Expected", full_expected),
            ("Actual", outputs[FULL_REPORT_ID]),
        ):
            lines += [f"  {label}:"] + [f"    | {row}" for row in text.split("\n")]
    return lines


def full_report_cases(cases: list[dict]) -> list[dict]:
    return [
        c for c in cases if not c.get("to_keep_list") and not c.get("to_remove_list")
    ]


@pytest.fixture(scope="module")
def deid_outputs(tmp_path_factory, request) -> Iterator[dict[str, str]]:
    cases = select_cases(load_cases(), request.config.getoption("deid_category"))

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for case in cases:
        key = (
            tuple(case.get("to_keep_list", [])),
            tuple(case.get("to_remove_list", [])),
        )
        groups[key].append(case)
    full_input, _, _ = build_full_report(full_report_cases(cases))
    groups[((), ())].append({"id": FULL_REPORT_ID, "report_input": full_input})

    outputs: dict[str, str] = {}
    for n, ((keep, remove), group) in enumerate(sorted(groups.items(), key=str)):
        outputs.update(
            run_text_deid(group, tmp_path_factory.mktemp(f"batch_{n}"), keep, remove)
        )
    yield outputs
    if request.config.getoption("deid_type") == "comp":
        reporter = request.config.pluginmanager.get_plugin("terminalreporter")
        reporter.write_sep("=", "text-deid comprehensive report")
        for line in _comprehensive_report(cases, outputs):
            reporter.write_line(line)
        full_report = _comprehensive_report(cases, outputs, detail=True)
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


def pytest_generate_tests(metafunc):
    if "case" not in metafunc.fixturenames:
        return
    cases = select_cases(load_cases(), metafunc.config.getoption("deid_category"))
    metafunc.parametrize("case", [_as_param(c) for c in cases])


def test_redactions(case: dict, deid_outputs: dict[str, str]) -> None:
    s = score_output(case, deid_outputs)
    assert not s.leaks, "\n".join(f"expected {e!r} -> got {a!r}" for e, a in s.leaks)


def test_over_redaction(case: dict, deid_outputs: dict[str, str]) -> None:
    s = score_output(case, deid_outputs)
    assert not s.over_redacted, f"{s.over_redacted_words} words over-redacted:\n" + (
        "\n".join(f"input {e!r} -> got {a!r}" for e, a in s.over_redacted)
    )


def test_full_report(request, deid_outputs: dict[str, str]) -> None:
    cases = select_cases(load_cases(), request.config.getoption("deid_category"))
    s, only_in_full = score_full_report(full_report_cases(cases), deid_outputs)
    problems = [f"LEAK: expected {e!r} -> got {a!r}" for e, a in s.leaks] + [
        f"OVER-REDACTED: input {e!r} -> got {a!r}" for e, a in s.over_redacted
    ]
    if only_in_full:
        problems.insert(
            0, "Pass alone but fail in the full report: " + ", ".join(only_in_full)
        )
    assert not problems, (
        f"{s.missed_redactions} of {s.expected_redactions} redactions missed, "
        f"{s.over_redacted_words} words over-redacted:\n" + "\n".join(problems)
    )