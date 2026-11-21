"""Exact string comparisons with an automatic comprehensive report.

Run: uv run pytest test_textdeid_accuracy.py -n0 -ra

Each case and the joined report require exact actual/expected string equality.
Whitespace, punctuation, capitalization, and placeholder labels must match.
scenario and test_objective are descriptive metadata only. Pytest reports passed, failed, xfailed, and xpassed tests.
Known mismatches are listed in KNOWN_FAILURES and marked xfail. If one starts
passing, the run fails (strict xfail) so the case can be reviewed and unmarked.
The comprehensive report is printed and saved after every evaluation run.
"""

from __future__ import annotations
import difflib
import json
import re
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
import pandas as pd
import pytest
from pipeline.pipelines import TextDeidPipeline
from utils import RunDirs

DATASET_DIR = Path(__file__).resolve().parent / "textdeid_test_dataset"
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

# Known failing cases, marked xfail(strict=True) with the reason shown. If one
# starts passing (XPASS), the run fails: check the fix, then remove the entry.
KNOWN_FAILURES: dict[str, str] = {
    "sh-01_names-02": "missed PHI: unlabeled patient name in narrative not detected",
    "sh-01_names-03": "missed PHI: LAST, FIRST uppercase name not detected",
    "sh-01_names-05": "wrong label: patient initials tagged [ADDRESS] instead of [PERSONALNAME]",
    "sh-01_names-08": "over-redaction: eponyms (Hodgkin, Baker) tagged as names",
    "sh-01_names-09": "over-redaction: eponym 'Schmorl node' tagged as a name",
    "sh-01_names-10": "over-redaction: eponyms (Foley, Murphy) tagged as names",
    "sh-02_geography-04": "missed PHI: city in prior residence not redacted",
    "sh-02_geography-07": "missed PHI: standalone city name in narrative not redacted",
    "sh-03_dates_age-04": "missed PHI: 'D Mon YYYY' date format not detected",
    "sh-03_dates_age-08": "over-redaction: duration 'three weeks' tagged as [DATE]",
    "sh-03_dates_age-11": "missed PHI: age >89 followed by 'years' not redacted",
    "sh-03_dates_age-14": "over-redaction: '3-month-old' tagged as [DATE]",
    "sh-04_telephone-04": "missed PHI: international +1 space-separated phone not detected",
    "sh-04_telephone-06": "over-redaction: public department phone line redacted",
    "sh-05_fax-02": "missed PHI: slash-separated fax number not detected",
    "sh-05_fax-04": "over-redaction: public department fax line redacted",
    "sh-06_email-05": "over-redaction: public department mailbox redacted",
    "sh-07_ssn-04": "missed PHI: SSN last four not redacted",
    "sh-07_ssn-05": "missed PHI: masked SSN XXX-XX-#### not redacted",
    "sh-07_ssn-06": "over-redaction: SSN-shaped lot number redacted",
    "sh-08_mrn-03": "wrong label: legacy MRN tagged [DATE] instead of [ALPHANUMERICID]",
    "sh-08_mrn-05": "over-redaction: 'Biopsy' tagged as [ADDRESS]",
    "sh-09_healthplan-02": "missed PHI: Medicare beneficiary ID not detected",
    "sh-09_healthplan-03": "missed PHI: Medicaid ID not detected",
    "sh-11_license-01": "over-redaction: 'DRIVER' label tagged as a name",
    "sh-11_license-02": "wrong label: birth certificate number tagged [DATE] instead of [ALPHANUMERICID]",
    "sh-12_vehicle-01": "missed PHI: license plate not detected",
    "sh-12_vehicle-02": "missed PHI: VIN not detected",
    "sh-12_vehicle-03": "missed PHI: boat hull ID not detected",
    "sh-12_vehicle-04": "missed PHI: license plate in narrative not detected",
    "sh-13_device-02": "partial redaction: only the serial portion of the UDI is redacted",
    "sh-15_ip-02": "missed PHI: private IPv4 address not detected",
    "sh-15_ip-03": "missed PHI: IPv6 address not detected",
    "sh-15_ip-04": "over-redaction: software version number partly tagged as [DATE]",
    "sh-16_biometrics-01": "missed PHI: fingerprint template ID not detected; no [BIOMETRIC] label",
    "sh-16_biometrics-02": "missed PHI: voiceprint template ID not detected; no [BIOMETRIC] label",
    "sh-16_biometrics-03": "missed PHI: iris template ID not detected; no [BIOMETRIC] label",
    "sh-16_biometrics-04": "missed PHI: retinal template ID not detected; no [BIOMETRIC] label",
    "sh-16_biometrics-05": "over-redaction: anatomic term 'Iris' tagged as a name",
    "sh-17_photos-01": "face photo reference not replaced with [FACEIMAGE]; filename partly redacted",
    "sh-17_photos-02": "face photo reference not replaced with [FACEIMAGE]; filename partly redacted",
    "sh-17_photos-03": "face photo reference not replaced with [FACEIMAGE]; filename partly redacted",
    "sh-17_photos-04": "over-redaction: non-identifying wound photo filename partly redacted",
    "sh-18_other_unique-02": "wrong label: date-prefixed accession tagged [DATE] instead of [ALPHANUMERICID]",
    "sh-18_other_unique-03": "missed PHI: UUID not detected",
    "sh-18_other_unique-04": "label 'RESEARCH LINKAGE KEY' tagged as a name; key value left in place",
    "sh-18_other_unique-05": "partial redaction: DICOM UID only partly redacted",
    "sh-18_other_unique-08": "over-redaction: 'Lung-RADS 2' tagged as a name",
}

def case_category(case_id: str) -> str | None:
    m = CASE_ID_RE.match(case_id)
    return m.group("category") if m else None


def load_cases() -> list[dict]:
    cases: list[dict] = []
    for path in (DATASET_DIR / "pairs.json",):
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
    if not cases:
        raise ValueError(f"{DATASET_DIR / 'pairs.json'}: no evaluation cases found")
    ids = [c["id"] for c in cases]
    duplicates = {i for i in ids if ids.count(i) > 1}
    if duplicates:
        raise ValueError(f"Duplicate case ids: {sorted(duplicates)}")
    return cases


def build_input_frame(cases: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Acc": [c["id"] for c in cases],
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
        columns_to_deid=["Report"],
        run_dirs=RunDirs(
            log_dir=str(log_dir),
            run_log_path=str(log_dir / "run.txt"),
            appdata_dir=str(appdata_dir),
        ),
        appdata_dir=str(appdata_dir),
    ).run()
    out = pd.read_excel(output_dir / "output.xlsx", keep_default_na=False, dtype=str)
    return dict(zip(out["Acc"], out["Report"]))


def build_full_report(cases: list[dict]) -> tuple[str, str]:
    by_section: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        by_section[c.get("section", "FINDINGS")].append(c)
    extra = sorted(set(by_section) - set(SECTION_ORDER))
    inputs: list[str] = []
    expected: list[str] = []
    for section in [s for s in SECTION_ORDER + extra if s in by_section]:
        inputs.append(f"{section}:")
        expected.append(f"{section}:")
        for c in by_section[section]:
            in_lines = c["report_input"].split("\n")
            inputs += in_lines
            expected += c["expected_output"].split("\n")
    return "\n".join(inputs), "\n".join(expected)


def _comprehensive_report(cases: list[dict], outputs: dict[str, str]) -> list[str]:
    """Describe exact matches and show complete strings for mismatches."""
    counts = dict.fromkeys(("PASS", "FAIL", "XFAIL", "XPASS"), 0)
    mismatches = []
    by_category = defaultdict(lambda: [0, 0])
    for case in cases:
        actual = outputs[case["id"]]
        matches = actual == case["expected_output"]
        if case["id"] in KNOWN_FAILURES:
            status = "XPASS" if matches else "XFAIL"
        else:
            status = "PASS" if matches else "FAIL"
        counts[status] += 1
        category = case_category(case["id"]) or "uncategorized"
        by_category[category][0] += matches
        by_category[category][1] += 1
        if status != "PASS":
            mismatches.append((case, status))

    full_input, full_expected = build_full_report(full_report_cases(cases))
    full_actual = outputs[FULL_REPORT_ID]
    full_status = "PASS" if full_actual == full_expected else "FAIL"
    lines = [
        f"Individual cases: {len(cases)}",
        ", ".join(f"{status}: {count}" for status, count in counts.items()),
        f"Full report: {full_status}",
        "",
        "Category: exact matches / cases",
    ]
    for category, (matches, total) in sorted(by_category.items()):
        lines.append(f"  {category}: {matches}/{total}")

    for case, status in mismatches:
        lines += ["", f"{case['id']} [{status}]"]
        if case["id"] in KNOWN_FAILURES:
            lines.append(f"  Xfail:    {KNOWN_FAILURES[case['id']]}")
        # repr makes whitespace and newline differences visible.
        lines += [
            f"  Input:    {case['report_input']!r}",
            f"  Expected: {case['expected_output']!r}",
            f"  Actual:   {outputs[case['id']]!r}",
        ]
    lines += ["", f"FULL REPORT [{full_status}]"]
    lines += [
        f"  Input:    {full_input!r}",
        f"  Expected: {full_expected!r}",
        f"  Actual:   {full_actual!r}",
        "",
        "Full report diff (expected versus actual):",
    ]
    lines.extend(
        difflib.unified_diff(
            full_expected.splitlines(keepends=True),
            full_actual.splitlines(keepends=True),
            fromfile="expected",
            tofile="actual",
            lineterm="",
        )
    )
    return lines


def full_report_cases(cases: list[dict]) -> list[dict]:
    return [
        c for c in cases if not c.get("to_keep_list") and not c.get("to_remove_list")
    ]


@pytest.fixture(scope="module")
def deid_outputs(tmp_path_factory, request) -> Iterator[dict[str, str]]:
    cases = load_cases()

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for case in cases:
        key = (
            tuple(case.get("to_keep_list", [])),
            tuple(case.get("to_remove_list", [])),
        )
        groups[key].append(case)
    full_input, _ = build_full_report(full_report_cases(cases))
    groups[((), ())].append({"id": FULL_REPORT_ID, "report_input": full_input})

    outputs: dict[str, str] = {}
    for n, ((keep, remove), group) in enumerate(sorted(groups.items(), key=str)):
        outputs.update(
            run_text_deid(group, tmp_path_factory.mktemp(f"batch_{n}"), keep, remove)
        )
    yield outputs
    full_report = _comprehensive_report(cases, outputs)
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_sep("=", "text-deid comprehensive report")
        for line in full_report:
            reporter.write_line(line)
    report_path = DATASET_DIR / "comprehensive_report.md"
    report_path.write_text(
        "# Text de-id comprehensive report\n\n```\n"
        + "\n".join(full_report)
        + "\n```\n"
    )
    if reporter is not None:
        reporter.write_line(f"Report written to {report_path}")


def pytest_generate_tests(metafunc):
    if "case" not in metafunc.fixturenames:
        return
    cases = load_cases()
    unknown = set(KNOWN_FAILURES) - {c["id"] for c in cases}
    if unknown:
        raise ValueError(f"KNOWN_FAILURES has unknown case ids: {sorted(unknown)}")
    metafunc.parametrize(
        "case", [pytest.param(c, id=c["id"], marks=case_marks(c)) for c in cases]
    )


def case_marks(case: dict) -> list[pytest.MarkDecorator]:
    """Mark a KNOWN_FAILURES case xfail; strict=True makes an XPASS fail the run."""
    reason = KNOWN_FAILURES.get(case["id"])
    if not reason:
        return []
    return [pytest.mark.xfail(reason=reason, raises=AssertionError, strict=True)]


def test_report_deid(case: dict, deid_outputs: dict[str, str]) -> None:
    """Compare the complete actual output with the expected output."""
    assert deid_outputs[case["id"]] == case["expected_output"]


@pytest.mark.xfail(
    reason="Known joined-report exact-output mismatches; see comprehensive_report.md",
    raises=AssertionError,
    strict=True,
)
def test_full_report(deid_outputs: dict[str, str]) -> None:
    cases = load_cases()
    _, expected = build_full_report(full_report_cases(cases))
    assert deid_outputs[FULL_REPORT_ID] == expected
