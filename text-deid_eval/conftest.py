import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--type",
        dest="deid_type",
        choices=["basic", "comp"],
        default="basic",
        help="text-deid pairs report: basic (pass/fail) or comp (detailed report)",
    )
    parser.addoption(
        "--category",
        dest="deid_category",
        default=None,
        help=(
            "run only cases for one PHI category, matching the "
            "'sh-NN_<category>-NN' id convention (e.g. names, geography, "
            "dates_age, ssn, mrn, ...). Omit to run every category."
        ),
    )


TEST_LABELS = {
    "test_redactions": "missed PHI",
    "test_over_redaction": "over-redaction",
    "test_full_report": "full report",
}
OUTCOME_COLORS = {"failed": "red", "passed": "green"}


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return (yield)

    original = reporter.build_summary_stats_line

    def split_summary():
        parts, color = original()
        counts = {
            label: dict.fromkeys(OUTCOME_COLORS, 0) for label in TEST_LABELS.values()
        }
        for outcome in OUTCOME_COLORS:
            for report in reporter.stats.get(outcome, []):
                name = report.nodeid.split("::")[-1].split("[")[0]
                if getattr(report, "when", None) == "call" and name in TEST_LABELS:
                    counts[TEST_LABELS[name]][outcome] += 1

        # Take the split tests out of pytest's "N failed" / "N passed" totals.
        remaining_parts = []
        for text, markup in parts:
            count, _, outcome = text.partition(" ")
            if outcome in OUTCOME_COLORS:
                left = int(count) - sum(c[outcome] for c in counts.values())
                if left:
                    remaining_parts.append((f"{left} {outcome}", markup))
            else:
                remaining_parts.append((text, markup))

        grouped = []
        for label, by_outcome in counts.items():
            shown = [(o, n) for o, n in by_outcome.items() if n]
            for i, (outcome, n) in enumerate(shown):
                prefix = f"{label}: " if i == 0 else ""
                grouped.append(
                    (f"{prefix}{n} {outcome}", {OUTCOME_COLORS[outcome]: True})
                )
        return grouped + remaining_parts, color

    # Pytest has no public hook for replacing just the final summary line.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(reporter, "build_summary_stats_line", split_summary)
        return (yield)
