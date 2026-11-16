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
