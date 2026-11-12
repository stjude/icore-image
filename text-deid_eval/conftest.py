def pytest_addoption(parser):
    parser.addoption(
        "--type",
        dest="deid_type",
        choices=["basic", "comp"],
        default="basic",
        help="text-deid pairs report: basic (pass/fail) or comp (detailed report)",
    )
