import os


def icore_dev(request):
    """Expose whether we're running under the dev harness to templates."""
    return {"icore_dev": os.environ.get("ICORE_DEV") == "1"}
