import os

from django.conf import settings


def icore_dev(request):
    """Expose the dev-harness flag and a CSS cache-busting version to templates."""
    is_dev = os.environ.get("ICORE_DEV") == "1"
    return {"icore_dev": is_dev, "css_version": _css_version() if is_dev else ""}


def _css_version():
    """Modification time of the built stylesheet, so dev busts the cache only on a
    rebuild — stable across navigations (no flash), fresh after `npm run build:css`."""
    try:
        css_path = os.path.join(settings.STATICFILES_DIRS[0], "tailwind.min.css")
        return int(os.path.getmtime(css_path))
    except OSError:
        return ""
