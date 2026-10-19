import os

from django.conf import settings

from .models import AppState


def icore_dev(request):
    """Expose the dev-harness flag and a CSS cache-busting version to templates."""
    is_dev = os.environ.get("ICORE_DEV") == "1"
    return {"icore_dev": is_dev, "css_version": _css_version() if is_dev else ""}


def admin_password_reset(request):
    """Flag base.html uses to show the blocking set-admin-password modal."""
    return {
        "admin_password_reset_required": AppState.load().admin_password_reset_required
    }


def _css_version():
    """Modification time of the built stylesheet, so dev busts the cache only on a
    rebuild — stable across navigations (no flash), fresh after `npm run build:css`."""
    try:
        css_path = os.path.join(settings.STATICFILES_DIRS[0], "tailwind.min.css")
        return int(os.path.getmtime(css_path))
    except OSError:
        return ""
