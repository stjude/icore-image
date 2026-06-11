import os
import sys
from pathlib import Path

from celery import Celery

# The processing modules (module_*.py, utils.py, pipeline/) live in the
# repository root, one level above the Django project directory.
REPO_ROOT = Path(__file__).resolve().parents[2]
for path in (str(REPO_ROOT), str(REPO_ROOT / "deid")):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("icore")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
