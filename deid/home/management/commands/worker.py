"""Run the Celery worker that processes iCore tasks.

Views enqueue tasks directly (see ``home/tasks.py``); the worker just consumes
the queue, with the sqlite database serving as the broker. The command keeps
the ``manage.py worker`` entry point so the Electron app and the packaged
``manage`` binary continue to work unchanged.
"""

import os
import sys

from django.core.management.base import BaseCommand


def run_worker():
    from config.celery import app

    argv = [
        "worker",
        "--loglevel=INFO",
        # One task at a time: pipelines bind fixed ports (e.g. storescp)
        # and are resource-heavy, so they must not run concurrently.
        "--concurrency=1",
    ]
    # The default prefork pool relies on fork(), which Windows lacks. The solo
    # pool runs tasks in the main process, which is fine since concurrency is 1.
    if sys.platform == "win32":
        argv.append("--pool=solo")
    app.worker_main(argv)


class Command(BaseCommand):
    help = "Run the Celery worker that processes iCore tasks."

    def handle(self, *args, **options):
        # In development, run the worker under Django's autoreloader so edits to
        # the worker / pipeline orchestration code restart it automatically.
        if os.environ.get("ICORE_DEV") == "1":
            from django.utils import autoreload

            print("Starting worker with autoreload (dev mode)")
            autoreload.run_with_reloader(run_worker)
        else:
            run_worker()
