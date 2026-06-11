"""Run the Celery worker that processes iCore tasks.

Views enqueue tasks directly (see ``home/tasks.py``); the worker just consumes
the queue, with the sqlite database serving as the broker. The command keeps
the ``manage.py worker`` entry point so the Electron app and the packaged
``manage`` binary continue to work unchanged.
"""

import os

from django.core.management.base import BaseCommand


def run_worker():
    from config.celery import app

    app.worker_main(
        [
            "worker",
            "--loglevel=INFO",
            # One task at a time: pipelines bind fixed ports (e.g. storescp)
            # and are resource-heavy, so they must not run concurrently.
            "--concurrency=1",
        ]
    )


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
