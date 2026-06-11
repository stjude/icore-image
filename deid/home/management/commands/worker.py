"""Run the Celery worker that processes iCore tasks.

This command replaces the legacy custom task manager that polled the Project
table and shelled out to the iCore CLI. Processing now runs as Celery tasks
(see ``home/tasks.py`` and the top-level ``tasks`` module), with the sqlite
database serving as the broker. Beat runs embedded in the worker and triggers
``home.tasks.dispatch_projects``, which enqueues due PENDING projects.

The command keeps the ``manage.py worker`` entry point so the Electron app
and the packaged ``manage`` binary continue to work unchanged.
"""

import os

from django.core.management.base import BaseCommand


def run_worker():
    from config.celery import app

    app.worker_main(
        [
            "worker",
            "--beat",
            "--loglevel=INFO",
            # One task at a time, matching the legacy worker's sequential
            # processing: pipelines bind fixed ports (e.g. storescp) and are
            # resource-heavy, so they must not run concurrently.
            "--concurrency=1",
        ]
    )


class Command(BaseCommand):
    help = "Run the Celery worker (with embedded beat) that processes iCore tasks."

    def handle(self, *args, **options):
        # In development, run the worker under Django's autoreloader so edits to
        # the worker / pipeline orchestration code restart it automatically.
        if os.environ.get("ICORE_DEV") == "1":
            from django.utils import autoreload

            print("Starting worker with autoreload (dev mode)")
            autoreload.run_with_reloader(run_worker)
        else:
            run_worker()
