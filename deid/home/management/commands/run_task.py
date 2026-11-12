"""Run one processing task in this process, reading its args as JSON on stdin.

``home.tasks.run_project`` launches this as a child of the worker so each
project runs in its own process: cancelling a project kills that process tree
without taking the worker down with it.
"""

import json
import sys

from celery import current_app
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run one processing task with JSON args from stdin."
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument("task_name")

    def handle(self, *args, **options):
        import tasks  # noqa: F401  (registers the processing tasks)

        task = current_app.tasks[options["task_name"]]
        result = task(json.load(sys.stdin))
        self.stdout.write(f"{task.name} finished: {result}")
