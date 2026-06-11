"""Celery orchestration for ``Project`` tasks.

Replaces the legacy polling loop in ``home/management/commands/worker.py``.
The ``Project`` table remains the source of truth for task state: a beat-
scheduled dispatcher enqueues due PENDING projects, and ``process_project``
runs the matching deid Celery task in-process, updating the project's status.
"""

import logging
import os
import uuid

from celery import shared_task
from celery.signals import worker_ready
from django.db import models
from django.utils import timezone

from home.dispatch import build_project_task
from home.models import Project
from utils import setup_run_directories

logger = logging.getLogger(__name__)


@shared_task
def dispatch_projects():
    """Enqueue due PENDING projects.

    Runs on a short beat interval, replacing the old worker's 1-second poll.
    ``celery_task_id`` acts as the claim that prevents a project from being
    enqueued twice while it waits in the queue.
    """
    now = timezone.now()
    due_projects = (
        Project.objects.filter(
            status=Project.TaskStatus.PENDING, celery_task_id__isnull=True
        )
        .filter(
            models.Q(scheduled_time__isnull=True) | models.Q(scheduled_time__lte=now)
        )
        .order_by("id")
    )
    for project in due_projects:
        task_id = str(uuid.uuid4())
        claimed = Project.objects.filter(
            pk=project.pk,
            status=Project.TaskStatus.PENDING,
            celery_task_id__isnull=True,
        ).update(celery_task_id=task_id)
        if claimed:
            process_project.apply_async(args=[project.pk], task_id=task_id)
            logger.info("Enqueued project %s (%s)", project.pk, project.name)


@shared_task
def process_project(project_id):
    """Run a single project's processing as the matching deid Celery task."""
    claimed = Project.objects.filter(
        pk=project_id, status=Project.TaskStatus.PENDING
    ).update(
        status=Project.TaskStatus.RUNNING,
        process_pid=os.getpid(),
        updated_at=timezone.now(),
    )
    if not claimed:
        # Cancelled while queued, or a duplicate message after a worker
        # restart — nothing to do.
        logger.info("Skipping project %s (no longer pending)", project_id)
        return None

    project = Project.objects.get(pk=project_id)
    try:
        run_dirs = setup_run_directories()
        project.log_path = run_dirs["run_log_path"]
        project.save(update_fields=["log_path", "updated_at"])

        task, args = build_project_task(project, run_dirs)
        logger.info(
            "Processing project %s (%s) with %s", project.pk, project.name, task.name
        )
        result = task(args.model_dump())

        Project.objects.filter(
            pk=project_id, status=Project.TaskStatus.RUNNING
        ).update(
            status=Project.TaskStatus.COMPLETED,
            process_pid=None,
            updated_at=timezone.now(),
        )
        logger.info("Project %s finished: %s", project_id, result)
        return result
    except Exception:
        logger.exception("Error processing project %s", project_id)
        # Guarded on RUNNING so a concurrent cancellation is not overwritten.
        Project.objects.filter(
            pk=project_id, status=Project.TaskStatus.RUNNING
        ).update(
            status=Project.TaskStatus.FAILED,
            process_pid=None,
            updated_at=timezone.now(),
        )
        raise


@worker_ready.connect
def _clear_stale_claims(**kwargs):
    """Release dispatch claims left behind by an unclean worker shutdown.

    The dispatcher will re-enqueue these projects; if the original queue
    message also survived, ``process_project``'s atomic PENDING -> RUNNING
    transition makes the duplicate a no-op.
    """
    released = Project.objects.filter(
        status=Project.TaskStatus.PENDING, celery_task_id__isnull=False
    ).update(celery_task_id=None)
    if released:
        logger.info("Released %d stale project claim(s)", released)
