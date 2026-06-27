"""Celery orchestration for ``Project`` tasks.

Views build typed args (see ``home.builders``) and enqueue ``run_project``
directly; scheduled tasks use Celery's ``eta``. ``run_project`` wraps the
actual processing task to keep the Project row's status lifecycle
(PENDING -> RUNNING -> COMPLETED/FAILED) in one place.

Cancellation is status-based: the kombu SQLAlchemy transport does not support
remote control commands, so ``revoke()`` would be a no-op. ``cancel_task``
just sets CANCELLED, and the atomic PENDING -> RUNNING claim below makes the
queued message a no-op when it arrives.
"""

import logging
import os

import psutil
from celery import current_app, shared_task
from celery.signals import worker_ready
from django.db import transaction
from django.utils import timezone

from home.models import Project
from utils import setup_run_directories

logger = logging.getLogger(__name__)


def enqueue_project(project, task, args):
    """Queue ``task(args)`` for ``project`` once the current transaction commits."""
    payload = args.model_dump()
    transaction.on_commit(
        lambda: run_project.apply_async(
            args=[project.pk, task.name, payload], eta=project.scheduled_time
        )
    )


@shared_task
def run_project(project_id, task_name, args):
    """Run a project's processing task in-process, tracking status on the row."""
    claimed = Project.objects.filter(
        pk=project_id, status=Project.TaskStatus.PENDING
    ).update(
        status=Project.TaskStatus.RUNNING,
        process_pid=os.getpid(),
        updated_at=timezone.now(),
    )
    if not claimed:
        # Cancelled while queued, or a duplicate message after a restart.
        logger.info("Skipping project %s (no longer pending)", project_id)
        return None

    project = Project.objects.get(pk=project_id)
    try:
        run_dirs = setup_run_directories(project.name, project.timestamp)
        project.log_path = run_dirs["run_log_path"]
        project.save(update_fields=["log_path", "updated_at"])

        task = current_app.tasks[task_name]
        logger.info(
            "Processing project %s (%s) with %s", project.pk, project.name, task_name
        )
        result = task({**args, "run_dirs": run_dirs})

        Project.objects.filter(pk=project_id, status=Project.TaskStatus.RUNNING).update(
            status=Project.TaskStatus.COMPLETED,
            process_pid=None,
            updated_at=timezone.now(),
        )
        logger.info("Project %s finished: %s", project_id, result)
        return result
    except Exception:
        logger.exception("Error processing project %s", project_id)
        # Guarded on RUNNING so a concurrent cancellation is not overwritten.
        Project.objects.filter(pk=project_id, status=Project.TaskStatus.RUNNING).update(
            status=Project.TaskStatus.FAILED,
            process_pid=None,
            updated_at=timezone.now(),
        )
        raise


@worker_ready.connect
def _recover_projects(**kwargs):
    """Reconcile project state with reality when the worker starts.

    RUNNING projects whose recorded process is gone died with a previous
    worker, so mark them FAILED (a live pid means a pool process is already
    executing the task — leave it alone). PENDING projects are recoverable:
    scheduled (eta) messages are held unacked in worker memory and a hard
    crash drops them, so re-enqueue from the args snapshot saved on the row
    at submission. Duplicates after a graceful restart are harmless: the
    atomic PENDING -> RUNNING claim in ``run_project`` makes the extra copy
    a no-op.
    """
    now = timezone.now()
    for project in Project.objects.filter(status=Project.TaskStatus.RUNNING):
        if project.process_pid and psutil.pid_exists(project.process_pid):
            continue
        Project.objects.filter(pk=project.pk, status=Project.TaskStatus.RUNNING).update(
            status=Project.TaskStatus.FAILED, process_pid=None, updated_at=now
        )
        logger.warning("Marked orphaned running project %s as failed", project.pk)
    for project in Project.objects.filter(status=Project.TaskStatus.PENDING):
        snapshot = project.parameters or {}
        if "task" not in snapshot:
            logger.warning(
                "Project %s has no enqueued-task snapshot; marking failed", project.pk
            )
            Project.objects.filter(pk=project.pk).update(
                status=Project.TaskStatus.FAILED, updated_at=now
            )
            continue
        still_scheduled = project.scheduled_time and project.scheduled_time > now
        eta = project.scheduled_time if still_scheduled else None
        run_project.apply_async(
            args=[project.pk, snapshot["task"], snapshot["args"]], eta=eta
        )
        logger.info("Re-enqueued pending project %s (%s)", project.pk, project.name)
