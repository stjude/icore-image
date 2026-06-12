import os

from django.db import models

# Task types whose output directory uses the DeID_ prefix; all others use PHI_.
DEID_OUTPUT_TASK_TYPES = (
    "IMAGE_DEID",
    "TEXT_DEID",
    "IMAGE_DEID_EXPORT",
    "SINGLE_CLICK_ICORE",
)


class Project(models.Model):
    name = models.CharField(max_length=200)
    timestamp = models.CharField(max_length=20, blank=True, null=True)
    log_path = models.CharField(max_length=200, blank=True, null=True)
    image_source = models.CharField(
        max_length=10, choices=[("LOCAL", "Local folder"), ("PACS", "PACS")]
    )
    input_folder = models.CharField(max_length=255)
    output_folder = models.CharField(max_length=255)
    pacs_configs = models.JSONField(default=list)
    application_aet = models.CharField(max_length=255, blank=True, null=True)
    ctp_dicom_filter = models.TextField(blank=True)

    class TaskType(models.TextChoices):
        IMAGE_DEID = "IMAGE_DEID", "Image Deidentification"
        IMAGE_QUERY = "IMAGE_QUERY", "Image Query"
        HEADER_EXTRACT = "HEADER_EXTRACT", "Header Extract"
        TEXT_DEID = "TEXT_DEID", "Text Deidentification"
        IMAGE_EXPORT = "IMAGE_EXPORT", "Image Export"
        IMAGE_DEID_EXPORT = "IMAGE_DEID_EXPORT", "Image Deidentification and Export"
        SINGLE_CLICK_ICORE = "SINGLE_CLICK_ICORE", "Single Click iCore"

    task_type = models.CharField(max_length=25, choices=TaskType.choices)

    class TaskStatus(models.TextChoices):
        PENDING = "PENDING", "Pending"
        RUNNING = "RUNNING", "Running"
        COMPLETED = "COMPLETED", "Completed"
        FAILED = "FAILED", "Failed"
        CANCELLED = "CANCELLED", "Cancelled"

    status = models.CharField(
        max_length=20, choices=TaskStatus.choices, default=TaskStatus.PENDING
    )
    process_pid = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    scheduled_time = models.DateTimeField(null=True, blank=True)
    # Snapshot of the enqueued Celery task: {"task": <name>, "args": <dump>}.
    # Audit trail, and lets the worker re-enqueue lost scheduled messages.
    parameters = models.JSONField(default=dict, blank=True)

    def output_dir(self):
        """The task's actual output directory.

        ``output_folder`` is the *user-selected base directory* (e.g.
        ~/Downloads); the pipeline writes into a per-task
        ``{DeID|PHI}_{name}_{timestamp}`` subfolder of it. Returns "" when the
        fields needed to derive the subfolder are missing — never the bare
        base directory, which must not be treated as task output (deleting it
        would destroy unrelated user files).
        """
        if not (self.output_folder and self.name and self.timestamp):
            return ""
        prefix = "DeID" if self.task_type in DEID_OUTPUT_TASK_TYPES else "PHI"
        return os.path.join(self.output_folder, f"{prefix}_{self.name}_{self.timestamp}")

    def __str__(self):
        return self.name

    class Meta:
        db_table = "deid_tasks"
