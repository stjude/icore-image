from django.db import models


class Project(models.Model):
    name = models.CharField(max_length=200)
    image_source = models.CharField(max_length=10, choices=[('LOCAL', 'Local folder'), ('PACS', 'PACS')])
    input_folder = models.CharField(max_length=255)
    output_folder = models.CharField(max_length=255)
    pacs_ip = models.CharField(max_length=255, blank=True, null=True)
    pacs_port = models.CharField(max_length=255, blank=True, null=True)
    pacs_aet = models.CharField(max_length=255, blank=True, null=True)
    application_aet = models.CharField(max_length=255, blank=True, null=True)
    ctp_dicom_filter = models.TextField(blank=True)
    class TaskType(models.TextChoices):
        IMAGE_DEID = 'IMAGE_DEID', 'Image De-identification'
        IMAGE_QUERY = 'IMAGE_QUERY', 'Image Query'
        HEADER_QUERY = 'HEADER_QUERY', 'Header Query'
        TEXT_DEID = 'TEXT_DEID', 'Text De-identification'
        IMAGE_EXPORT = 'IMAGE_EXPORT', 'Image Export'
        TEXT_EXTRACT = 'TEXT_EXTRACT', 'Text Extraction'
    task_type = models.CharField(max_length=20, choices=TaskType.choices)
    class TaskStatus(models.TextChoices):
        PENDING = 'PENDING', 'Pending'
        RUNNING = 'RUNNING', 'Running'
        COMPLETED = 'COMPLETED', 'Completed'
        FAILED = 'FAILED', 'Failed'

    status = models.CharField(
        max_length=20,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    scheduled_time = models.DateTimeField(null=True, blank=True)
    parameters = models.JSONField()
    
    def __str__(self):
        return self.name

    class Meta:
        db_table = 'deid_tasks'
