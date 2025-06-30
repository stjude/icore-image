from django.db import models


class Project(models.Model):
    name = models.CharField(max_length=200)
    timestamp = models.CharField(max_length=20, blank=True, null=True)
    log_path = models.CharField(max_length=200, blank=True, null=True)
    image_source = models.CharField(max_length=10, choices=[('LOCAL', 'Local folder'), ('PACS', 'PACS')])
    input_folder = models.CharField(max_length=255)
    output_folder = models.CharField(max_length=255)
    pacs_configs = models.JSONField(default=list)
    application_aet = models.CharField(max_length=255, blank=True, null=True)
    ctp_dicom_filter = models.TextField(blank=True)
    class TaskType(models.TextChoices):
        IMAGE_DEID = 'IMAGE_DEID', 'Image De-identification'
        IMAGE_QUERY = 'IMAGE_QUERY', 'Image Query'
        HEADER_QUERY = 'HEADER_QUERY', 'Header Query'
        IMAGE_EXPORT = 'IMAGE_EXPORT', 'Image Export'
        GENERAL_MODULE = 'GENERAL_MODULE', 'General Module'
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

class Module(models.Model):
    name = models.CharField(max_length=255)
    file_path = models.CharField(max_length=512)
    uploaded_at = models.DateTimeField(auto_now_add=True)
    is_active = models.BooleanField(default=True)
    version = models.CharField(max_length=50, null=True, blank=True)

    def __str__(self):
        return f"{self.name} (v{self.version})"

    class Meta:
        ordering = ['-uploaded_at']
