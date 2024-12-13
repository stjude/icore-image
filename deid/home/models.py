from django.db import models

class Project(models.Model):
    name = models.CharField(max_length=200)
    image_source = models.CharField(max_length=10, choices=[('LOCAL', 'Local folder'), ('PACS', 'PACS')])
    input_folder = models.CharField(max_length=255)
    output_folder = models.CharField(max_length=255)
    ctp_dicom_filter = models.TextField(blank=True)
    class TaskType(models.TextChoices):
        IMAGE_DEID = 'image_deid', 'Image De-identification'
        IMAGE_QUERY = 'image_query', 'Image Query'
        HEADER_QUERY = 'header_query', 'DICOM Header Query/Retrieve'
    task_type = models.CharField(max_length=20, choices=TaskType.choices)
    class TaskStatus(models.TextChoices):
        PENDING = 'pending', 'Pending'
        RUNNING = 'running', 'Running'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'

    status = models.CharField(
        max_length=20,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    parameters = models.JSONField()
    
    def __str__(self):
        return self.name

    class Meta:
        db_table = 'deid_tasks'
