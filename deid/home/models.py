from django.db import models

class Project(models.Model):
    name = models.CharField(max_length=200)
    image_source = models.CharField(max_length=10, choices=[('LOCAL', 'Local folder'), ('PACS', 'PACS')])
    input_folder = models.CharField(max_length=255)
    output_folder = models.CharField(max_length=255)
    ctp_dicom_filter = models.TextField(blank=True)
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

class Settings(models.Model):
    default_image_source = models.CharField(max_length=10, default='LOCAL')
    default_tags_to_keep = models.TextField(blank=True)
    default_tags_to_dateshift = models.TextField(blank=True)
    default_tags_to_randomize = models.TextField(blank=True)
    default_date_shift_days = models.IntegerField(null=True, blank=True)
    id_generation_method = models.CharField(max_length=10, default='UNIQUE')
    general_filters = models.JSONField(default=list)
    modality_filters = models.JSONField(default=dict)