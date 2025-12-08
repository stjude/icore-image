from django.apps import AppConfig


class HomeConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'home'

    def ready(self):
        from django.db import OperationalError, ProgrammingError
        try:
            from .models import Project
            stale_count = Project.objects.filter(
                status__in=[Project.TaskStatus.PENDING, Project.TaskStatus.RUNNING]
            ).update(status=Project.TaskStatus.FAILED, process_pid=None)
            if stale_count > 0:
                print(f"Marked {stale_count} stale task(s) as FAILED on startup")
        except (OperationalError, ProgrammingError):
            pass

