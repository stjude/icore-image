import django
import os
import time
from django.db import transaction

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from home.models import DeidentificationTask

def process_task(task):
    """Process a single task"""
    print(f"Processing task {task.id}")
    print("Parameters:")
    print(f"Study Name: {task.parameters['study_name']}")
    print(f"Image Source: {task.parameters['image_source']}")
    
    if task.parameters['image_source'] == 'LOCAL':
        print(f"Input Folder: {task.parameters['input_folder']}")
        print(f"Output Folder: {task.parameters['output_folder']}")
    else:  # PACS
        print(f"Input File: {task.parameters['input_file']}")
        print(f"Column Header: {task.parameters['column_header']}")
    
    print(f"CTP DICOM Filter: {task.parameters['ctp_dicom_filter']}")

def run_worker():
    while True:
        try:
            # Use select_for_update() to prevent race conditions
            with transaction.atomic():
                task = (DeidentificationTask.objects
                       .select_for_update(skip_locked=True)
                       .filter(status=DeidentificationTask.TaskStatus.PENDING)
                       .first())
                
                if task:
                    # Mark as running
                    task.status = DeidentificationTask.TaskStatus.RUNNING
                    task.save()
                    
                    try:
                        # Process the task
                        process_task(task)
                        task.status = DeidentificationTask.TaskStatus.COMPLETED
                    except Exception as e:
                        print(f"Error processing task {task.id}: {str(e)}")
                        task.status = DeidentificationTask.TaskStatus.FAILED
                    finally:
                        task.save()
            
            # Wait before next poll
            time.sleep(1)
            
        except Exception as e:
            print(f"Worker error: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    run_worker()