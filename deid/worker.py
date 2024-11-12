import django
import os
import time
import yaml
import subprocess
from lark import Lark
from django.db import transaction

from grammar import preprocess_input


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from home.models import Project

def process_image_deid(task):
    print("task: ", task)
    print('input_folder: ', task.input_folder)
    print('output_folder: ', task.output_folder)
    input_folder = task.input_folder
    output_folder = task.output_folder
    build_image_deid_config(task)

    docker_cmd = [
        'docker', 'run', '--rm',
        '-v', f'{os.path.abspath("config.yml")}:/config.yml',
        '-v', f'{os.path.abspath(input_folder)}:/input',
        '-v', f'{os.path.abspath(output_folder)}:/output',
        'aiminer'
    ]
    
    # Print a shell-ready version of the command
    shell_cmd = ' '.join(f'"{arg}"' if ' ' in arg else arg for arg in docker_cmd)
    print("Copy and run this command to test:")
    print(shell_cmd)
    
    try:
        result = subprocess.run(docker_cmd, check=True, capture_output=True, text=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Docker container failed with exit code {e.returncode}: {e.stderr}")

def build_image_deid_config(task):
    """Build the configuration for image deidentification"""
    config = {'module': 'imagedeid'}
    
    # Add PACS configuration if needed
    if task.image_source == 'PACS':
        config.update({
            'pacs_ip': task.parameters['pacs_ip'],
            'pacs_port': task.parameters['pacs_port'],
            'pacs_aet': task.parameters['pacs_aet'],
        })

    general_filters = task.parameters['general_filters']
    modality_filters = task.parameters['modality_filters']
    # TODO: fix expression_string generation and uncomment. Hardcoding ct filters for now
    # expression_string = preprocess_input(general_filters, modality_filters)
    # config['ctp_filters'] = expression_string
    config['ctp_filters'] = '!ImageType.contains("INVALID") + !InstanceNumber.equals("1")'

    # Write config to file
    with open('config.yml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    return config

def run_worker():
    while True:
        try:
            # Use select_for_update() to prevent race conditions
            with transaction.atomic():
                task = (Project.objects
                       .select_for_update(skip_locked=True)
                       .filter(status=Project.TaskStatus.PENDING)
                       .first())
                
                if task:
                    # Mark as running
                    task.status = Project.TaskStatus.RUNNING
                    task.save()
                    print("parameters: ", task.parameters)
                    print("id: ", task.id)
                    print("image_source: ", task.image_source)
                    print("input_folder: ", task.input_folder)
                    print("output_folder: ", task.output_folder)
                    
                    try:
                        # Process the task
                        process_image_deid(task)
                        task.status = Project.TaskStatus.COMPLETED
                    except Exception as e:
                        print(f"Error processing task {task.id}: {str(e)}")
                        task.status = Project.TaskStatus.FAILED
                    finally:
                        task.save()
            
            # Wait before next poll
            time.sleep(1)
            
        except Exception as e:
            print(f"Worker error: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    run_worker()