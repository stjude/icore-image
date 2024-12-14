import os
import time
import subprocess
import shutil
from ruamel.yaml import YAML, scalarstring
from django.db import transaction
from django.core.management.base import BaseCommand
from shutil import which

from grammar import generate_filters_string, generate_anonymizer_script

from home.models import Project

PACS_IP = 'host.docker.internal'
PACS_PORT = 4242
PACS_AET = 'ORTHANC'

CONFIG_PATH = os.path.abspath(os.path.join(os.path.expanduser('~'), '.aiminer', 'config.yml'))

def process_image_deid(task):
    output_folder = task.output_folder
    temp_dir = os.path.join(os.path.expanduser('~'), '.aiminer', 'temp_input')
    build_image_deid_config(task)
    if task.image_source == 'PACS':
        input_folder = os.path.dirname(task.parameters['input_file'])
        pacs_port = task.pacs_port
        if not os.path.basename(task.parameters['input_file']) == 'input.xlsx':
            os.makedirs(temp_dir, exist_ok=True)
            temp_input = os.path.join(temp_dir, 'input.xlsx')
            shutil.copy2(task.parameters['input_file'], temp_input)
            input_folder = temp_dir
        docker_cmd = [
            which('docker') or '/usr/local/bin/docker', 'run', '--rm',
            '-v', f'{CONFIG_PATH}:/config.yml',
            '-v', f'{os.path.abspath(input_folder)}:/input',
            '-v', f'{os.path.abspath(output_folder)}:/output',
            '-p', '50001:50001',
            '-p', f'{pacs_port}:{pacs_port}',
            'aiminer'
        ]
    else:
        input_folder = task.input_folder
        docker_cmd = [
            which('docker') or '/usr/local/bin/docker', 'run', '--rm',
            '-v', f'{CONFIG_PATH}:/config.yml',
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
    finally:
        if os.path.basename(task.parameters['input_file']) != 'input.xlsx' and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def build_image_deid_config(task):
    """Build the configuration for image deidentification"""
    config = {'module': 'imagedeid'}
    
    # Add PACS configuration if needed
    if task.image_source == 'PACS':
        config.update({
            'pacs_ip': task.pacs_ip,
            'pacs_port': task.pacs_port,
            'pacs_aet': task.pacs_aet,
        })
        if task.parameters['acc_col'] != '':
            config.update({
                'acc_col': task.parameters['acc_col']
            })
        elif task.parameters['mrn_col'] != '' and task.parameters['date_col'] != '':
            config.update({
                'mrn_col': task.parameters['mrn_col'],
                'date_col': task.parameters['date_col']
            })
            

    general_filters = task.parameters['general_filters']
    modality_filters = task.parameters['modality_filters']
    expression_string = generate_filters_string(general_filters, modality_filters)
    if expression_string != '': 
        config['ctp_filters'] = scalarstring.LiteralScalarString(expression_string)

    tags_to_keep = task.parameters['tags_to_keep']
    tags_to_dateshift = task.parameters['tags_to_dateshift']
    tags_to_randomize = task.parameters['tags_to_randomize']
    date_shift_days = task.parameters['date_shift_days']

    anonymizer_script = generate_anonymizer_script(tags_to_keep, tags_to_dateshift, tags_to_randomize, date_shift_days)
    config['ctp_anonymizer'] = scalarstring.LiteralScalarString(anonymizer_script)

    # Write config to file
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)
    
    return config

def process_image_query(task):
    print('Processing image query')
    output_folder = task.output_folder
    build_image_query_config(task)

    pacs_port = task.pacs_port
    input_folder = os.path.dirname(task.parameters['input_file'])
    temp_dir = os.path.join(os.path.expanduser('~'), '.aiminer', 'temp_input')
    if not os.path.basename(task.parameters['input_file']) == 'input.xlsx':
        os.makedirs(temp_dir, exist_ok=True)
        temp_input = os.path.join(temp_dir, 'input.xlsx')
        shutil.copy2(task.parameters['input_file'], temp_input)
        input_folder = temp_dir

    docker_cmd = [
        which('docker') or '/usr/local/bin/docker', 'run', '--rm',
        '-v', f'{CONFIG_PATH}:/config.yml',
        '-v', f'{os.path.abspath(input_folder)}:/input',
        '-v', f'{os.path.abspath(output_folder)}:/output',
        '-p', '50001:50001',
        '-p', f'{pacs_port}:{pacs_port}',
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
    finally:
        if os.path.basename(task.parameters['input_file']) != 'input.xlsx' and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def build_image_query_config(task):
    """Build the configuration for image query"""
    config = {'module': 'imageqr'}
    config.update({
            'pacs_ip': task.pacs_ip,
            'pacs_port': task.pacs_port,
            'pacs_aet': task.pacs_aet,
        })
    if task.parameters['acc_col'] != '':
        config.update({
            'acc_col': task.parameters['acc_col']
        })
    elif task.parameters['mrn_col'] != '' and task.parameters['date_col'] != '':
        config.update({
            'mrn_col': task.parameters['mrn_col'],
            'date_col': task.parameters['date_col']
        })
    general_filters = task.parameters['general_filters']
    modality_filters = task.parameters['modality_filters']
    expression_string = generate_filters_string(general_filters, modality_filters)
    if expression_string != '': 
        config['ctp_filters'] = scalarstring.LiteralScalarString(expression_string)
    
    # Write config to file
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)
    
    return config

def process_header_query(task):
    print('Processing header query')
    output_folder = task.output_folder
    build_header_query_config(task)

    pacs_port = task.pacs_port
    input_folder = os.path.dirname(task.parameters['input_file'])
    docker_cmd = [
        which('docker') or '/usr/local/bin/docker', 'run', '--rm',
        '-v', f'{CONFIG_PATH}:/config.yml',
        '-v', f'{os.path.abspath(input_folder)}:/input',
        '-v', f'{os.path.abspath(output_folder)}:/output',
        '-p', '50001:50001',
        '-p', f'{pacs_port}:{pacs_port}',
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


def build_header_query_config(task):
    """Build the configuration for header query"""
    config = {'module': 'headerqr'}
    config.update({
            'pacs_ip': task.pacs_ip,
            'pacs_port': task.pacs_port,
            'pacs_aet': task.pacs_aet,
        })
    if task.parameters['acc_col'] != '':
        config.update({
            'acc_col': task.parameters['acc_col']
        })
    elif task.parameters['mrn_col'] != '' and task.parameters['date_col'] != '':
        config.update({
            'mrn_col': task.parameters['mrn_col'],
            'date_col': task.parameters['date_col']
        })
    general_filters = task.parameters['general_filters']
    modality_filters = task.parameters['modality_filters']
    expression_string = generate_filters_string(general_filters, modality_filters)
    if expression_string != '': 
        config['ctp_filters'] = scalarstring.LiteralScalarString(expression_string)
    
    # Write config to file
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)
    
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
                    try:
                        if task.task_type == Project.TaskType.IMAGE_DEID:
                            process_image_deid(task)
                        elif task.task_type == Project.TaskType.IMAGE_QUERY:
                            process_image_query(task)
                        elif task.task_type == Project.TaskType.HEADER_QUERY:
                            process_header_query(task)
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


class Command(BaseCommand):
    def handle(self, *args, **options):
        run_worker()