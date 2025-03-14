import json
import os
import shutil
import subprocess
import time
import traceback
from datetime import datetime
from shutil import which

import pytz
from django.core.management.base import BaseCommand
from django.db import models, transaction
from grammar import (
    generate_anonymizer_script,
    generate_filters_string,
    generate_lookup_table,
)
from home.models import Project
from ruamel.yaml import YAML, scalarstring

PACS_IP = 'host.docker.internal'
PACS_PORT = 4242
PACS_AET = 'ORTHANC'

HOME_DIR = os.path.expanduser('~')
CONFIG_PATH = os.path.abspath(os.path.join(HOME_DIR, '.aiminer', 'config.yml'))
SETTINGS_PATH = os.path.abspath(os.path.join(HOME_DIR, '.aiminer', 'settings.json'))
RCLONE_CONFIG_PATH = os.path.abspath(os.path.join(HOME_DIR, '.aiminer', 'rclone.conf'))
TMP_INPUT_PATH = os.path.abspath(os.path.join(HOME_DIR, '.aiminer', 'temp_input'))
DOCKER = which('docker') or '/usr/local/bin/docker'

def process_image_deid(task):
    output_folder = task.output_folder
    build_image_deid_config(task)
    if task.image_source == 'PACS':
        pacs_port = task.pacs_port

        os.makedirs(TMP_INPUT_PATH, exist_ok=True)
        temp_input = os.path.join(TMP_INPUT_PATH, 'input.xlsx')
        shutil.copy2(task.parameters['input_file'], temp_input)
        input_folder = TMP_INPUT_PATH
        docker_cmd = [
            DOCKER, 'run', '--rm',
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
            DOCKER, 'run', '--rm',
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
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_image_deid_config(task):
    """Build the configuration for image deidentification"""
    config = {'module': 'imagedeid'}
    # Add PACS configuration if needed
    if task.image_source == 'PACS':
        config.update({
            'pacs_ip': task.pacs_ip,
            'pacs_port': task.pacs_port,
            'pacs_aet': task.pacs_aet,
            'application_aet': task.application_aet,
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

    lookup_file = task.parameters['lookup_file'] if task.parameters['use_lookup_table'] else None
    lookup_table = generate_lookup_table(lookup_file)
    config['ctp_lookup_table'] = scalarstring.LiteralScalarString(lookup_table)

    anonymizer_script = generate_anonymizer_script(tags_to_keep, tags_to_dateshift, tags_to_randomize, date_shift_days, lookup_file)
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

    os.makedirs(TMP_INPUT_PATH, exist_ok=True)
    temp_input = os.path.join(TMP_INPUT_PATH, 'input.xlsx')
    shutil.copy2(task.parameters['input_file'], temp_input)
    input_folder = TMP_INPUT_PATH

    docker_cmd = [
        DOCKER, 'run', '--rm',
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
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_image_query_config(task):
    """Build the configuration for image query"""
    config = {'module': 'imageqr'}
    config.update({
            'pacs_ip': task.pacs_ip,
            'pacs_port': task.pacs_port,
            'pacs_aet': task.pacs_aet,
            'application_aet': task.application_aet,
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
        DOCKER, 'run', '--rm',
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
            'application_aet': task.application_aet,
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

def process_text_deid(task):
    print('Processing text deid')
    build_text_deid_config(task)

    output_folder = task.output_folder

    os.makedirs(TMP_INPUT_PATH, exist_ok=True)
    temp_input = os.path.join(TMP_INPUT_PATH, 'input.xlsx')
    shutil.copy2(task.parameters['input_file'], temp_input)
    input_folder = TMP_INPUT_PATH

    docker_cmd = [
        DOCKER, 'run', '--rm',
        '-v', f'{CONFIG_PATH}:/config.yml', 
        '-v', f'{os.path.abspath(input_folder)}:/input',
        '-v', f'{os.path.abspath(output_folder)}:/output',
        'aiminer'
    ]

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
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_text_deid_config(task):
    """Build the configuration for text deidentification"""
    config = {'module': 'textdeid'}
    print(task.parameters)
    to_keep_list = task.parameters['text_to_keep'].split('\n')
    to_remove_list = task.parameters['text_to_remove'].split('\n')
    date_shift_by = int(task.parameters['date_shift_days'])
    config.update({
        'to_keep_list': to_keep_list,
        'to_remove_list': to_remove_list,
        'date_shift_by': date_shift_by
    })
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)

    return config

def process_image_export(task):
    print('Processing image export')
    input_folder = task.input_folder
    output_folder = task.output_folder
    build_image_export_config(task)
    print(RCLONE_CONFIG_PATH)

    docker_cmd = [
        DOCKER, 'run', '--rm',
        '-v', f'{CONFIG_PATH}:/config.yml',
        '-v', f'{RCLONE_CONFIG_PATH}:/rclone.conf',
        '-v', f'{os.path.abspath(input_folder)}:/input',
        '-v', f'{os.path.abspath(output_folder)}:/output',
        'aiminer'
    ]
    shell_cmd = ' '.join(f'"{arg}"' if ' ' in arg else arg for arg in docker_cmd)
    print("Copy and run this command to test:")
    print(shell_cmd)
    try:
        result = subprocess.run(docker_cmd, check=True, capture_output=True, text=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Docker container failed with exit code {e.returncode}: {e.stderr}")

def build_image_export_config(task):
    """Build the configuration for image export"""
    config = {
        'module': 'imageexport',
        'rclone_config': RCLONE_CONFIG_PATH,
        'storage_location': task.parameters['storage_location'],
        'project_name': task.name
    }
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)
    return config

def process_text_extract(task):
    print('Processing text extract')
    build_text_extract_config()
    shutil.copytree(task.input_folder, TMP_INPUT_PATH, dirs_exist_ok=True)

    docker_cmd = [
        DOCKER, 'run', '--rm',
        '-v', f'{CONFIG_PATH}:/config.yml', 
        '-v', f'{os.path.abspath(TMP_INPUT_PATH)}:/input',
        '-v', f'{os.path.abspath(task.output_folder)}:/output',
        'aiminer'
    ]

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
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_text_extract_config():
    """Build the configuration for text extraction"""
    config = {'module': 'textextract'}
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)

    return config

def run_worker():
    settings = json.load(open(SETTINGS_PATH))
    timezone = settings.get('timezone', 'UTC')
    timezone = pytz.timezone(timezone)
    while True:
        try:
            task = None
            with transaction.atomic():
                
                now = datetime.now(timezone)
                task = (Project.objects
                       .select_for_update(skip_locked=True)
                       .filter(status=Project.TaskStatus.PENDING)
                       .filter(
                           models.Q(scheduled_time__isnull=True) |
                           models.Q(scheduled_time__lte=now)
                       )
                       .first())
            if task:
                task.status = Project.TaskStatus.RUNNING
                task.save()
                try:
                    if task.task_type == Project.TaskType.IMAGE_DEID:
                        process_image_deid(task)
                    elif task.task_type == Project.TaskType.IMAGE_QUERY:
                        process_image_query(task)
                    elif task.task_type == Project.TaskType.HEADER_QUERY:
                        process_header_query(task)
                    elif task.task_type == Project.TaskType.TEXT_DEID:
                        process_text_deid(task)
                    elif task.task_type == Project.TaskType.IMAGE_EXPORT:
                        process_image_export(task)
                    elif task.task_type == Project.TaskType.TEXT_EXTRACT:
                        process_text_extract(task)
                    task.status = Project.TaskStatus.COMPLETED
                except Exception as e:
                    traceback.print_exc()
                    print(f"Error processing task {task.id}: {str(e)}")
                    task.status = Project.TaskStatus.FAILED
                finally:
                    task.save()
                    print(f"Task {task.id} finished with status: {task.status}")
            
            time.sleep(1)
            
        except Exception as e:
            print(f"Worker error: {str(e)}")
            time.sleep(5)


class Command(BaseCommand):
    def handle(self, *args, **options):
        run_worker()
