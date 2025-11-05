import json
import os
import shutil
import subprocess
import sys
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
    generate_lookup_contents,
    generate_lookup_table,
)
from home.models import Project
from ruamel.yaml import YAML, scalarstring


def run_subprocess_and_capture_log_path(cmd, env, task):
    shell_cmd = ' '.join(f'"{arg}"' if ' ' in arg else arg for arg in cmd)
    print("Copy and run this command to test:")
    print(shell_cmd)
    
    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    log_path_captured = False
    stdout_lines = []
    stderr_lines = []
    
    try:
        first_line = process.stdout.readline()
        if first_line:
            stdout_lines.append(first_line)
            try:
                log_data = json.loads(first_line.strip())
                if 'log_path' in log_data:
                    task.log_path = log_data['log_path']
                    task.save()
                    log_path_captured = True
                    print(f"Captured log path: {task.log_path}")
            except json.JSONDecodeError:
                pass
        
        for line in process.stdout:
            stdout_lines.append(line)
            print(line, end='')
        
        for line in process.stderr:
            stderr_lines.append(line)
            print(line, end='', file=sys.stderr)
        
        process.wait()
        
        stdout_output = ''.join(stdout_lines)
        stderr_output = ''.join(stderr_lines)
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode,
                cmd,
                stdout_output,
                stderr_output
            )
        
        return stdout_output
    
    except Exception as e:
        process.kill()
        raise

PACS_IP = 'host.docker.internal'
PACS_PORT = 4242
PACS_AET = 'ORTHANC'

HOME_DIR = os.path.expanduser('~')
ICORE_BASE_DIR = os.path.join(HOME_DIR, 'Documents', 'iCore')
CONFIG_DIR = os.path.join(ICORE_BASE_DIR, 'config')
CONFIG_PATH = os.path.abspath(os.path.join(CONFIG_DIR, 'config.yml'))
SETTINGS_PATH = os.path.abspath(os.path.join(CONFIG_DIR, 'settings.json'))
RCLONE_CONFIG_PATH = os.path.abspath(os.path.join(CONFIG_DIR, 'rclone.conf'))
MODULES_PATH = os.path.abspath(os.path.join(CONFIG_DIR, 'modules'))
APP_DATA_PATH = os.path.abspath(os.path.join(ICORE_BASE_DIR, 'app_data'))
TMP_INPUT_PATH = os.path.abspath(os.path.join(ICORE_BASE_DIR, 'temp_input'))

IS_DEV = os.environ.get('ICORE_DEV') == '1'

if IS_DEV:
    ICORE_PROCESSOR_PATH = 'python'
    ICORE_CLI_SCRIPT = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'cli.py')
    )
else:
    ICORE_PROCESSOR_PATH = os.path.abspath(
        os.path.join(os.path.dirname(sys.executable), '..', 'icorecli', 'icorecli')
    )
    ICORE_CLI_SCRIPT = None

def process_image_deid(task):
    output_folder = task.output_folder
    build_image_deid_config(task)
    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))
    output_full_path = os.path.abspath(os.path.join(output_folder, f"DeID_{task.name}_{task.timestamp}"))

    if task.image_source == 'PACS':
        os.makedirs(TMP_INPUT_PATH, exist_ok=True)
        temp_input = os.path.join(TMP_INPUT_PATH, 'input.xlsx')
        shutil.copy2(task.parameters['input_file'], temp_input)
        input_folder = TMP_INPUT_PATH
    else:
        input_folder = task.input_folder
    
    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH
    
    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")
    finally:
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_image_deid_config(task):
    """Build the configuration for image deidentification"""
    config = {'module': 'imagedeid'}
    # Add PACS configuration if needed
    if task.image_source == 'PACS':
        config.update({
            'pacs': task.pacs_configs,
            'application_aet': task.application_aet,
        })
        if task.parameters['acc_col'] != '':
            config.update({
                'acc_col': task.parameters['acc_col'],
                'mrn_col': task.parameters['mrn_col']
            })
        elif task.parameters['mrn_col'] != '' and task.parameters['date_col'] != '':
            config.update({
                'mrn_col': task.parameters['mrn_col'],
                'date_col': task.parameters['date_col'],
                'date_window': 0
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
    lookup_contents, anonymizer_lookup_contents = generate_lookup_contents(lookup_file)
    lookup_table = generate_lookup_table(lookup_contents)
    config['ctp_lookup_table'] = scalarstring.LiteralScalarString(lookup_table)

    anonymizer_script = generate_anonymizer_script(
        tags_to_keep,
        tags_to_dateshift,
        tags_to_randomize,
        date_shift_days,
        task.parameters['site_id'],
        anonymizer_lookup_contents
    )
    config['ctp_anonymizer'] = scalarstring.LiteralScalarString(anonymizer_script)
    print(config)
    # Write config to file
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)
    return config

def process_image_query(task):
    print('Processing image query')
    output_folder = task.output_folder
    build_image_query_config(task)

    os.makedirs(TMP_INPUT_PATH, exist_ok=True)
    temp_input = os.path.join(TMP_INPUT_PATH, 'input.xlsx')
    shutil.copy2(task.parameters['input_file'], temp_input)
    input_folder = TMP_INPUT_PATH
    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))
    output_full_path = os.path.abspath(os.path.join(output_folder, f"PHI_{task.name}_{task.timestamp}"))

    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH
    
    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")
    finally:
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_image_query_config(task):
    """Build the configuration for image query"""
    config = {'module': 'imageqr'}
    config.update({
            'pacs': task.pacs_configs,
            'application_aet': task.application_aet,
        })
    if task.parameters['acc_col'] != '':
        config.update({
            'acc_col': task.parameters['acc_col'],
            'mrn_col': task.parameters['mrn_col']
        })
    elif task.parameters['mrn_col'] != '' and task.parameters['date_col'] != '':
        config.update({
            'mrn_col': task.parameters['mrn_col'],
            'date_col': task.parameters['date_col'],
            'date_window': 0
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

    input_folder = os.path.dirname(task.parameters['input_file'])
    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))
    output_full_path = os.path.abspath(os.path.join(output_folder, f"PHI_{task.name}_{task.timestamp}"))
    
    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH
    
    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")


def build_header_query_config(task):
    """Build the configuration for header query"""
    config = {'module': 'headerqr'}
    config.update({
            'pacs': task.pacs_configs,
            'application_aet': task.application_aet,
        })
    if task.parameters['acc_col'] != '':
        config.update({
            'acc_col': task.parameters['acc_col']
        })
    elif task.parameters['mrn_col'] != '' and task.parameters['date_col'] != '':
        config.update({
            'mrn_col': task.parameters['mrn_col'],
            'date_col': task.parameters['date_col'],
            'date_window': 0
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
    
def process_header_extract(task):
    print('Processing header extract')
    input_folder = task.input_folder
    build_header_extract_config(task)
    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))
    output_full_path = os.path.abspath(os.path.join(task.output_folder, f"PHI_{task.name}_{task.timestamp}"))
    
    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH
    
    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")

def build_header_extract_config(task):
    """Build the configuration for header extract"""
    config = {'module': 'headerextract'}
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
    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))
    output_full_path = os.path.abspath(os.path.join(output_folder, f"DeID_{task.name}_{task.timestamp}"))

    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(input_folder), os.path.abspath(output_full_path)]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH

    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")
    finally:
        if os.path.exists(TMP_INPUT_PATH):
            shutil.rmtree(TMP_INPUT_PATH)

def build_text_deid_config(task):
    """Build the configuration for text deidentification"""
    config = {'module': 'textdeid'}
    to_keep_list = task.parameters['text_to_keep'].split('\n') if task.parameters.get('text_to_keep') else []
    to_remove_list = task.parameters['text_to_remove'].split('\n') if task.parameters.get('text_to_remove') else []
    date_shift_by = int(task.parameters['date_shift_days'])
    
    columns_to_deid = task.parameters.get('columns_to_deid', '')
    columns_to_drop = task.parameters.get('columns_to_drop', '')
    
    columns_to_deid_list = [col.strip() for col in columns_to_deid.split('\n') if col.strip()] if columns_to_deid else None
    columns_to_drop_list = [col.strip() for col in columns_to_drop.split('\n') if col.strip()] if columns_to_drop else None
    
    config.update({
        'to_keep_list': to_keep_list,
        'to_remove_list': to_remove_list,
        'date_shift_by': date_shift_by
    })
    
    if columns_to_deid_list:
        config['columns_to_deid'] = columns_to_deid_list
    if columns_to_drop_list:
        config['columns_to_drop'] = columns_to_drop_list
    
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)

    return config

def process_image_export(task):
    print('Processing image export')
    input_folder = task.input_folder
    build_rclone_config(task)
    build_image_export_config(task)

    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))
    output_full_path = os.path.abspath(os.path.join(ICORE_BASE_DIR, 'temp_output'))
    os.makedirs(output_full_path, exist_ok=True)

    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(input_folder), output_full_path]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(input_folder), output_full_path]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH
    
    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")

def build_image_export_config(task):
    """Build the configuration for image export"""
    config = {
        'module': 'imageexport',
        'container_name': task.parameters['container_name'],
        'project_name': task.name,
        'site_id': task.parameters['site_id'],
    }
    with open(CONFIG_PATH, 'w') as f:
        yaml = YAML()
        yaml.dump(config, f)
    return config

def build_rclone_config(task):
    """Build the configuration for rclone"""
    config = f"""
        [azure]
        type = azureblob
        sas_url = {task.parameters['blob_url']}
    """
    with open(RCLONE_CONFIG_PATH, 'w') as f:
        f.write(config)


def process_general_module(task):
    module_name = task.parameters['module_name']
    print(f'Processing {module_name} module')
    build_general_module_config(task)

    output_full_path = os.path.abspath(os.path.join(task.output_folder, f"PHI_{task.name}_{task.timestamp}"))
    app_data_full_path = os.path.abspath(os.path.join(APP_DATA_PATH, f"PHI_{task.name}_{task.timestamp}"))

    print(output_full_path)
    
    if IS_DEV:
        cmd = [ICORE_PROCESSOR_PATH, ICORE_CLI_SCRIPT, CONFIG_PATH, os.path.abspath(task.input_folder), os.path.abspath(output_full_path)]
    else:
        cmd = [ICORE_PROCESSOR_PATH, CONFIG_PATH, os.path.abspath(task.input_folder), os.path.abspath(output_full_path)]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = app_data_full_path
    env['ICORE_MODULES_DIR'] = MODULES_PATH
    
    try:
        output = run_subprocess_and_capture_log_path(cmd, env, task)
        print("Output:", output)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")

def build_general_module_config(task):
    """Build the configuration for general module"""
    config_string = task.parameters['config']
    yaml = YAML()
    config = yaml.load(config_string)
    config['module'] = task.parameters['module_name']
    with open(CONFIG_PATH, 'w') as f:
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
                    elif task.task_type == Project.TaskType.HEADER_EXTRACT:
                        process_header_extract(task)
                    elif task.task_type == Project.TaskType.TEXT_DEID:
                        process_text_deid(task)
                    elif task.task_type == Project.TaskType.IMAGE_EXPORT:
                        process_image_export(task)
                    elif task.task_type == Project.TaskType.GENERAL_MODULE:
                        process_general_module(task)
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
