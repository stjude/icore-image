import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from utils import configure_run_logging, setup_run_directories


def _get_rclone_binary():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        rclone_binary = os.path.join(bundle_dir, '_internal', 'rclone', 'rclone')
    else:
        rclone_binary = os.path.join(os.path.dirname(__file__), 'rclone', 'rclone')
    
    return rclone_binary


def _parse_sas_url(sas_url):
    parsed = urlparse(sas_url)

    is_azurite = parsed.netloc.startswith('127.0.0.1') or parsed.netloc.startswith('localhost')
    
    if is_azurite:
        path_parts = [p for p in parsed.path.split('/') if p]
        if len(path_parts) >= 2:
            account_name = path_parts[0]
            container_name = path_parts[1]
        else:
            account_name = "devstoreaccount1"
            container_name = path_parts[0] if path_parts else ""
    else:
        hostname = parsed.netloc
        account_name = hostname.split('.')[0]

        container_name = parsed.path.lstrip('/')

    sas_token = parsed.query

    return account_name, container_name, sas_token


def _create_rclone_config(sas_url, config_path):
    parsed = urlparse(sas_url)

    is_azurite = parsed.netloc.startswith('127.0.0.1') or parsed.netloc.startswith('localhost')
    
    if is_azurite:
        path_parts = [p for p in parsed.path.split('/') if p]
        account_name = path_parts[0] if path_parts else "devstoreaccount1"
        endpoint = f"{parsed.scheme}://{parsed.netloc}/{account_name}"
        
        config_content = f"""[azure]
            type = azureblob
            account = {account_name}
            use_emulator = true
            endpoint = {endpoint}
            sas_url = {sas_url}
        """
    else:
        account_name = parsed.netloc.split('.')[0]
        config_content = f"""[azure]
            type = azureblob
            account = {account_name}
            sas_url = {sas_url}
        """
    
    with open(config_path, 'w') as f:
        f.write(config_content)


def image_export(input_dir, sas_url, project_name, appdata_dir=None, debug=False, run_dirs=None):
    """
    Export files from input directory to Azure Blob Storage using rclone.
    
    All files are uploaded to the container with the path structure:
    {container}/{project_name}/{original_relative_path}
    
    Args:
        input_dir: Input directory containing files to export
        sas_url: SAS URL for the Azure container (includes container name)
        project_name: Project name (used as folder prefix in blob storage)
        appdata_dir: Application data directory for logs
        debug: Enable debug logging
        run_dirs: Run directories dictionary (optional)
        
    Returns:
        dict: Export statistics
    """
    if run_dirs is None:
        run_dirs = setup_run_directories()

    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    logging.info("="*80)
    logging.info("IMAGE EXPORT MODULE (rclone)")
    logging.info("="*80)
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Project name: {project_name}")
    logging.info(f"SAS URL: {sas_url.split('?')[0]}...")
    logging.info("="*80)
    
    if not os.path.exists(input_dir):
        raise Exception(f"Input directory does not exist: {input_dir}")

    if not os.listdir(input_dir):
        raise Exception(f"Input directory is empty: {input_dir}")
    
    try:
        _, container_name, _ = _parse_sas_url(sas_url)
        
        rclone_config_path = os.path.join(tempfile.gettempdir(), f"rclone_{os.getpid()}.conf")
        _create_rclone_config(sas_url, rclone_config_path)
        
        try:
            destination = f"azure:{container_name}/{project_name}"
            
            rclone_binary = _get_rclone_binary()
            cmd = [
                rclone_binary,
                "copy",
                "--progress",
                "--config", rclone_config_path,
                input_dir,
                destination
            ]
            
            logging.info(f"Running rclone command: {' '.join(cmd[:4])} ... {destination}")
            
            
            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True
                )
                logging.info("PROGRESS: COMPLETE")
                
                return {
                    "status": "completed"
                }
            except subprocess.CalledProcessError as e:
                raise Exception(f"rclone error: Command failed with exit code {e.returncode}: {e.stderr}")
        finally:
            if os.path.exists(rclone_config_path):
                os.remove(rclone_config_path)
        
    except subprocess.CalledProcessError as e:
        error_parts = []
        
        if e.stderr:
            error_parts.append(f"rclone stderr: {e.stderr}")
            logging.error(f"rclone stderr: {e.stderr}")
        if e.stdout:
            error_parts.append(f"rclone stdout: {e.stdout}")
            logging.error(f"rclone stdout: {e.stdout}")
        
        error_details = '\n'.join(error_parts) if error_parts else str(e)
        error_msg = f"rclone error: Command failed with exit code {e.returncode}"
        if error_details:
            error_msg += f"\n{error_details}"
        
        logging.error(error_msg)

        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error during export: {str(e)}"
        logging.error(error_msg)
        raise
