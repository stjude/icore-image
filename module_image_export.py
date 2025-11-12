import logging
import os
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from utils import configure_run_logging, setup_run_directories


def _parse_sas_url(sas_url):
    """
    Parse SAS URL to extract account, container, and SAS token.
    
    Supports both formats:
    - Real Azure: http://{account}.blob.core.windows.net/{container}?{sas_token}
    - Azurite: http://127.0.0.1:port/{account}/{container}?{sas_token}
    
    Args:
        sas_url: Full SAS URL including container and token
        
    Returns:
        tuple: (account_name, container_name, sas_token)
    """
    parsed = urlparse(sas_url)
    
    # Check if we're using Azurite (localhost/127.0.0.1)
    is_azurite = parsed.netloc.startswith('127.0.0.1') or parsed.netloc.startswith('localhost')
    
    if is_azurite:
        # For Azurite, path format is: /{account_name}/{container_name}
        path_parts = [p for p in parsed.path.split('/') if p]
        if len(path_parts) >= 2:
            account_name = path_parts[0]
            container_name = path_parts[1]
        else:
            # Fallback
            account_name = "devstoreaccount1"
            container_name = path_parts[0] if path_parts else ""
    else:
        # For real Azure, account name is in hostname: {account}.blob.core.windows.net
        hostname = parsed.netloc
        account_name = hostname.split('.')[0]
        
        # Container name is the path (remove leading /)
        container_name = parsed.path.lstrip('/')
    
    # Extract SAS token from query string
    sas_token = parsed.query
    
    return account_name, container_name, sas_token


def _create_rclone_config(sas_url, config_path, azurite_endpoint=None):
    """
    Create rclone config file for Azure Blob Storage with SAS URL.
    Supports both real Azure and Azurite emulator.
    
    Args:
        sas_url: Full SAS URL for the container
        config_path: Path where to write the rclone config file
        azurite_endpoint: Optional Azurite endpoint (e.g., "http://127.0.0.1:10000")
                         Only needed for Azurite to specify the emulator endpoint
    """
    parsed = urlparse(sas_url)
    
    # Check if we're using Azurite (localhost/127.0.0.1)
    is_azurite = parsed.netloc.startswith('127.0.0.1') or parsed.netloc.startswith('localhost')
    
    if is_azurite:
        # For Azurite, account name is in the path: /devstoreaccount1/container
        # Path format: /{account_name}/{container_name}
        path_parts = [p for p in parsed.path.split('/') if p]
        account_name = path_parts[0] if path_parts else "devstoreaccount1"
        
        if azurite_endpoint:
            # For Azurite, endpoint should include the account name
            # Format: http://127.0.0.1:port/{account_name}
            endpoint = f"{azurite_endpoint}/{account_name}"
        else:
            # Fallback: construct endpoint from URL
            endpoint = f"{parsed.scheme}://{parsed.netloc}/{account_name}"
        
        # Use SAS URL for Azurite too - rclone supports this with use_emulator
        config_content = f"""[azure]
type = azureblob
account = {account_name}
use_emulator = true
endpoint = {endpoint}
sas_url = {sas_url}
"""
    else:
        # For real Azure, account name is in hostname: {account}.blob.core.windows.net
        account_name = parsed.netloc.split('.')[0]
        config_content = f"""[azure]
type = azureblob
account = {account_name}
sas_url = {sas_url}
"""
    
    with open(config_path, 'w') as f:
        f.write(config_content)


def image_export(input_dir, sas_url, project_name, appdata_dir=None, debug=False, run_dirs=None, azurite_endpoint=None):
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
    # Set up run directories if not provided (same pattern as other modules)
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    # Set up logging using the same pattern as other modules
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    logging.info("="*80)
    logging.info("IMAGE EXPORT MODULE (rclone)")
    logging.info("="*80)
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Project name: {project_name}")
    logging.info(f"SAS URL: {sas_url.split('?')[0]}...")  # Log URL without token
    logging.info("="*80)
    
    # Verify input directory exists
    if not os.path.exists(input_dir):
        raise Exception(f"Input directory does not exist: {input_dir}")
    
    # Verify input directory is not empty
    if not os.listdir(input_dir):
        logging.warning("Input directory is empty - no files to export")
        return {"status": "completed", "files_uploaded": 0}
    
    try:
        # Parse SAS URL to get container name
        _, container_name, _ = _parse_sas_url(sas_url)
        
        # Create temporary rclone config file
        rclone_config_path = os.path.join(tempfile.gettempdir(), f"rclone_{os.getpid()}.conf")
        _create_rclone_config(sas_url, rclone_config_path, azurite_endpoint)
        
        try:
            # Build rclone command
            # Destination: azure:{container}/{project_name}/
            # rclone will preserve the folder structure from input_dir
            destination = f"azure:{container_name}/{project_name}"
            
            cmd = [
                "rclone",
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
        # Collect error details from stderr, stdout, and log file
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
