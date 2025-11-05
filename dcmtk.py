import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

from tenacity import retry, stop_after_attempt, wait_chain, wait_fixed, retry_if_exception_type, retry_if_result, RetryCallState, before_sleep_log


class DCMTKError(Exception):
    pass


class DCMTKCommandError(DCMTKError):
    pass


class DCMTKParseError(DCMTKError):
    pass


def _get_default_dcmtk_home():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        dcmtk_home = os.path.join(bundle_dir, '_internal', 'dcmtk')
    else:
        dcmtk_home = os.path.join(os.path.dirname(__file__), 'dcmtk')
    return dcmtk_home


def _build_dcmtk_env():
    env = os.environ.copy()
    dcmtk_home = _get_default_dcmtk_home()
    env['DCMDICTPATH'] = os.path.join(dcmtk_home, 'share', 'dcmtk-3.6.9', 'dicom.dic')
    return env


def _parse_find_xml(xml_content):
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise DCMTKParseError(f"Failed to parse XML response: {e}")
    
    results = []
    
    for dataset in root.findall('data-set'):
        study_data = {}
        for element in dataset.findall('element'):
            name = element.get('name')
            value = element.text
            if name and value:
                study_data[name] = value
        
        if study_data:
            results.append(study_data)
    
    return results


def _parse_move_output(stderr, returncode):
    result = {
        "success": False,
        "num_completed": 0,
        "num_failed": 0,
        "num_warning": 0,
        "message": ""
    }
    
    if "Received Final Move Response (Success)" in stderr:
        result["success"] = True
        
        completed_match = re.search(r'Sub-Operations Complete:\s*(\d+)', stderr)
        if completed_match:
            result["num_completed"] = int(completed_match.group(1))
        
        failed_match = re.search(r'Complete:\s*\d+,\s*Failed:\s*(\d+)', stderr)
        if failed_match:
            result["num_failed"] = int(failed_match.group(1))
        
        warning_match = re.search(r'Failed:\s*\d+,\s*Warning:\s*(\d+)', stderr)
        if warning_match:
            result["num_warning"] = int(warning_match.group(1))
        
        result["message"] = "Move completed successfully"
    else:
        if "Failed: UnableToProcess" in stderr:
            result["message"] = "Move failed: UnableToProcess"
        elif "Failed" in stderr:
            result["message"] = "Move failed"
        else:
            result["message"] = f"Move failed with exit code {returncode}"
    
    return result


def _log_find_retry(retry_state: RetryCallState):
    logging.info("Query failed. Retrying")


@retry(
    stop=stop_after_attempt(4),
    wait=wait_chain(wait_fixed(4), wait_fixed(16), wait_fixed(32)),
    retry=(retry_if_exception_type(DCMTKCommandError) | retry_if_exception_type(DCMTKParseError)),
    before_sleep=_log_find_retry,
    reraise=True
)
def find_studies(host, port, calling_aet, called_aet, query_params, query_level="STUDY", return_tags=None):
    """
    Query PACS for studies using findscu.
    
    Args:
        host: PACS hostname or IP address
        port: PACS DICOM port
        calling_aet: AE title of the calling application
        called_aet: AE title of the PACS
        query_params: Dict of DICOM tags to query (e.g. {"AccessionNumber": "12345"})
        query_level: Query/retrieve level (default: "STUDY")
        return_tags: Optional list of DICOM tags to retrieve in results
        
    Returns:
        List of dicts, one per matching study, with DICOM tag names as keys
        
    Raises:
        DCMTKCommandError: If findscu command fails
        DCMTKParseError: If XML response cannot be parsed
    """
    dcmtk_home = _get_default_dcmtk_home()
    findscu_binary = os.path.join(dcmtk_home, 'bin', 'findscu')
    
    temp_dir = tempfile.mkdtemp()
    xml_path = os.path.join(temp_dir, 'output.xml')
    
    try:
        cmd = [
            findscu_binary,
            "-od", temp_dir,
            "-Xs", xml_path,
            "-aet", calling_aet,
            "-aec", called_aet,
            "-S",
            "-k", f"QueryRetrieveLevel={query_level}"
        ]
        
        for tag, value in query_params.items():
            cmd.extend(["-k", f"{tag}={value}"])
        
        if return_tags:
            for tag in return_tags:
                cmd.extend(["-k", tag])
        else:
            cmd.extend(["-k", "StudyInstanceUID"])
        
        cmd.extend([host, str(port)])
        
        logging.debug(f"Running findscu: {' '.join(cmd)}")
        
        env = _build_dcmtk_env()
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        
        if result.returncode != 0:
            raise DCMTKCommandError(
                f"findscu command failed with exit code {result.returncode}: {result.stderr}"
            )
        
        try:
            with open(xml_path, 'r') as f:
                xml_content = f.read()
        except FileNotFoundError:
            raise DCMTKCommandError("findscu did not produce XML output file")
        
        return _parse_find_xml(xml_content)
    
    finally:
        try:
            shutil.rmtree(temp_dir)
        except:
            pass


def _return_last_result(retry_state: RetryCallState):
    return retry_state.outcome.result()

def _log_move_retry(retry_state: RetryCallState):
    logging.info("Move failed. Retrying")


@retry(
    stop=stop_after_attempt(4),
    wait=wait_chain(wait_fixed(4), wait_fixed(16), wait_fixed(32)),
    retry=retry_if_result(lambda result: not result["success"]),
    before_sleep=_log_move_retry,
    retry_error_callback=_return_last_result
)
def move_study(host, port, calling_aet, called_aet, move_destination, study_uid, query_level="STUDY"):
    """
    Move a study from PACS using movescu.
    
    Args:
        host: PACS hostname or IP address
        port: PACS DICOM port
        calling_aet: AE title of the calling application
        called_aet: AE title of the PACS
        move_destination: AE title of the move destination
        study_uid: StudyInstanceUID to move
        query_level: Query/retrieve level (default: "STUDY")
        
    Returns:
        Dict with keys: success (bool), num_completed (int), num_failed (int),
        num_warning (int), message (str)
    """
    dcmtk_home = _get_default_dcmtk_home()
    movescu_binary = os.path.join(dcmtk_home, 'bin', 'movescu')
    
    cmd = [
        movescu_binary,
        "-v",
        "-aet", calling_aet,
        "-aem", move_destination,
        "-aec", called_aet,
        "-k", f"QueryRetrieveLevel={query_level}",
        "-k", f"StudyInstanceUID={study_uid}",
        host,
        str(port)
    ]
    
    logging.debug(f"Running movescu: {' '.join(cmd)}")
    
    env = _build_dcmtk_env()
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    
    return _parse_move_output(result.stderr, result.returncode)

