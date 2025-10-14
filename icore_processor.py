import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import pydicom
import string
import tempfile
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Thread

import pandas as pd
import requests
import yaml
from lark import Lark

INPUT_DIR = "input"
OUTPUT_DIR = "output"
APPDATA_DIR = "appdata"
MODULES_DIR = "modules"
CONFIG_PATH = "config.yml"


IMAGEQR_CONFIG = """<Configuration>
    <Server
        maxThreads="20"
        port="50000">
        <Log/>
    </Server>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="AuditLog"
        name="AuditLog"
        root="{appdata_dir}/temp/roots/AuditLog"/>
    <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="50001"
            calledAETTag="{application_aet}"
            root="{appdata_dir}/temp/roots/DicomImportService"
            quarantine="{appdata_dir}/quarantine/DicomImportService"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{appdata_dir}/temp/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{appdata_dir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{appdata_dir}/temp/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageServices
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}/images"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="no"
            returnStoredFile="yes"
            quarantine="{appdata_dir}/quarantine/DirectoryStorageService"
            whitespaceReplacement="_" />
    </Pipeline>
</Configuration>
"""

IMAGEDEID_LOCAL_CONFIG = """<Configuration>
    <Server
        maxThreads="20"
        port="50000">
        <Log/>
    </Server>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="AuditLog"
        name="AuditLog"
        root="{appdata_dir}/temp/roots/AuditLog"/>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="DeidAuditLog"
        name="DeidAuditLog"
        root="{appdata_dir}/temp/roots/DeidAuditLog"/>
    <Pipeline name="imagedeid">
        <ArchiveImportService
            class="org.rsna.ctp.stdstages.ArchiveImportService"
            name="ArchiveImportService"
            fsName="DICOM Image Directory"
            root="{appdata_dir}/temp/roots/ArchiveImportService"
            treeRoot="{input_dir}"
            quarantine="{appdata_dir}/quarantine/ArchiveImportService"
            acceptFileObjects="no"
            acceptXmlObjects="no"
            acceptZipObjects="no"
            expandTARs="no"/>
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{appdata_dir}/temp/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{appdata_dir}/quarantine/DicomFilter"/>
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{appdata_dir}/temp/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DicomDecompressor
            class="org.rsna.ctp.stdstages.DicomDecompressor"
            name="DicomDecompressor"
            root="{appdata_dir}/temp/roots/DicomDecompressor"
            script="scripts/DicomDecompressor.script"
            quarantine="{appdata_dir}/quarantine/DicomDecompressor"/>
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            name="IDMap"
            root="{appdata_dir}/temp/roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="{appdata_dir}/temp/roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="{appdata_dir}/quarantine/DicomAnonymizer" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{appdata_dir}/temp/roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}/"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="no"
            returnStoredFile="yes"
            quarantine="{appdata_dir}/quarantine/DirectoryStorageService"
            whitespaceReplacement="_" />
    </Pipeline>
</Configuration>"""

IMAGEDEID_PACS_CONFIG = """<Configuration>
    <Server
        maxThreads="20"
        port="50000">
        <Log/>
    </Server>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="AuditLog"
        name="AuditLog"
        root="{appdata_dir}/temp/roots/AuditLog"/>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="DeidAuditLog"
        name="DeidAuditLog"
        root="{appdata_dir}/temp/roots/DeidAuditLog"/>
    <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="50001"
            calledAETTag="{application_aet}"
            root="{appdata_dir}/temp/roots/DicomImportService"
            quarantine="{appdata_dir}/quarantine"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{appdata_dir}/temp/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{appdata_dir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{appdata_dir}/temp/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DicomDecompressor
            class="org.rsna.ctp.stdstages.DicomDecompressor"
            name="DicomDecompressor"
            root="{appdata_dir}/temp/roots/DicomDecompressor"
            script="scripts/DicomDecompressor.script"
            quarantine="{appdata_dir}/quarantine"/>
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            name="IDMap"
            root="{appdata_dir}/temp/roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="{appdata_dir}/temp/roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="{appdata_dir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{appdata_dir}/temp/roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="no"
            returnStoredFile="yes"
            quarantine="{appdata_dir}/quarantine"
            whitespaceReplacement="_" />
    </Pipeline>
</Configuration>
"""

NLM_CONFIG_TEMPALTE = """ClinicalReports_dir = {input_dir}
nPHI_outdir = {output_dir}
ClinicalReports_files = [^\\.].*
Preserved_phrases = {preserved_file}
Redacted_phrases = {pii_file}
AutoOpenOutDir = Off
"""

COMMON_DATE_FORMATS = [
    '%m/%d/%Y','%Y-%m-%d','%d/%m/%Y','%m-%d-%Y','%Y/%m/%d','%d-%m-%Y',
    '%m/%d/%y','%y-%m-%d','%d/%m/%y'
]


def print_and_log(message):
    logging.info(message)
    print(message)

def error_and_exit(error):
    print_and_log(error)
    sys.exit(1)

def write(path, data):
    with open(path, "w") as f:
        f.write(data)

def strip_ctp_cell(value):
    return value.strip('=(")') if isinstance(value, str) else value

def ctp_get(url):
    request_url = f"http://localhost:50000/{url}"
    response = requests.get(request_url, auth=("admin", "password"))
    return response.text

def ctp_post(url, data):
    request_url = f"http://localhost:50000/{url}"
    response = requests.post(request_url, auth=("admin", "password"),
        data=data, headers={"Referer": f"http://localhost:50000/{url}"})
    return response.text

def ctp_get_status(key):
    return int(re.search(re.compile(rf"{key}:\s*<\/td><td>(\d+)"), ctp_get("status")).group(1))

def count_files(path, exclude_files):
    return sum(len([f for f in files if not f.startswith('.') and f not in exclude_files]) for _, _, files in os.walk(path))

def count_dicom_files(path):
    dicom_count = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                with open(os.path.join(root, f), 'rb') as file:
                    file.seek(128)
                    if file.read(4) == b'DICM':
                        dicom_count += 1
            except:
                continue
    return dicom_count

def run_progress(data):
    received = ctp_get_status("Files received") if data["querying_pacs"] else data["dicom_count"]
    quarantined = count_files(os.path.join(APPDATA_DIR, "quarantine"), {".", "..", "QuarantineIndex.db", "QuarantineIndex.lg"})
    saved = ctp_get_status("Files actually stored")
    stable = received == (quarantined + saved)
    return saved, quarantined, received, stable

def tick(tick_func, data):
    stable_for = 0
    while True:
        time.sleep(3)
        if tick_func is not None:
            tick_func(data)
        saved, quarantined, received, stable = run_progress(data)
        stable_for = stable_for + 1 if stable else 0
        if data["complete"] and stable_for > 3:
            break
        num, denom = (saved + quarantined), received
        if int(num) != int(denom):
            print_and_log(f"PROGRESS: {num}/{denom} files")
    print_and_log("PROGRESS: COMPLETE")

def start_ctp_run(tick_func, tick_data, logf, ctp_dir):
    if hasattr(sys, '_MEIPASS'):
        java_home = os.path.join(sys._MEIPASS, 'jre8', 'Contents', 'Home')
    else:
        java_home = os.environ.get('JAVA_HOME')
    
    java_executable = os.path.join(java_home, "bin", "java")
    env = os.environ.copy()
    env['JAVA_HOME'] = java_home
    
    ctp_process = subprocess.Popen(
        [java_executable, "-Xms16g", "-Xmx16g", "-jar", "Runner.jar"],
        cwd=ctp_dir, stdout=logf, stderr=logf, text=True, env=env
    )
    
    tick_data = {"complete": False, "querying_pacs": True, "dicom_count": count_dicom_files(INPUT_DIR)} | tick_data
    tick_thread = Thread(target=tick, args=(tick_func, tick_data,), daemon=True)
    tick_thread.start()
    return (ctp_process, tick_thread, tick_data)

def finish_ctp_run(ctp_process, tick_thread, tick_data, temp_ctp_dir):
    tick_data["complete"] = True
    tick_thread.join()
    try:
        response = requests.get(
            "http://localhost:50000/shutdown",
            headers={"servicemanager": "shutdown"},
            timeout=5
        )
        logging.info(f"HTTP shutdown response: {response.status_code}")
        try:
            ctp_process.wait(timeout=30)
            logging.info("CTP process terminated via HTTP shutdown")
            return
        except subprocess.TimeoutExpired:
            logging.warning("HTTP shutdown did not complete in time, falling back to signals")
    except Exception as e:
        logging.warning(f"HTTP shutdown failed: {e}, falling back to signals")

    ctp_process.send_signal(signal.SIGINT)
    try:
        ctp_process.wait(timeout=30)
        logging.info("CTP process terminated gracefully via SIGINT")
    except subprocess.TimeoutExpired:
        logging.warning("SIGINT did not terminate CTP, sending SIGTERM")
        ctp_process.terminate()
        try:
            ctp_process.wait(timeout=10)
            logging.info("CTP process terminated after SIGTERM")
        except subprocess.TimeoutExpired:
            logging.warning("CTP process did not terminate after SIGTERM, sending SIGKILL")
            ctp_process.kill()
            ctp_process.wait()
            logging.info("CTP process force killed")
    finally:
        shutil.rmtree(temp_ctp_dir, ignore_errors=True)

@contextmanager
def ctp_workspace(func, data, config_setup_func=None):
    temp_roots_dir = os.path.join(APPDATA_DIR, "temp", "roots")
    os.makedirs(temp_roots_dir, exist_ok=True)
    temp_ctp_dir = setup_ctp_directory()
    
    if config_setup_func:
        config_setup_func(temp_ctp_dir)
    
    with open(os.path.join(APPDATA_DIR, "log.txt"), "a") as logf:
        try:
            process, thread, data = start_ctp_run(func, data, logf, temp_ctp_dir)
            time.sleep(3)
            yield logf
        finally:
            finish_ctp_run(process, thread, data, temp_ctp_dir)
            shutil.rmtree(temp_roots_dir, ignore_errors=True)

def setup_ctp_directory():
    if hasattr(sys, '_MEIPASS'):
        source_ctp_dir = os.path.join(sys._MEIPASS, 'ctp')
    else:
        source_ctp_dir = "ctp"
    
    temp_ctp_dir = tempfile.mkdtemp(prefix='ctp_')
    shutil.copytree(source_ctp_dir, temp_ctp_dir, dirs_exist_ok=True)
    return temp_ctp_dir

def save_ctp_filters(ctp_filters, ctp_dir):
    with open(os.path.join(ctp_dir, "scripts", "dicom-filter.script"), "w") as f:
        f.write(ctp_filters if ctp_filters is not None else "true.")

def save_ctp_anonymizer(ctp_anonymizer, ctp_dir):
    if ctp_anonymizer is not None:
        with open(os.path.join(ctp_dir, "scripts", "DicomAnonymizer.script"), "w") as f:
            f.write(ctp_anonymizer)

def save_ctp_lookup_table(ctp_lookup_table, ctp_dir):
    if ctp_lookup_table is not None:
        with open(os.path.join(ctp_dir, "scripts", "LookupTable.properties"), "w") as f:
            f.write(ctp_lookup_table)
    else:
        with open(os.path.join(ctp_dir, "scripts", "LookupTable.properties"), "w") as f:
            f.write("")

def save_config(config, ctp_dir):
    with open(os.path.join(ctp_dir, "config.xml"), "w") as f:
        f.write(config)

def parse_dicom_tag_dict(output):
    tags = {}
    expr = rf".*\[(.+)\].+\#.+\,.+ (.+)"
    for value, tag in re.findall(expr, output):
        tags[tag.strip("\x00").strip()] = value.strip("\x00").strip()
    expr = rf".*=(.+).+\#.+\,.+ (.+)"
    for value, tag in re.findall(expr, output):
        tags[tag.strip("\x00").strip()] = value.strip("\x00").strip()
    expr = rf".*FD (.+).+\#.+\,.+ (.+)"
    for value, tag in re.findall(expr, output):
        tags[tag.strip("\x00").strip()] = value.strip("\x00").strip()
    expr = rf".*US (.+).+\#.+\,.+ (.+)"
    for value, tag in re.findall(expr, output):
        tags[tag.strip("\x00").strip()] = value.strip("\x00").strip()
    return tags

def cmove_queries(**config):
    df = pd.read_excel(os.path.join(INPUT_DIR, "input.xlsx"))
    queries = []
    accession_numbers = []  # Track all accession numbers
    logging.info(f"acc_col: {config.get('acc_col')}, mrn_col: {config.get('mrn_col')}")
    if config.get("acc_col") is not None:
        acc_mrn = list(df[[config.get("acc_col"), config.get("mrn_col")]].itertuples(index=False, name=None))
        for acc, mrn in acc_mrn:
            queries.append(f"-k QueryRetrieveLevel=STUDY -k AccessionNumber=*{str(acc)}* -k PatientID={str(mrn)}")
            accession_numbers.append(str(acc))
    else:
        mrn_dates = list(df[[config.get("mrn_col"), config.get("date_col")]].itertuples(index=False, name=None))
        for mrn, dt in mrn_dates:
            dts = datetime.strftime((dt - timedelta(days=config.get("date_window"))), "%Y%m%d")
            dte = datetime.strftime((dt + timedelta(days=config.get("date_window"))), "%Y%m%d")
            queries.append(f"-k QueryRetrieveLevel=STUDY -k PatientID={str(mrn)} -k StudyDate={dts}-{dte}")
    return queries, accession_numbers

def cmove_images(logf, **config):
    failed_accessions = []
    successful_accessions = set()
    study_uids_accessions = {}
    
    queries, accession_numbers = cmove_queries(**config)
    
    for pacs in config.get("pacs"):
        study_uids = set()
        ip, port, aec = pacs.get("ip"), pacs.get("port"), pacs.get("ae")
        aet, aem = config.get("application_aet"), config.get("application_aet")
        queries, accession_numbers = cmove_queries(**config)
        for i, query in enumerate(queries):
            cmd = ["findscu", "-v", "-aet", aet, "-aec", aec, "-S"] + query.split() + ["-k", "StudyInstanceUID", ip, str(port)]
            logging.info(" ".join(cmd))
            process = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output = process.stderr
            for entry in output.split("Find Response:")[1:]:
                tags = parse_dicom_tag_dict(entry)
                study_uid = tags.get("StudyInstanceUID")
                acc_num = tags.get("AccessionNumber")
                if len(study_uid) > 0:
                    study_uids.add(study_uid)
                    if acc_num:
                        study_uids_accessions[study_uid] = acc_num
                else:
                    logging.info(f"No studies found for query: {query}")
                
                logging.info(f"Processed {i+1}/{len(queries)} rows")

        # Process all moves with up to 3 attempts
        retry_count = 0
        current_moves = list(study_uids)
        
        while current_moves and retry_count < 3:
            if retry_count > 0:
                logging.info(f"Retry attempt {retry_count} for {len(current_moves)} failed moves")
                time.sleep(5)  # Wait between retry batches
                
            failed_moves = []
            for i, study_uid in enumerate(current_moves):
                cmd = ["movescu", "-v", "-aet", aet, "-aem", aem, "-aec", aec, "-S", "-k", "QueryRetrieveLevel=STUDY", "-k", f"StudyInstanceUID={study_uid}", ip, str(port)]
                logging.info(" ".join(cmd))
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                stdout, stderr = process.communicate()
                process.wait()
                
                success = "Received Final Move Response (Success)" in (stdout + stderr)
                if success:
                    successful_accessions.add(study_uids_accessions[study_uid])
                else:
                    failed_moves.append(study_uid)
                    logging.info(f"Failed to move study: {study_uid}")
                
                logging.info(f"Downloaded {i+1}/{len(current_moves)} studies")
            
            current_moves = failed_moves
            retry_count += 1

    failed_accessions = []
    for acc in accession_numbers:
        if acc not in successful_accessions:
            if not any(acc in successful_acc for successful_acc in successful_accessions):
                failed_accessions.append(acc)
    
    return failed_accessions

def save_metadata_csv():
    metadata_csv = ctp_get("AuditLog?export&csv&suppress")
    with open(os.path.join(APPDATA_DIR, "metadata.csv"), "w") as f:
        f.write(metadata_csv)

def save_deid_metadata_csv():
    deid_metadata_csv = ctp_get("DeidAuditLog?export&csv&suppress")
    with open(os.path.join(APPDATA_DIR, "deid_metadata.csv"), "w") as f:
        f.write(deid_metadata_csv)

def save_failed_accessions(failed_accessions):
    metadata_path = os.path.join(APPDATA_DIR, "metadata.csv")
    
    with open(metadata_path, "a") as f:
        for acc in failed_accessions:
            line = f"{time.strftime('%Y-%m-%d %H:%M:%S')},{acc},Failed to retrieve\n"
            f.write(line)

def save_quarantined_files_log():
    """Save detailed log of quarantined files to appdata"""
    log_path = os.path.join(APPDATA_DIR, "quarantined_files_log.csv")
    
    quarantine_dirs = {
        "ArchiveImportService": os.path.join(APPDATA_DIR, "quarantine", "ArchiveImportService"),
        "DicomFilter": os.path.join(APPDATA_DIR, "quarantine", "DicomFilter"), 
        "DicomDecompressor": os.path.join(APPDATA_DIR, "quarantine", "DicomDecompressor"),
        "DicomAnonymizer": os.path.join(APPDATA_DIR, "quarantine", "DicomAnonymizer"),
        "DirectoryStorageService": os.path.join(APPDATA_DIR, "quarantine", "DirectoryStorageService")
    }

    with open(log_path, "w") as f:
        f.write("Stage,Filename,Path\n")
        
        for stage, quarantine_path in quarantine_dirs.items():
            if os.path.exists(quarantine_path):
                for root, dirs, filenames in os.walk(quarantine_path):
                    for filename in filenames:
                        if not filename.startswith('.') and filename not in ['QuarantineIndex.db', 'QuarantineIndex.lg']:
                            file_path = os.path.join(root, filename)
                            f.write(f"{stage},{filename},{file_path}\n")
    
    logging.info(f"Quarantined files log saved to {log_path}")

def save_linker_csv():
    linker_csv = ctp_post("idmap", {"p": 0, "s": 5, "keytype": "trialAN", "keys": "", "format": "csv"})
    with open(os.path.join(APPDATA_DIR, "linker.csv"), "w") as f:
        f.write(linker_csv)

def scrub(data, whitelist, blacklist):
    try:
        # Create a specific temp directory instead of using default system temp
        temp_dir = "/app/temp"  # or another path where we know we have write permissions
        os.makedirs(temp_dir, exist_ok=True)
        os.makedirs(f"{temp_dir}/input", exist_ok=True)
        os.makedirs(f"{temp_dir}/output", exist_ok=True)
        for i, d in enumerate(data):
            text = str(d) if d is not None else "Empty" 
            write(f"{temp_dir}/input/{i}.txt", ''.join(c for c in text if c in string.printable))
        write(f"{temp_dir}/preserved.txt", "\n".join(whitelist))
        write(f"{temp_dir}/pii.txt", "\n".join(blacklist))
        write(f"{temp_dir}/config.txt", NLM_CONFIG_TEMPALTE.format(
            input_dir=f"{temp_dir}/input",
            output_dir=f"{temp_dir}/output", 
            preserved_file=f"{temp_dir}/preserved.txt",
            pii_file=f"{temp_dir}/pii.txt",
        ))
        result = subprocess.run(["/root/scrubber.19.0403.lnx", f"{temp_dir}/config.txt"], 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={
                'TMPDIR': temp_dir,
                'TEMP': temp_dir,
                'TMP': temp_dir,
            })
        results = []
        for i in range(len(data)):
            output_file = f"{temp_dir}/output/{i}.nphi.txt"
            with open(output_file) as f:
                scrubbed = f.read().split("##### DOCUMENT #")[0].strip()
                results.append(scrubbed)
            print_and_log(f"PROGRESS: {i}/{len(data)} rows de-identified")
        return results
    except Exception as e:
        error_msg = f"Error in scrub function: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def date_shift_text(original_list, deided_list, date_shift_by):
    shifted_list = []
    for i, (original, deided) in enumerate(zip(original_list, deided_list)):
        dates = []
        for d_line, o_line in zip(deided.split('\n'), original.split('\n')):
            pos = 0
            while '[DATE]' in d_line[pos:]:
                pos = d_line.find('[DATE]', pos) 
                context = o_line[max(0,pos-30):min(len(o_line),pos+35)]
                for word in context.split():
                    for fmt in COMMON_DATE_FORMATS:
                        try:
                            datetime.strptime(word, fmt)
                            dates.append(word)
                            break
                        except ValueError:
                            continue
                    else:
                        continue
                    break
                pos += 1
        result = deided
        for date in dates:
            shifted = datetime.strptime(date, '%m/%d/%Y') + timedelta(days=date_shift_by)
            result = result.replace('[DATE]', shifted.strftime('%m/%d/%Y'), 1)
        shifted_list.append(result)
        print_and_log(f"PROGRESS: {i+1}/{len(original_list)} rows date shifted")
    return shifted_list

def imageqr_func(_):
    save_metadata_csv()

def imageqr_main(**config):
    def setup_config(temp_ctp_dir):
        save_ctp_filters(config.get("ctp_filters"), temp_ctp_dir)
        appdata_abs = os.path.abspath(APPDATA_DIR)
        output_abs = os.path.abspath(OUTPUT_DIR)
        formatted_config = IMAGEQR_CONFIG.format(appdata_dir=appdata_abs, output_dir=output_abs, application_aet=config.get("application_aet"))
        save_config(formatted_config, temp_ctp_dir)
    
    with ctp_workspace(imageqr_func, {}, setup_config) as logf:
        failed_accessions = cmove_images(logf, **config)
        logging.info(f"Accessions that failed to process: {', '.join(failed_accessions)}")

    save_failed_accessions(failed_accessions)
    save_quarantined_files_log()

def header_extract_main(**config):
    dicom_folder = INPUT_DIR
    excel_path = os.path.join(OUTPUT_DIR, "headers.xlsx")
    batch_size = config.get('batch_size', 100)  # Process 100 files at a time by default
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(excel_path), exist_ok=True)
    
    # Collect all DICOM files first
    dicom_files = []
    for root, _, files in os.walk(dicom_folder):
        for filename in files:
            filepath = os.path.join(root, filename)
            if (os.path.isfile(filepath) and 
                filename.endswith('.dcm') and 
                not filename.startswith('.')):
                dicom_files.append(filepath)
    
    if not dicom_files:
        logging.error("No valid DICOM files found.")
        return
    
    total_files = len(dicom_files)
    logging.info(f"Found {total_files} DICOM files to process")
    
    csv_path = excel_path.replace('.xlsx', '.csv')
    total_headers_processed = 0
    first_batch = True
    known_columns = []
    
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch_files = dicom_files[batch_start:batch_end]
        
        logging.info(f"Processing batch {batch_start//batch_size + 1}: files {batch_start+1}-{batch_end} of {total_files}")
        
        header_data = []
        batch_columns = set()
        
        for filepath in batch_files:
            filename = os.path.basename(filepath)
            try:
                ds = pydicom.dcmread(filepath, stop_before_pixels=True)
                headers = {elem.keyword or str(elem.tag): elem.value for elem in ds.iterall() if elem.VR != 'SQ'}
                headers['__Filename__'] = filename
                batch_columns.update(headers.keys())
                header_data.append(headers)
            except Exception as e:
                logging.error(f"Error reading {filename}: {e}")
        
        if header_data:
            try:
                # Check for new columns in this batch
                new_columns = batch_columns - set(known_columns)
                if new_columns:
                    new_columns = sorted(list(new_columns))
                    logging.info(f"Found {len(new_columns)} new columns: {new_columns}")
                    known_columns.extend(new_columns)
                    known_columns.sort()  # Keep columns sorted
                
                df = pd.json_normalize(header_data)
                
                # If this is not the first batch and we have new columns, update existing CSV
                if not first_batch and new_columns:
                    # Read existing CSV and add new columns
                    existing_df = pd.read_csv(csv_path)
                    
                    # Add new columns to existing data (fill with None)
                    for col in new_columns:
                        existing_df[col] = None
                    
                    # Reorder columns to match known_columns
                    existing_df = existing_df.reindex(columns=known_columns, fill_value=None)
                    
                    # Write updated CSV
                    existing_df.to_csv(csv_path, index=False, mode='w')
                    logging.info(f"Updated existing CSV with {len(new_columns)} new columns")
                
                # Ensure current batch has all known columns
                df = df.reindex(columns=known_columns, fill_value=None)
                
                # Write to CSV (append mode for subsequent batches)
                if first_batch:
                    df.to_csv(csv_path, index=False, mode='w')
                    first_batch = False
                else:
                    df.to_csv(csv_path, index=False, mode='a', header=False)
                
                total_headers_processed += len(df)
                logging.info(f"Processed {len(header_data)} headers in this batch (total: {total_headers_processed})")
                
            except Exception as e:
                logging.error(f"Error processing batch: {e}")
    
    if total_headers_processed == 0:
        logging.error("No valid DICOM headers extracted.")
        return
    
    try:
        logging.info(f"Converting CSV to Excel format in batches...")
        
        chunk_size = 1000  # Process 1000 rows at a time
        all_chunks = []
        
        # Collect all chunks first (but still memory efficient)
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            all_chunks.append(chunk_df)
            logging.info(f"Loaded chunk of {len(chunk_df)} rows for Excel conversion")
        
        # Write all chunks to Excel at once (more efficient than reading back)
        if all_chunks:
            logging.info(f"Writing {len(all_chunks)} chunks to Excel...")
            combined_df = pd.concat(all_chunks, ignore_index=True)
            combined_df.to_excel(excel_path, index=False)
        
        # Clean up CSV file
        os.remove(csv_path)
        
        logging.info(f"Successfully extracted and saved {total_headers_processed} headers to {excel_path}")
    except Exception as e:
        logging.error(f"Error converting CSV to Excel: {e}")
        logging.info(f"CSV file saved at {csv_path} as backup")

def imagedeid_func(_):
    save_metadata_csv()
    save_deid_metadata_csv()
    save_linker_csv()

def imagedeid_main(**config):
    querying_pacs = os.path.exists(os.path.join(INPUT_DIR, "input.xlsx"))
    
    def setup_config(temp_ctp_dir):
        save_ctp_filters(config.get("ctp_filters"), temp_ctp_dir)
        save_ctp_anonymizer(config.get("ctp_anonymizer"), temp_ctp_dir)
        save_ctp_lookup_table(config.get("ctp_lookup_table"), temp_ctp_dir)
        config_template = IMAGEDEID_PACS_CONFIG if querying_pacs else IMAGEDEID_LOCAL_CONFIG
        appdata_abs = os.path.abspath(APPDATA_DIR)
        output_abs = os.path.abspath(OUTPUT_DIR)
        input_abs = os.path.abspath(INPUT_DIR)
        formatted_config = config_template.format(appdata_dir=appdata_abs, output_dir=output_abs, input_dir=input_abs, application_aet=config.get("application_aet"))
        save_config(formatted_config, temp_ctp_dir)
    
    with ctp_workspace(imagedeid_func, {"querying_pacs": querying_pacs}, setup_config) as logf:
        if querying_pacs:
            failed_accessions = cmove_images(logf, **config)
            logging.info(f"Accessions that failed to process: {', '.join(failed_accessions)}")
        else:
            failed_accessions = []

    save_failed_accessions(failed_accessions)
    save_quarantined_files_log()

    total_quarantined = count_files(os.path.join(APPDATA_DIR, "quarantine"), {".", "..", "QuarantineIndex.db", "QuarantineIndex.lg"})
    logging.info(f"PROCESSING COMPLETE - Failed accessions: {len(failed_accessions)}, Quarantined files: {total_quarantined}")

def parse_rclone_config(config_path):
    """Parse rclone config file into sections"""
    sections = {}
    current_section = None
    
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1]
                sections[current_section] = {}
            elif current_section and '=' in line:
                key, value = [x.strip() for x in line.split('=', 1)]
                sections[current_section][key] = value
    
    return sections

def get_rclone_flags(storage_type):
    """Get storage-specific rclone flags"""
    common_flags = [
        "--progress",
        "--transfers", "4",
        "--retries", "10",
        "--low-level-retries", "10",
        "--contimeout", "60s",
        "--timeout", "300s",
    ]
    
    if storage_type == 's3':
        return common_flags + ["--s3-upload-concurrency", "4"]
    elif storage_type == 'azureblob':
        return common_flags + ["--azure-upload-concurrency", "4"]
    elif storage_type in ['drive', 'onedrive']:
        return common_flags + [
            "--drive-chunk-size", "64M",
            "--drive-upload-cutoff", "64M",
            "--drive-acknowledge-abuse"
        ]
    return common_flags

def get_rclone_path(config):
    storage_location = config.get('storage_location')
    project_name = config.get('project_name')
    safe_project_name = project_name.replace(' ', '-').lower()

    # Get the storage configuration
    rclone_sections = parse_rclone_config('/rclone.conf')
    if storage_location not in rclone_sections:
        raise ValueError(f"Storage location '{storage_location}' not found in rclone config")
    
    storage_config = rclone_sections[storage_location]
    storage_type = storage_config.get('type')

    if storage_type == 's3':
        bucket_name = config.get('bucket_name', storage_config.get('bucket'))
        return f"{storage_location}:{bucket_name}/{safe_project_name}"
    elif storage_type == 'drive':
        return f"{storage_location}:/{safe_project_name}"
    elif storage_type == 'azureblob':
        container_name = config.get('container_name', storage_config.get('container'))
        return f"{storage_location}:{container_name}/{safe_project_name}"
    elif storage_type == 'onedrive':
        return f"{storage_location}:/{safe_project_name}"
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

def image_export_main(**config):
    try:
        container_name = config.get('container_name')
        site_id = config.get('site_id')
        project_name = config.get('project_name')
        if container_name is None or project_name is None:
            error_and_exit("Container name and project name are required.")
        cmd = ["rclone", "copy", "--progress", "--config", "/rclone.conf", INPUT_DIR, f"azure:{container_name}/{site_id}/{project_name}"]
        logging.info(' '.join(cmd))
        with open(os.path.join(APPDATA_DIR, "log.txt"), "a") as logf:
            subprocess.run(cmd, text=True, stdout=logf, stderr=logf)
        logging.info("PROGRESS: COMPLETE")
    except Exception as e:
        error_and_exit(f"Error: {e}")

def textdeid_main(**config):
    try:
        df = pd.read_excel(os.path.join(INPUT_DIR, "input.xlsx"), header=None)
        original_data = df.iloc[:,0].tolist()
        deid_data = scrub(original_data, config.get("to_keep_list"), config.get("to_remove_list"))
        shifted_data = date_shift_text(original_data, deid_data, config.get("date_shift_by"))
        pd.DataFrame(shifted_data).to_excel(os.path.join(OUTPUT_DIR, "output.xlsx"), index=False, header=False)
        print_and_log("PROGRESS: COMPLETE")
    except Exception as e:
        error_and_exit(f"Error: {e}")

def validate_config(input_dir, output_dir, config):
    if config is None:
        error_and_exit("Config file unable to load or invalid.")
    if not os.path.exists(input_dir):
        error_and_exit("Input directory not found.")
    if os.listdir(output_dir) != []:
        error_and_exit("Output directory must be empty.")
    if config.get("to_keep_list") is None or config.get("to_remove_list") is None:
        error_and_exit("to_keep_list and to_remove_list must be provided.")
    if config.get("date_shift_by") is None:
        error_and_exit("Date shift by must be provided.")

def validate_ctp_filters(filters):
    grammar = r"""start: expr
        ?expr: term | expr ("+" | "*") term
        ?term: "!" term | item "." method "(" STRING ")" | "true." | "false." | "(" expr ")"
        item: /[A-Za-z][\w:]*/ | /\[[0-9A-Fa-f]{4},[0-9A-Fa-f]{4}\]/
        method: "equals" | "equalsIgnoreCase" | "matches" | "contains" | "containsIgnoreCase" | "startsWith" | "startsWithIgnoreCase" | "endsWith" | "endsWithIgnoreCase" | "isLessThan" | "isGreaterThan"
        STRING: /"[^"]*"/
        %import common.WS
        %ignore WS
        %ignore /\/\/[^\n]*/"""
    parser = Lark(grammar, start='start', parser='lalr')
    try:
        parser.parse(filters.strip())
    except Exception as e:
        error_and_exit(f"Invalid CTP filters: {e}")

def validate_ctp_anonymizer(anonymizer):
    try:
        ET.fromstring(anonymizer)
    except ET.ParseError as e:
        error_and_exit(f"Invalid CTP anonymizer. {e}")

def validate_excel(path, **config):
    acc_col = config.get("acc_col")
    mrn_col = config.get("mrn_col")
    date_col = config.get("date_col")
    try:
        df = pd.read_excel(path)
    except ValueError:
        error_and_exit("Invalid excel file.")
    if acc_col is not None:
        if acc_col not in df.columns:
            error_and_exit(f"Column {acc_col} not found in excel file.")
        if df[acc_col].isnull().values.any():
            error_and_exit(f"Column {acc_col} has empty values.")
    else:
        if mrn_col not in df.columns:
            error_and_exit(f"Column {mrn_col} not found in excel file.")
        if date_col not in df.columns:
            error_and_exit(f"Column {date_col} not found in excel file.")
        if df[mrn_col].isnull().values.any():
            error_and_exit(f"Column {mrn_col} cannot have empty values.")
        if df[date_col].isnull().values.any():
            error_and_exit(f"Column {date_col} cannot have empty values.")
        if not df[date_col].apply(lambda x: isinstance(x, pd.Timestamp)).all():
            error_and_exit(f"Column {date_col} must be in excel date format.")

def validate_config(config):
    if config is None:
        error_and_exit("Config file unable to load or invalid.")
    if config.get("module") is None:
        error_and_exit("Module not specified in config file.")
    if not os.environ.get('JAVA_HOME') and not hasattr(sys, '_MEIPASS'):
        error_and_exit("JAVA_HOME environment variable is not set")
    # if config.get("module") not in ["imageqr", "imagedeid", "imageexport"]:
    #     error_and_exit("Module invalid or not implemented.")
    if not os.path.exists(INPUT_DIR):
        error_and_exit(f"Input directory not found: {INPUT_DIR}")
    if config.get("ctp_filters") is not None:
        validate_ctp_filters(config.get("ctp_filters"))
    if config.get("ctp_anonymizer") is not None:
        validate_ctp_anonymizer(config.get("ctp_anonymizer"))
    if config.get("module") in ["imageqr", "imagedeid"] and os.listdir(OUTPUT_DIR) != []:
        error_and_exit("Output directory must be empty.")
    if os.path.exists(os.path.join(INPUT_DIR, "input.xlsx")):
        if config.get("module") in ["imageqr", "imagedeid"]:
            if any([config.get("pacs_ip"), config.get("pacs_port"), config.get("pacs_aet")]):
                error_and_exit("pacs_ip, pacs_port, and pacs_aet have been deprecated. Please use the pacs list instead.")
            if not config.get("pacs"):
                error_and_exit("Pacs details missing in config file.")
            for pacs in config.get("pacs"):
                if not all([pacs.get("ip"), pacs.get("port"), pacs.get("ae")]):
                    error_and_exit("Pacs details missing in config file.")
            if config.get("application_aet") is None or config.get("application_aet") == "":
                error_and_exit("Application AET missing in config file.")
            if config.get("acc_col") is None and (config.get("mrn_col") is None or config.get("date_col") is None):
                error_and_exit("Either the accession column name or mrn + date column names are required.")
            if config.get("acc_col") is not None and config.get("mrn_col") is not None and config.get("date_col") is not None:
                error_and_exit("Can only query using one of accession or mrn + date. Not both.")
            if config.get("date_window") is not None and not isinstance(config.get("date_window"), int):
                error_and_exit("Date window must be an integer.")
            validate_excel(os.path.join(INPUT_DIR, "input.xlsx"), **config)
    elif config.get("module") in ["imagedeid", "imageexport"]:
        if count_dicom_files(INPUT_DIR) == 0:
            error_and_exit("No DICOM files found in input directory.")

def imageqr(**config):
    """
    Download DICOM images from PACS. The input directory must contain
    an input.xlsx file with accession numbers or MRN and date columns.

    Keyword Args:
        pacs (list): List of PACS configurations, each containing:
            - ip (str): IP address of the PACS
            - port (str): Port of the PACS 
            - ae (str): AE title of the PACS
        application_aet (str): AE title of the calling application.
        acc_col (str): Accession number column name in excel file.            
        mrn_col (str): MRN column name in excel file.
        date_col (str): Date column name in excel file.
        date_window (int): Number of days to search around the date.
        ctp_filters (str): Filters for the query in the CTP format.
        ctp_anonymizer (str): Anonymization script in the CTP xml format.
    """
    imageqr_main(**config)

def headerextract(**config):
    """
    Extracts DICOM headers from local directory.
    Takes all files in the input directory and saves the headers 
    to an Excel file.
    """
    header_extract_main(**config)

def imagedeid(**config):
    """
    Deidentify DICOM images from local directory or PACS. If input
    directory contains input.xlsx file, the PACS will be queried.
    If input directory doesn't contain input.xlsx file, all DICOM
    files in the input directory will be deidentified.

    Keyword Args:
        pacs (list): List of PACS configurations, each containing:
            - ip (str): IP address of the PACS
            - port (str): Port of the PACS 
            - ae (str): AE title of the PACS
        application_aet (str): AE title of the calling application.
        acc_col (str): Accession number column name in excel file.            
        mrn_col (str): MRN column name in excel file.
        date_col (str): Date column name in excel file.
        date_window (int): Number of days to search around the date.
        ctp_filters (str): Filters for the query in the CTP format.
        ctp_anonymizer (str): Anonymization script in the CTP xml format.
    """
    imagedeid_main(**config)

def imageexport(**config):
    """
    Export DICOM images to a cloud storage location.
    Takes all files in the input directory and exports them to the
    storage location.

    Keyword Args:
        storage_location (str): Cloud storage location.
        project_name (str): Name of the project for folder name.
    """
    image_export_main(**config)

def textdeid(**config):
    """
    De-identifies text from the input.xlsx file in the input directory.
    The excel file should only have one column with text to be de-identified.
    The excel file should not have any headers.

    Keyword Args:
        date_shift_by (int): Number of days to shift the date.
        to_keep_list (list): List of phrases to be preserved.
        to_remove_list (list): List of phrases to be de-identified.
    """
    textdeid_main(**config)

def generalmodule(**config):
    logging.info(f"Running module: {config.get('module')}")
    logging.info(f"Config: {config}")
    module = config.get("module")
    if not MODULES_DIR:
        error_and_exit("ICORE_MODULES_DIR environment variable must be set for custom modules")
    print(os.listdir(MODULES_DIR))
    module_path = os.path.abspath(os.path.join(MODULES_DIR, f"{module}"))
    if not os.path.exists(module_path):
        error_and_exit(f"Module {module} not found.")

    module_cmd = [
        module_path,
        CONFIG_PATH,
        INPUT_DIR,
        OUTPUT_DIR,
        os.path.join(APPDATA_DIR, "log.txt")
    ]

    try:
        result = subprocess.run(module_cmd, check=True, capture_output=True, text=True)
        logging.info("Module output: %s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.info(f"Module output: {e.stderr}")
        error_and_exit(f"Module execution failed with exit code {e.returncode}: {e.stderr}")

def run_module(**config):
    logging.info(f"Running module: {config.get('module')}")
    logging.info(f"Config: {config}")
    if config.get("module") in ["imageqr", "imagedeid", "imageexport", "headerextract", "textdeid"]:
        globals()[config.get("module")](**config)
    else:
        generalmodule(**config)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        error_and_exit("Usage: icore_processor <config.yml> <input_dir> <output_dir>")
    
    CONFIG_PATH = sys.argv[1]
    INPUT_DIR = sys.argv[2]
    OUTPUT_DIR = sys.argv[3]
    APPDATA_DIR = os.environ.get('ICORE_APPDATA_DIR')
    MODULES_DIR = os.environ.get('ICORE_MODULES_DIR')
    
    if not APPDATA_DIR:
        error_and_exit("ICORE_APPDATA_DIR environment variable must be set")
    
    if not os.path.exists(CONFIG_PATH):
        error_and_exit(f"Config file not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "r") as file:
        config = yaml.safe_load(file)
    logging.basicConfig(filename=os.path.join(APPDATA_DIR, "log.txt"), level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    validate_config(config)
    run_module(**config)
