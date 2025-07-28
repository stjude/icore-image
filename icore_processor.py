import logging
import os
import re
import signal
import subprocess
import sys
import time
import pydicom
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Thread

import pandas as pd
import requests
import yaml
from lark import Lark

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
        root="roots/AuditLog"/>
    <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="50001"
            calledAETTag="{application_aet}"
            root="roots/DicomImportService"
            quarantine="quarantines/DicomImportService"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="../appdata/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageServices
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="../output/images"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="no"
            returnStoredFile="yes"
            quarantine="quarantines/DirectoryStorageService"
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
        root="roots/AuditLog"/>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="DeidAuditLog"
        name="DeidAuditLog"
        root="roots/DeidAuditLog"/>
    <Pipeline name="imagedeid">
        <ArchiveImportService
            class="org.rsna.ctp.stdstages.ArchiveImportService"
            name="ArchiveImportService"
            fsName="DICOM Image Directory"
            root="roots/ArchiveImportService"
            treeRoot="../input"
            quarantine="quarantines/ArchiveImportService"
            acceptFileObjects="no"
            acceptXmlObjects="no"
            acceptZipObjects="no"
            expandTARs="no"/>
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="../appdata/quarantine"/>
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DicomDecompressor
            class="org.rsna.ctp.stdstages.DicomDecompressor"
            name="DicomDecompressor"
            root="roots/DicomDecompressor"
            script="scripts/DicomDecompressor.script"
            quarantine="quarantines/DicomDecompressor"/>
        <DicomPixelAnonymizer
            class="org.rsna.ctp.stdstages.DicomPixelAnonymizer"
            name="DicomPixelAnonymizer"
            root="roots/DicomPixelAnonymizer"
            script="scripts/DicomPixelAnonymizer.script"
            quarantine="quarantines/DicomPixelAnonymizer"/>
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            name="IDMap"
            root="roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="../appdata/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="../output/"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="no"
            returnStoredFile="yes"
            quarantine="quarantines/DirectoryStorageService"
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
        root="roots/AuditLog"/>
    <Plugin
        class="org.rsna.ctp.stdplugins.AuditLog"
        id="DeidAuditLog"
        name="DeidAuditLog"
        root="roots/DeidAuditLog"/>
    <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="50001"
            calledAETTag="{application_aet}"
            root="roots/DicomImportService"
            quarantine="../appdata/quarantine"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="../appdata/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DicomDecompressor
            class="org.rsna.ctp.stdstages.DicomDecompressor"
            name="DicomDecompressor"
            root="roots/DicomDecompressor"
            script="scripts/DicomDecompressor.script"
            quarantine="../appdata/quarantine"/>
        <DicomPixelAnonymizer
            class="org.rsna.ctp.stdstages.DicomPixelAnonymizer"
            name="DicomPixelAnonymizer"
            root="roots/DicomPixelAnonymizer"
            script="scripts/DicomPixelAnonymizer.script"
            quarantine="../appdata/quarantine"/>
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            name="IDMap"
            root="roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="../appdata/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="../output"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="no"
            returnStoredFile="yes"
            quarantine="../appdata/quarantine"
            whitespaceReplacement="_" />
    </Pipeline>
</Configuration>
"""


def print_and_log(message):
    logging.info(message)
    print(message)

def error_and_exit(error):
    print_and_log(error)
    sys.exit(1)

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
    return len([f for f in os.listdir(path) if not f.startswith('.') and os.path.isfile(os.path.join(path, f)) and f not in exclude_files])

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
    quarantined = count_files(os.path.join("appdata", "quarantine"), {".", "..", "QuarantineIndex.db", "QuarantineIndex.lg"})
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

def start_ctp_run(tick_func, tick_data, logf):
    ctp_process = subprocess.Popen(["java", "-Xms16g", "-Xmx16g", "-jar", "Runner.jar"], cwd="ctp", stdout=logf, stderr=logf, text=True)
    tick_data = {"complete": False, "querying_pacs": True, "dicom_count": count_dicom_files("input")} | tick_data
    tick_thread = Thread(target=tick, args=(tick_func, tick_data,), daemon=True)
    tick_thread.start()
    return (ctp_process, tick_thread, tick_data)

def finish_ctp_run(ctp_process, tick_thread, tick_data):
    tick_data["complete"] = True
    tick_thread.join()
    ctp_process.send_signal(signal.SIGINT)

@contextmanager
def ctp_workspace(func, data):
    with open(os.path.join("appdata", "log.txt"), "a") as logf:
        try:
            process, thread, data = start_ctp_run(func, data, logf)
            time.sleep(3)
            yield logf
        finally:
            finish_ctp_run(process, thread, data)

def save_ctp_filters(ctp_filters):
    with open(os.path.join("ctp", "scripts", "dicom-filter.script"), "w") as f:
        f.write(ctp_filters if ctp_filters is not None else "true.")

def save_ctp_anonymizer(ctp_anonymizer):
    if ctp_anonymizer is not None:
        with open(os.path.join("ctp", "scripts", "DicomAnonymizer.script"), "w") as f:
            f.write(ctp_anonymizer)

def save_ctp_lookup_table(ctp_lookup_table):
    if ctp_lookup_table is not None:
        with open(os.path.join("ctp", "scripts", "LookupTable.properties"), "w") as f:
            f.write(ctp_lookup_table)

def save_config(config):
    with open(os.path.join("ctp", "config.xml"), "w") as f:
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
    df = pd.read_excel(os.path.join("input", "input.xlsx"))
    queries = []
    accession_numbers = []  # Track all accession numbers
    if config.get("acc_col") is not None:
        for acc in df[config.get("acc_col")]:
            accession_numbers.append(str(acc))
            queries.append(f"-k QueryRetrieveLevel=STUDY -k AccessionNumber={str(acc)}")
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

    # Compare original accession numbers with successful ones to determine failures
    failed_accessions = [acc for acc in accession_numbers if acc not in successful_accessions]
    
    return failed_accessions

def save_metadata_csv():
    metadata_csv = ctp_get("AuditLog?export&csv&suppress")
    with open(os.path.join("appdata", "metadata.csv"), "w") as f:
        f.write(metadata_csv)

def save_deid_metadata_csv():
    deid_metadata_csv = ctp_get("DeidAuditLog?export&csv&suppress")
    with open(os.path.join("appdata", "deid_metadata.csv"), "w") as f:
        f.write(deid_metadata_csv)

def save_failed_accessions(failed_accessions):
    metadata_path = os.path.join("appdata", "metadata.csv")
    
    with open(metadata_path, "a") as f:
        for acc in failed_accessions:
            line = f"{time.strftime('%Y-%m-%d %H:%M:%S')},{acc},Failed to retrieve\n"
            f.write(line)

def save_linker_csv():
    linker_csv = ctp_post("idmap", {"p": 0, "s": 5, "keytype": "trialAN", "keys": "", "format": "csv"})
    with open(os.path.join("appdata", "linker.csv"), "w") as f:
        f.write(linker_csv)

def imageqr_func(_):
    save_metadata_csv()

def imageqr_main(**config):
    save_ctp_filters(config.get("ctp_filters"))
    save_config(IMAGEQR_CONFIG)
    with ctp_workspace(imageqr_func, {}) as logf:
        failed_accessions = cmove_images(logf, **config)
        logging.info(f"Accessions that failed to process: {', '.join(failed_accessions)}")
    save_failed_accessions(failed_accessions)

def header_extract_main(**config):
    dicom_folder = "input"
    excel_path = "output/headers.xlsx"
    header_data = []

    for root, _, files in os.walk(dicom_folder):
        for i, filename in enumerate(files):
            logging.info(f"Processing {i+1}/{len(files)} files in {root}")
            filepath = os.path.join(root, filename)
            if not os.path.isfile(filepath):
                continue
            if not filename.endswith('.dcm'):
                continue
            if filename.startswith('.'):
                continue
            try:
                ds = pydicom.dcmread(filepath, stop_before_pixels=True)
                headers = {elem.keyword or elem.tag: elem.value for elem in ds.iterall() if elem.VR != 'SQ'}
                headers['__Filename__'] = filename
                header_data.append(headers)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    if not header_data:
        print("No valid DICOM files found.")
        return

    df = pd.json_normalize(header_data)
    df.to_excel(excel_path, index=False)
    print(f"Extracted headers saved to {excel_path}")

def imagedeid_func(_):
    save_metadata_csv()
    save_deid_metadata_csv()
    save_linker_csv()

def imagedeid_main(**config):
    save_ctp_filters(config.get("ctp_filters"))
    save_ctp_anonymizer(config.get("ctp_anonymizer"))
    save_ctp_lookup_table(config.get("ctp_lookup_table"))
    querying_pacs = os.path.exists(os.path.join("input", "input.xlsx"))
    config_template = IMAGEDEID_PACS_CONFIG if querying_pacs else IMAGEDEID_LOCAL_CONFIG
    formatted_config = config_template.format(application_aet=config.get("application_aet"))
    save_config(formatted_config)
    with ctp_workspace(imagedeid_func, {"querying_pacs": querying_pacs}) as logf:
        if querying_pacs:
            failed_accessions = cmove_images(logf, **config)
            logging.info(f"Accessions that failed to process: {', '.join(failed_accessions)}")
        else:
            failed_accessions = []
    save_failed_accessions(failed_accessions)

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
        cmd = ["rclone", "copy", "--progress", "--config", "/rclone.conf", "input", f"azure:{container_name}/{site_id}/{project_name}"]
        logging.info(' '.join(cmd))
        with open(os.path.join("appdata", "log.txt"), "a") as logf:
            subprocess.run(cmd, text=True, stdout=logf, stderr=logf)
        logging.info("PROGRESS: COMPLETE")
    except Exception as e:
        error_and_exit(f"Error: {e}")

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
    # if config.get("module") not in ["imageqr", "imagedeid", "imageexport"]:
    #     error_and_exit("Module invalid or not implemented.")
    if not os.path.exists("input"):
        error_and_exit("Input directory not found.")
    if config.get("ctp_filters") is not None:
        validate_ctp_filters(config.get("ctp_filters"))
    if config.get("ctp_anonymizer") is not None:
        validate_ctp_anonymizer(config.get("ctp_anonymizer"))
    if config.get("module") in ["imageqr", "imagedeid"] and os.listdir("output") != []:
        error_and_exit("Output directory must be empty.")
    if os.path.exists(os.path.join("input", "input.xlsx")):
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
            validate_excel(os.path.join("input", "input.xlsx"), **config)
    elif config.get("module") in ["imagedeid", "imageexport"]:
        if count_dicom_files("input") == 0:
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

def generalmodule(**config):
    logging.info(f"Running module: {config.get('module')}")
    logging.info(f"Config: {config}")
    module = config.get("module")
    print(os.listdir("modules"))
    module_path = os.path.abspath(os.path.join("modules", f"{module}"))
    if not os.path.exists(module_path):
        error_and_exit(f"Module {module} not found.")

    module_cmd = [
        module_path,
        "config.yml",
        "input",
        "output",
        "appdata/log.txt"
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
    if config.get("module") in ["imageqr", "imagedeid", "imageexport", "headerextract"]:
        globals()[config.get("module")](**config)
    else:
        generalmodule(**config)

if __name__ == "__main__":  
    if not os.path.exists("config.yml"):
        error_and_exit("File config.yml not found.")
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    logging.basicConfig(filename=os.path.join("appdata", "log.txt"), level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    validate_config(config)
    run_module(**config)
