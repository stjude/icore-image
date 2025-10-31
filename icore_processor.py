import logging
import os
import platform
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
import warnings
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Thread

import pandas as pd
import requests
import yaml
from lark import Lark
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

warnings.filterwarnings('ignore')
os.environ.setdefault('TF_CPP_MIN_LOG_LEVEL', '3')

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
        <DirectoryStorageService
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

COMMON_DATE_FORMATS = [
    '%m/%d/%Y','%Y-%m-%d','%d/%m/%Y','%m-%d-%Y','%Y/%m/%d','%d-%m-%Y',
    '%m/%d/%y','%y-%m-%d','%d/%m/%y'
]


def get_dcmtk_binary(binary_name):
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        binary_path = os.path.join(bundle_dir, '_internal', 'dcmtk', 'bin', binary_name)
        return binary_path
    else:
        dcmtk_home = os.environ.get('DCMTK_HOME')
        return os.path.join(dcmtk_home, 'bin', binary_name)


def get_dcmtk_dict_path():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        return os.path.join(bundle_dir, '_internal', 'dcmtk', 'share', 'dcmtk-3.6.9', 'dicom.dic')
    else:
        dcmtk_home = os.environ.get('DCMTK_HOME')
        return os.path.join(dcmtk_home, 'share', 'dcmtk-3.6.9', 'dicom.dic')


def create_analyzer_engine():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        model_path = os.path.join(bundle_dir, '_internal', 'en_core_web_sm', 'en_core_web_sm-3.7.1')
        import spacy
        from presidio_analyzer.nlp_engine import SpacyNlpEngine
        nlp_engine = SpacyNlpEngine(models=[{"lang_code": "en", "model_name": model_path}])
    else:
        configuration = {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
        }
        provider = NlpEngineProvider(nlp_configuration=configuration)
        nlp_engine = provider.create_engine()
    
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
    
    mrn_recognizer = PatternRecognizer(
        supported_entity="MRN",
        name="mrn_recognizer",
        patterns=[
            Pattern(name="mrn_pattern", regex=r"\b(?!0{7,10}\b)\d{7,10}\b", score=0.5),
            Pattern(name="mrn_prefix_pattern", regex=r"\b[A-Z]{2,6}-\d{4,10}\b", score=0.7),
        ],
    )
    
    alphanumeric_id_recognizer = PatternRecognizer(
        supported_entity="ALPHANUMERIC_ID",
        name="alphanumeric_id_recognizer",
        patterns=[
            Pattern(name="date_id_pattern", regex=r"\b\d{4}-\d{2}-\d{2}\b", score=0.95),
        ],
    )
    
    title_name_recognizer = PatternRecognizer(
        supported_entity="PERSON",
        name="title_name_recognizer",
        patterns=[
            Pattern(
                name="dr_name_pattern",
                regex=r"(?<=Dr\.\s)([A-Z][A-Z]+)\b",
                score=0.85
            ),
            Pattern(
                name="dr_no_period_pattern",
                regex=r"(?<=Dr\s)([A-Z][A-Z]+)\b",
                score=0.85
            ),
        ],
    )
    
    last_name_recognizer = PatternRecognizer(
        supported_entity="PERSON",
        name="last_name_recognizer",
        patterns=[
            Pattern(
                name="patient_full_name_pattern",
                regex=r"(?<=Patient:\s)([A-Z][a-z]+\s+[A-Z]{2,})\b",
                score=0.85
            ),
        ],
    )
    
    ssn_recognizer = PatternRecognizer(
        supported_entity="US_SSN",
        name="ssn_recognizer",
        patterns=[
            Pattern(
                name="ssn_pattern",
                regex=r"\b\d{3}-\d{2}-\d{4}\b",
                score=0.95
            ),
        ],
    )
    
    analyzer.registry.add_recognizer(mrn_recognizer)
    analyzer.registry.add_recognizer(alphanumeric_id_recognizer)
    analyzer.registry.add_recognizer(title_name_recognizer)
    analyzer.registry.add_recognizer(last_name_recognizer)
    analyzer.registry.add_recognizer(ssn_recognizer)
    
    return analyzer


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
    for attempt in range(3):
        try:
            return requests.get(request_url, auth=("admin", "password")).text
        except Exception:
            if attempt < 2:
                time.sleep(3)
            else:
                raise

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
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        if platform.system() == 'Darwin':
            java_home = os.path.join(bundle_dir, '_internal', 'jre8', 'Contents', 'Home')
        else:
            java_home = os.path.join(bundle_dir, '_internal', 'jre8')
    else:
        java_home = os.environ.get('JAVA_HOME')
    
    java_executable = os.path.join(java_home, "bin", "java")
    env = {'JAVA_HOME': java_home}
    
    ctp_process = subprocess.Popen(
        [java_executable, "-Djava.awt.headless=true", "-Dapple.awt.UIElement=true", "-Xms2048m", "-Xmx16384m", "-jar", "libraries/CTP.jar"],
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

def generate_series_date_filter(config):
    input_path = os.path.join(INPUT_DIR, "input.xlsx")
    if not os.path.exists(input_path):
        return None
        
    df = pd.read_excel(input_path)
    
    if not config.get('date_col'):
        return None
    
    # Get patient-date pairs
    mrn_col = config.get('mrn_col')
    date_col = config.get('date_col')
    date_window = config.get('date_window', 0)
    
    # Group by patient and get their dates
    patient_date_pairs = df[[mrn_col, date_col]].drop_duplicates()
    
    # Create per-patient conditions with their specific date ranges
    patient_conditions = []
    for _, row in patient_date_pairs.iterrows():
        pid = str(row[mrn_col])
        target_date = row[date_col]
        
        # Calculate date range for this specific patient
        date_start = (target_date - timedelta(days=date_window)).strftime("%Y%m%d")
        date_end = (target_date + timedelta(days=date_window)).strftime("%Y%m%d")
        
        # Create date condition for this patient
        if date_window == 0:
            # If no window, just match the exact date
            date_condition = f'StudyDate.equals("{date_start}")'
        else:
            # If window, match start date OR end date OR anything in between
            date_condition = f'(StudyDate.equals("{date_start}") + StudyDate.equals("{date_end}") + (StudyDate.isGreaterThan("{date_start}") * StudyDate.isLessThan("{date_end}")))'
        
        # Combine patient ID with their specific date condition
        patient_conditions.append(f'(PatientID.equals("{pid}") * {date_condition})')
    
    # OR all patient conditions together
    filter_expr = " + ".join(patient_conditions)
    
    return filter_expr

def get_combined_ctp_filter(config):
    generated_filter = generate_series_date_filter(config)
    user_filter = config.get("ctp_filters")
    
    if generated_filter and user_filter:
        return f'({user_filter}) * ({generated_filter})'
    elif generated_filter:
        return generated_filter
    else:
        return user_filter

def cmove_queries(**config):
    df = pd.read_excel(os.path.join(INPUT_DIR, "input.xlsx"))
    queries = []
    accession_numbers = []  # Track all accession numbers
    logging.info(f"acc_col: {config.get('acc_col')}, mrn_col: {config.get('mrn_col')}")
    logging.info(f"Processing {len(df)} rows from input.xlsx")
    
    if config.get("acc_col") is not None:
        mrn_col = config.get("mrn_col")
        if mrn_col and mrn_col in df.columns:
            acc_mrn = list(df[[config.get("acc_col"), mrn_col]].itertuples(index=False, name=None))
            for i, (acc, mrn) in enumerate(acc_mrn, 1):
                query = f"-k QueryRetrieveLevel=STUDY -k AccessionNumber=*{str(acc)}* -k PatientID={str(mrn)}"
                queries.append(query)
                accession_numbers.append(str(acc))
                logging.info(f"Row {i}: Accession={acc}, MRN={mrn}")
                logging.info(f"  Query: {query}")
        else:
            acc_list = df[config.get("acc_col")].tolist()
            for i, acc in enumerate(acc_list, 1):
                query = f"-k QueryRetrieveLevel=STUDY -k AccessionNumber=*{str(acc)}*"
                queries.append(query)
                accession_numbers.append(str(acc))
                logging.info(f"Row {i}: Accession={acc}")
                logging.info(f"  Query: {query}")
    else:
        mrn_dates = list(df[[config.get("mrn_col"), config.get("date_col")]].itertuples(index=False, name=None))
        for i, (mrn, dt) in enumerate(mrn_dates, 1):
            dts = datetime.strftime((dt - timedelta(days=config.get("date_window"))), "%Y%m%d")
            dte = datetime.strftime((dt + timedelta(days=config.get("date_window"))), "%Y%m%d")
            query = f"-k QueryRetrieveLevel=STUDY -k PatientID={str(mrn)} -k StudyDate={dts}-{dte}"
            queries.append(query)
            logging.info(f"Row {i}: MRN={mrn}, TargetDate={dt.strftime('%Y-%m-%d')}, Window={config.get('date_window')} days")
            logging.info(f"  Date Range: {dts} to {dte}")
            logging.info(f"  Query: {query}")
    
    logging.info(f"Total queries generated: {len(queries)}")
    return queries, accession_numbers

def cmove_images(logf, **config):
    failed_accessions = []
    successful_rows = set()
    study_uids_rows = {}
    
    queries, accession_numbers = cmove_queries(**config)
    
    for pacs in config.get("pacs"):
        study_uids = set()
        ip, port, aec = pacs.get("ip"), pacs.get("port"), pacs.get("ae")
        aet, aem = config.get("application_aet"), config.get("application_aet")
        logging.info(f"Querying PACS: {ip}:{port} (AE: {aec})")
        
        queries, accession_numbers = cmove_queries(**config)
        for i, query in enumerate(queries):
            logging.info(f"\n{'='*80}")
            logging.info(f"FINDSCU Query {i+1}/{len(queries)}")
            logging.info(f"{'='*80}")
            cmd = [get_dcmtk_binary("findscu"), "-v", "-aet", aet, "-aec", aec, "-S"] + query.split() + ["-k", "StudyInstanceUID", ip, str(port)]
            logging.info(f"Command: {' '.join(cmd)}")
            env = os.environ.copy()
            env['DCMDICTPATH'] = get_dcmtk_dict_path()
            process = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            
            # Log full debug output
            logging.info("FINDSCU STDOUT:")
            logging.info(process.stdout)
            logging.info("FINDSCU STDERR:")
            logging.info(process.stderr)
            
            output = process.stderr
            studies_found_this_query = 0
            for entry in output.split("Find Response:")[1:]:
                tags = parse_dicom_tag_dict(entry)
                study_uid = tags.get("StudyInstanceUID")
                if study_uid and len(study_uid) > 0:
                    study_uids.add(study_uid)
                    study_uids_rows[study_uid] = i
                    studies_found_this_query += 1
                    study_date = tags.get("StudyDate", "N/A")
                    logging.info(f"  Found StudyInstanceUID: {study_uid}, StudyDate: {study_date}")
            
            if studies_found_this_query == 0:
                logging.info(f"  No studies found for this query")
            else:
                logging.info(f"  Total studies found for this query: {studies_found_this_query}")
            
            logging.info(f"Processed {i+1}/{len(queries)} query rows")

        logging.info(f"\n{'='*80}")
        logging.info(f"FINDSCU SUMMARY: Found {len(study_uids)} unique studies total from PACS {ip}:{port}")
        logging.info(f"{'='*80}\n")

        retry_count = 0
        current_moves = list(study_uids)
        
        while current_moves and retry_count < 3:
            if retry_count > 0:
                logging.info(f"Retry attempt {retry_count} for {len(current_moves)} failed moves")
                time.sleep(5)
                
            failed_moves = []
            for i, study_uid in enumerate(current_moves):
                logging.info(f"\n{'='*80}")
                logging.info(f"MOVESCU {i+1}/{len(current_moves)} (Retry {retry_count})")
                logging.info(f"{'='*80}")
                cmd = [get_dcmtk_binary("movescu"), "-v", "-aet", aet, "-aem", aem, "-aec", aec, "-S", "-k", "QueryRetrieveLevel=STUDY", "-k", f"StudyInstanceUID={study_uid}", ip, str(port)]
                logging.info(f"Command: {' '.join(cmd)}")
                logging.info(f"StudyInstanceUID: {study_uid}")
                
                env = os.environ.copy()
                env['DCMDICTPATH'] = get_dcmtk_dict_path()
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
                stdout, stderr = process.communicate()
                process.wait()
                
                # Log full debug output
                logging.info("MOVESCU STDOUT:")
                logging.info(stdout)
                logging.info("MOVESCU STDERR:")
                logging.info(stderr)
                
                success = "Received Final Move Response (Success)" in (stdout + stderr)
                if success:
                    successful_rows.add(study_uids_rows[study_uid])
                    logging.info(f"✓ SUCCESS: Study {study_uid} moved successfully")
                else:
                    failed_moves.append(study_uid)
                    logging.info(f"✗ FAILED: Study {study_uid} failed to move")
                
                logging.info(f"Progress: {i+1}/{len(current_moves)} studies processed in this batch")
            
            current_moves = failed_moves
            retry_count += 1

    failed_accessions = []
    for i in range(len(queries)):
        if i not in successful_rows:
            failed_accessions.append(i)
    
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
    linker_csv = ctp_post("idmap", {"p": 0, "s": 4, "keytype": "trialAN", "keys": "", "format": "csv"})
    with open(os.path.join(APPDATA_DIR, "linker.csv"), "w") as f:
        f.write(linker_csv)

def scrub(data, whitelist, blacklist):
    try:
        analyzer = create_analyzer_engine()
        anonymizer = AnonymizerEngine()
        
        medical_terms_deny_list = {
            'cardiomediastinal', 'ventricles', 'medullaris', 'conus', 'calvarium',
            'paraspinal', 'mediastinum', 'pleura', 'parenchyma', 'foramina',
            'mucosal', 'multiplanar', 'heterogeneously', 'schmorl',
            'md', 'pneumonia', 'pneumothorax', 'effusion', 'opacity', 
            'consolidation', 'calcification', 'abnormality', 'silhouette',
            'technique', 'ap', 'lateral', 'ct', 'mri', 'radiograph', 'examination',
            'copd', 'emg', 'npi', 'acr', 'lmp', 'afi',
            'hu', 'ed', 'npo', 'iv', 'or', 'er', 'icu', 'po', 'im', 'sc',
            'degrees', 'cm', 'mm', 'ml'
        }
        
        medical_person_deny_list = {
            'ventricles', 'mucosal', 'multiplanar', 'schmorl', 'medullaris', 'conus',
            'standard', 'g2p1', 'referring',
            'son', 'daughter', 'wife', 'husband', 'mother', 'father', 'parent',
            'pine', 'cedar', 'oak', 'maple',
            'diverticulosis', 'diverticulitis'
        }
        
        if whitelist:
            medical_terms_deny_list.update(whitelist)
            medical_person_deny_list.update(whitelist)
        
        for item in blacklist:
            blacklist_recognizer = PatternRecognizer(
                supported_entity="CUSTOM_BLACKLIST",
                name=f"blacklist_{hash(item)}",
                patterns=[Pattern(name=f"blacklist_pattern_{hash(item)}", regex=re.escape(item), score=0.95)],
            )
            analyzer.registry.add_recognizer(blacklist_recognizer)
        
        entities_to_detect = [
            "PERSON", "DATE_TIME", "MRN", "ALPHANUMERIC_ID", "PHONE_NUMBER",
            "EMAIL_ADDRESS", "LOCATION", "US_SSN", "MEDICAL_LICENSE",
            "US_DRIVER_LICENSE", "US_PASSPORT", "CREDIT_CARD", "US_ITIN",
            "NRP", "IBAN_CODE", "CUSTOM_BLACKLIST"
        ]
        
        alphanumeric_date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
        age_pattern = re.compile(r'\b\d{1,3}[-\s]year[-\s]old\b', re.IGNORECASE)
        duration_pattern = re.compile(r'\b\d{1,3}\s+(weeks?|months?|days?|hours?|minutes?|seconds?|mins?|secs?)\b', re.IGNORECASE)
        time_pattern = re.compile(r'\b\d{1,2}:\d{2}(\s*[AP]M)?\b', re.IGNORECASE)
        gestational_age_pattern = re.compile(r'\b\d{1,2}\s*weeks?\s*\d*\s*days?\b', re.IGNORECASE)
        time_reference_pattern = re.compile(r'\b(midnight|noon|morning|evening|afternoon)\b', re.IGNORECASE)
        complex_age_pattern = re.compile(r'\b\d{1,3}\s+years?,\s*\d{1,2}\s+(months?|days?)\s+old\b', re.IGNORECASE)
        
        operators = {
            "PERSON": OperatorConfig("replace", {"new_value": "[PERSONALNAME]"}),
            "DATE_TIME": OperatorConfig("replace", {"new_value": "[DATE]"}),
            "MRN": OperatorConfig("replace", {"new_value": "[MRN]"}),
            "ALPHANUMERIC_ID": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
            "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[PHONE]"}),
            "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[EMAIL]"}),
            "LOCATION": OperatorConfig("replace", {"new_value": "[LOCATION]"}),
            "US_SSN": OperatorConfig("replace", {"new_value": "[SSN]"}),
            "MEDICAL_LICENSE": OperatorConfig("replace", {"new_value": "[MEDICALID]"}),
            "US_DRIVER_LICENSE": OperatorConfig("replace", {"new_value": "[DRIVERSLICENSE]"}),
            "US_PASSPORT": OperatorConfig("replace", {"new_value": "[PASSPORT]"}),
            "CREDIT_CARD": OperatorConfig("replace", {"new_value": "[CREDITCARD]"}),
            "US_ITIN": OperatorConfig("replace", {"new_value": "[ITIN]"}),
            "NRP": OperatorConfig("replace", {"new_value": "[NRP]"}),
            "IBAN_CODE": OperatorConfig("replace", {"new_value": "[IBAN]"}),
            "CUSTOM_BLACKLIST": OperatorConfig("replace", {"new_value": "[REDACTED]"}),
        }
        
        results = []
        for i, text_item in enumerate(data):
            text = str(text_item) if text_item is not None else "Empty"
            text = ''.join(c for c in text if c in string.printable)
            
            results_analysis = analyzer.analyze(
                text=text,
                entities=entities_to_detect,
                language='en',
                score_threshold=0.5
            )
            
            filtered_results = []
            for result in results_analysis:
                detected_text = text[result.start:result.end]
                detected_lower = detected_text.lower()
                
                if result.entity_type == "LOCATION":
                    if detected_lower in medical_terms_deny_list:
                        continue
                
                if result.entity_type == "PERSON":
                    if detected_lower in medical_person_deny_list:
                        continue
                
                if result.entity_type == "DATE_TIME":
                    if alphanumeric_date_pattern.match(detected_text):
                        continue
                    if age_pattern.search(detected_text):
                        continue
                    if complex_age_pattern.search(detected_text):
                        continue
                    if duration_pattern.search(detected_text):
                        continue
                    if time_pattern.match(detected_text):
                        continue
                    if gestational_age_pattern.match(detected_text):
                        continue
                    if time_reference_pattern.search(detected_text):
                        continue
                
                filtered_results.append(result)
            
            filtered_results = sorted(filtered_results, key=lambda x: x.start)
            
            anonymized_result = anonymizer.anonymize(
                text=text,
                analyzer_results=filtered_results,
                operators=operators
            )
            
            results.append(anonymized_result.text)
            print_and_log(f"PROGRESS: {i+1}/{len(data)} rows de-identified")
        
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

def format_ctp_filter(filter_expr):
    """Format CTP filter expression for better readability"""
    if not filter_expr:
        return ""
    
    # Replace * with AND and + with OR
    formatted = filter_expr.replace(" * ", " AND ")
    formatted = formatted.replace(" + ", " OR ")
    
    # Split on OR that's between major patient conditions (not inside parens)
    # Find OR that separates PatientID conditions
    import re
    # Split by ") OR (" pattern which separates patient conditions
    parts = re.split(r'\) OR \((?=PatientID)', formatted)
    
    if len(parts) > 1:
        # Reconstruct with newlines between patient conditions
        result = []
        for i, part in enumerate(parts):
            if i == 0:
                result.append(part + ")")
            elif i == len(parts) - 1:
                result.append("\nOR (" + part)
            else:
                result.append("\nOR (" + part + ")")
        return ''.join(result)
    else:
        # No split needed, just return with operators replaced
        return formatted

def imageqr_func(_):
    save_metadata_csv()

def imageqr_main(**config):
    def setup_config(temp_ctp_dir):
        # Log filter generation details
        logging.info("\n" + "="*80)
        logging.info("CTP FILTER GENERATION")
        logging.info("="*80)
        
        generated_filter = generate_series_date_filter(config)
        user_filter = config.get("ctp_filters")
        combined_filter = get_combined_ctp_filter(config)
        
        if generated_filter:
            logging.info("Generated Series/Date Filter:")
            logging.info("Raw: " + generated_filter)
            logging.info("Formatted:")
            logging.info(format_ctp_filter(generated_filter))
        else:
            logging.info("No series/date filter generated (no input.xlsx with date columns)")
        
        if user_filter:
            logging.info("User-provided CTP Filter:")
            logging.info("Raw: " + user_filter)
            logging.info("Formatted:")
            logging.info(format_ctp_filter(user_filter))
        else:
            logging.info("No user-provided CTP filter")
        
        if combined_filter:
            logging.info("Combined CTP Filter (used by CTP):")
            logging.info("Raw: " + combined_filter)
            logging.info("Formatted:")
            logging.info(format_ctp_filter(combined_filter))
        else:
            logging.info("No CTP filter will be applied (all studies will pass)")
        
        save_ctp_filters(combined_filter, temp_ctp_dir)
        
        # Log the actual filter script that was saved
        logging.info("\n" + "="*80)
        logging.info("ACTUAL CTP FILTER SCRIPT SAVED TO: scripts/dicom-filter.script")
        logging.info("="*80)
        with open(os.path.join(temp_ctp_dir, "scripts", "dicom-filter.script"), "r") as f:
            logging.info(f.read())
        logging.info("="*80 + "\n")
        
        # Log CTP configuration
        logging.info("\n" + "="*80)
        logging.info("FINAL CTP CONFIG.XML (saved to temp directory)")
        logging.info("="*80)
        appdata_abs = os.path.abspath(APPDATA_DIR)
        output_abs = os.path.abspath(OUTPUT_DIR)
        formatted_config = IMAGEQR_CONFIG.format(appdata_dir=appdata_abs, output_dir=output_abs, application_aet=config.get("application_aet"))
        save_config(formatted_config, temp_ctp_dir)
        
        # Read back and log the actual saved config
        with open(os.path.join(temp_ctp_dir, "config.xml"), "r") as f:
            logging.info(f.read())
        logging.info("="*80 + "\n")
    
    with ctp_workspace(imageqr_func, {}, setup_config) as logf:
        failed_accessions = cmove_images(logf, **config)
        logging.info(f"Accessions that failed to process: {', '.join(map(str, failed_accessions))}")

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
        # Log filter generation details
        logging.info("\n" + "="*80)
        logging.info("CTP FILTER GENERATION")
        logging.info("="*80)
        
        generated_filter = generate_series_date_filter(config)
        user_filter = config.get("ctp_filters")
        combined_filter = get_combined_ctp_filter(config)
        
        if generated_filter:
            logging.info("Generated Series/Date Filter:")
            logging.info("Raw: " + generated_filter)
            logging.info("Formatted:")
            logging.info(format_ctp_filter(generated_filter))
        else:
            logging.info("No series/date filter generated (no input.xlsx with date columns)")
        
        if user_filter:
            logging.info("User-provided CTP Filter:")
            logging.info("Raw: " + user_filter)
            logging.info("Formatted:")
            logging.info(format_ctp_filter(user_filter))
        else:
            logging.info("No user-provided CTP filter")
        
        if combined_filter:
            logging.info("Combined CTP Filter (used by CTP):")
            logging.info("Raw: " + combined_filter)
            logging.info("Formatted:")
            logging.info(format_ctp_filter(combined_filter))
        else:
            logging.info("No CTP filter will be applied (all studies will pass)")
        
        save_ctp_filters(combined_filter, temp_ctp_dir)
        save_ctp_anonymizer(config.get("ctp_anonymizer"), temp_ctp_dir)
        save_ctp_lookup_table(config.get("ctp_lookup_table"), temp_ctp_dir)
        
        # Log the actual filter script that was saved
        logging.info("\n" + "="*80)
        logging.info("ACTUAL CTP FILTER SCRIPT SAVED TO: scripts/dicom-filter.script")
        logging.info("="*80)
        with open(os.path.join(temp_ctp_dir, "scripts", "dicom-filter.script"), "r") as f:
            logging.info(f.read())
        logging.info("="*80 + "\n")
        
        # Log CTP configuration
        logging.info("\n" + "="*80)
        logging.info("FINAL CTP CONFIG.XML (saved to temp directory)")
        logging.info("="*80)
        config_template = IMAGEDEID_PACS_CONFIG if querying_pacs else IMAGEDEID_LOCAL_CONFIG
        appdata_abs = os.path.abspath(APPDATA_DIR)
        output_abs = os.path.abspath(OUTPUT_DIR)
        input_abs = os.path.abspath(INPUT_DIR)
        formatted_config = config_template.format(appdata_dir=appdata_abs, output_dir=output_abs, input_dir=input_abs, application_aet=config.get("application_aet"))
        save_config(formatted_config, temp_ctp_dir)
        
        # Read back and log the actual saved config
        with open(os.path.join(temp_ctp_dir, "config.xml"), "r") as f:
            logging.info(f.read())
        logging.info("="*80 + "\n")
    
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
        # shifted_data = date_shift_text(original_data, deid_data, config.get("date_shift_by"))
        pd.DataFrame(deid_data).to_excel(os.path.join(OUTPUT_DIR, "output.xlsx"), index=False, header=False)
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
        if mrn_col and mrn_col in df.columns:
            if df[mrn_col].isnull().values.any():
                error_and_exit(f"Column {mrn_col} has empty values.")
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
    if not getattr(sys, 'frozen', False):
        if not os.environ.get('DCMTK_HOME'):
            error_and_exit("DCMTK_HOME environment variable is not set")
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
            if config.get("acc_col") is None and config.get("date_window") is None:
                error_and_exit("Date window is required when querying by MRN + date.")
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
    logging.info("="*80)
    logging.info("FULL CONFIG:")
    logging.info(yaml.dump(config, default_flow_style=False, sort_keys=False))
    logging.info("="*80)
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
        APPDATA_DIR = os.path.abspath(os.path.join(os.getcwd(), "appdata"))

    os.makedirs(APPDATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
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
