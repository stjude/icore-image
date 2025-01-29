import os
import re
import sys
import yaml
import time
import signal
import string
import logging
import requests
import tempfile
import subprocess

import pandas as pd
import xml.etree.ElementTree as ET

from lark import Lark
from threading import Thread
from contextlib import contextmanager
from datetime import datetime, timedelta

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
            quarantine="../output/appdata/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="../output/images"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
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
            quarantine="../output/appdata/quarantine"/>
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
            quarantine="quarantines/DicomAnonymizer" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="../output/deidentified"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
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
            quarantine="../output/appdata/quarantine" />
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
            quarantine="quarantines/DicomAnonymizer" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="../output/deidentified"
            structure="{{StudyInstanceUID}}/{{SeriesInstanceUID}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="quarantines/DirectoryStorageService"
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
    quarantined = count_files(os.path.join("output", "appdata", "quarantine"), {".", "..", "QuarantineIndex.db", "QuarantineIndex.lg"})
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
        print_and_log(f"PROGRESS: {(saved + quarantined)}/{received} files")
    print_and_log("PROGRESS: COMPLETE")

def start_ctp_run(tick_func, tick_data, logf):
    ctp_process = subprocess.Popen(["java", "-jar", "Runner.jar"], cwd="ctp", stdout=logf, stderr=logf, text=True)
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
    with open(os.path.join("output", "appdata", "log.txt"), "a") as logf:
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

def save_config(config):
    with open(os.path.join("ctp", "config.xml"), "w") as f:
        f.write(config)

def shiftf_date(dt, date_window):
    return (dt + timedelta(days=date_window)).strftime('%Y%m%d')

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
    if config.get("acc_col") is not None:
        for acc in df[config.get("acc_col")]:
            queries.append(f"-k QueryRetrieveLevel=STUDY -k AccessionNumber={str(acc)}")
    else:
        mrn_dates = list(df[[config.get("mrn_col"), config.get("date_col")]].itertuples(index=False, name=None))
        for mrn, dt in mrn_dates:
            dts = datetime.strftime((dt - timedelta(days=config.get("date_window"))), "%Y%m%d")
            dte = datetime.strftime((dt + timedelta(days=config.get("date_window"))), "%Y%m%d")
            queries.append(f"-k QueryRetrieveLevel=STUDY -k PatientID={str(mrn)} -k StudyDate={dts}-{dte}")
    return queries

def cmove_images(logf, **config):
    study_uids = set()
    ip, aet, aec, aem, port = config.get("pacs_ip"), config.get("application_aet"), config.get("pacs_aet"), config.get("application_aet"), config.get("pacs_port")
    for query in cmove_queries(**config):
        cmd = ["findscu", "-v", "-aet", aet, "-aec", aec, "-S"] + query.split() + ["-k", "StudyInstanceUID",ip, str(port)]
        logging.info(" ".join(cmd))
        process = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = process.stderr
        for entry in output.split("Find Response:")[1:]:
            study_uids.add(parse_dicom_tag_dict(entry).get("StudyInstanceUID"))
    logging.info(f"Found {len(study_uids)} studies")
    for study_uid in study_uids:
        cmd = ["movescu", "-v", "-aet", aet, "-aem", aem, "-aec", aec, "-S", "-k", "QueryRetrieveLevel=STUDY", "-k", f"StudyInstanceUID={study_uid}", ip, str(port)]
        logging.info(" ".join(cmd))
        process = subprocess.Popen(cmd, stdout=logf, stderr=logf, text=True)
        process.wait()

def save_metadata_csv():
    metadata_csv = ctp_get("AuditLog?export&csv&suppress")
    with open(os.path.join("output", "appdata", "metadata.csv"), "w") as f:
        f.write(metadata_csv)

def save_linker_csv():
    linker_csv = ctp_post("idmap", {"p": 0, "s": 5, "keytype": "trialAN", "keys": "", "format": "csv"})
    with open(os.path.join("output", "appdata", "linker.csv"), "w") as f:
        f.write(linker_csv)

def imageqr_func(_):
    save_metadata_csv()

def imageqr_main(**config):
    save_ctp_filters(config.get("ctp_filters"))
    save_config(IMAGEQR_CONFIG)
    with ctp_workspace(imageqr_func, {}) as logf:
        cmove_images(logf, **config)

def imagedeid_func(_):
    save_metadata_csv()
    save_linker_csv()

def imagedeid_main(**config):
    save_ctp_filters(config.get("ctp_filters"))
    save_ctp_anonymizer(config.get("ctp_anonymizer"))
    querying_pacs = os.path.exists(os.path.join("input", "input.xlsx"))
    config_template = IMAGEDEID_PACS_CONFIG if querying_pacs else IMAGEDEID_LOCAL_CONFIG
    formatted_config = config_template.format(application_aet=config.get("application_aet"))
    save_config(formatted_config)
    with ctp_workspace(imagedeid_func, {"querying_pacs": querying_pacs}) as logf:
        if querying_pacs:
            cmove_images(logf, **config)

def write(path, data):
    with open(path, "w") as f:
        f.write(data)

def scrub(data, whitelist, blacklist):
    with tempfile.TemporaryDirectory() as temp_dir:
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
        subprocess.run(["./scrubber.19.0403.lnx", f"{temp_dir}/config.txt"], 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        results = []
        for i in range(len(data)):
            with open(f"{temp_dir}/output/{i}.nphi.txt") as f:
                scrubbed = f.read().split("##### DOCUMENT #")[0].strip()
                results.append(scrubbed)
            print_and_log(f"PROGRESS: {i}/{len(data)} rows de-identified")
        return results
    
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

def textdeid_main(**config):
    try:
        df = pd.read_excel(os.path.join("input", "input.xlsx"), header=None)
        original_data = df.iloc[:,0].tolist()
        deid_data = scrub(original_data, config.get("to_keep_list"), config.get("to_remove_list"))
        shifted_data = date_shift_text(original_data, deid_data, config.get("date_shift_by"))
        pd.DataFrame(shifted_data).to_excel(os.path.join("output", "output.xlsx"), index=False, header=False)
        print_and_log("PROGRESS: COMPLETE")
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
    if config.get("module") not in ["imageqr", "imagedeid", "textdeid"]:
        error_and_exit("Module invalid or not implemented.")
    if not os.path.exists("input"):
        error_and_exit("Input directory not found.")
    if config.get("ctp_filters") is not None:
        validate_ctp_filters(config.get("ctp_filters"))
    if config.get("ctp_anonymizer") is not None:
        validate_ctp_anonymizer(config.get("ctp_anonymizer"))
    if os.listdir("output") != ["appdata"]:
        error_and_exit("Output directory must be empty.")
    if os.path.exists(os.path.join("input", "input.xlsx")):
        if config.get("module") in ["imageqr", "imagedeid"]:
            if not all([config.get("pacs_ip"), config.get("pacs_port"), config.get("pacs_aet")]):
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
        else:
            if config.get("to_keep_list") is None or config.get("to_remove_list") is None:
                error_and_exit("to_keep_list and to_remove_list must be provided.")
            if config.get("date_shift_by") is None:
                error_and_exit("Date shift by must be provided.")
    elif config.get("module") in ["imagedeid"]:
        if count_dicom_files("input") == 0:
            error_and_exit("No DICOM files found in input directory.")
    else:
        error_and_exit("Input directory must contain input.xlsx file.")

def imageqr(**config):
    """
    Download DICOM images from PACS. The input directory must contain
    an input.xlsx file with accession numbers or MRN and date columns.

    Keyword Args:
        pacs_ip (str): IP address of the PACS.
        pacs_port (int): Port of the PACS.
        pacs_aet (str): AE title of the PACS.
        application_aet (str): AE title of the calling application.
        acc_col (str): Accession number column name in excel file.            
        mrn_col (str): MRN column name in excel file.
        date_col (str): Date column name in excel file.
        date_window (int): Number of days to search around the date.
        ctp_filters (str): Filters for the query in the CTP format.
        ctp_anonymizer (str): Anonymization script in the CTP xml format.
    """
    imageqr_main(**config)

def imagedeid(**config):
    """
    Deidentify DICOM images from local directory or PACS. If input
    directory contains input.xlsx file, the PACS will be queried.
    If input directory doesn't contain input.xlsx file, all DICOM
    files in the input directory will be deidentified.

    Keyword Args:
        pacs_ip (str): IP address of the PACS.
        pacs_port (int): Port of the PACS.
        pacs_aet (str): AE title of the PACS.
        application_aet (str): AE title of the calling application.
        acc_col (str): Accession number column name in excel file.            
        mrn_col (str): MRN column name in excel file.
        date_col (str): Date column name in excel file.
        date_window (int): Number of days to search around the date.
        ctp_filters (str): Filters for the query in the CTP format.
        ctp_anonymizer (str): Anonymization script in the CTP xml format.
    """
    imagedeid_main(**config)

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

def run_module(**config):
    logging.info(f"Running module: {config.get('module')}")
    logging.info(f"Config: {config}")
    globals()[config.get("module")](**config)

if __name__ == "__main__":
    if not os.path.exists("config.yml"):
        error_and_exit("File config.yml not found.")
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    os.makedirs(os.path.join("output", "appdata"), exist_ok=True)
    logging.basicConfig(filename=os.path.join("output", "appdata", "log.txt"), level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    validate_config(config)
    run_module(**config)