import os
import re
import sys
import yaml
import time
import signal
import logging
import requests
import subprocess

import pandas as pd

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
    <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="50001"
            calledAETTag="AIMINER"
            root="roots/DicomImportService"
            quarantine="quarantines/DicomImportService"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="../output/quarantine" />
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
            quarantine="../output/quarantine"/>
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
            root="../output/images"
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
    <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="50001"
            calledAETTag="AIMINER"
            root="roots/DicomImportService"
            quarantine="quarantines/DicomImportService"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="../output/quarantine" />
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

def print_and_log(message):
    logging.info(message)
    print(message)

def error_and_exit(error):
    print_and_log(error)
    sys.exit(1)

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

def count_dicom_files(path):
    return sum(f.endswith(".dcm") for _, _, files in os.walk(path) for f in files)

def run_progress(querying_pacs):
    received = ctp_get_status("Files received") if querying_pacs else count_dicom_files("input")
    quarantined = count_dicom_files(os.path.join("output", "quarantine"))
    saved = ctp_get_status("Files actually stored")
    stable = received == (quarantined + saved)
    return saved, quarantined, received, stable

def tick(tick_func, data):
    stable_for = 0
    while True:
        time.sleep(3)
        if tick_func is not None:
            tick_func()
        saved, quarantined, received, stable = run_progress(data["querying_pacs"])
        stable_for = stable_for + 1 if stable else 0
        if data["complete"] and stable_for > 3:
            break
        print_and_log(f"PROGRESS: {(saved + quarantined)}/{received} files")

def start_ctp_run(tick_func, tick_data, logf):
    ctp_process = subprocess.Popen(["java", "-jar", "Runner.jar"], cwd="ctp", stdout=logf, stderr=logf, text=True)
    tick_data = {"complete": False, "querying_pacs": True} | tick_data
    tick_thread = Thread(target=tick, args=(tick_func, tick_data,), daemon=True)
    tick_thread.start()
    return (ctp_process, tick_thread, tick_data)

def finish_ctp_run(ctp_process, tick_thread, tick_data):
    tick_data["complete"] = True
    tick_thread.join()
    ctp_process.send_signal(signal.SIGINT)

@contextmanager
def ctp_workspace(func, data):
    with open(os.path.join("output", "log.txt"), "a") as logf:
        try:
            process, thread, data = start_ctp_run(func, data, logf)
            time.sleep(3)
            yield logf
        finally:
            finish_ctp_run(process, thread, data)

def save_ctp_filters(ctp_filters):
    with open(os.path.join("ctp", "scripts", "dicom-filter.script"), "w") as f:
        f.write(ctp_filters if ctp_filters is not None else "true.")

def save_config(config):
    with open(os.path.join("ctp", "config.xml"), "w") as f:
        f.write(config)

def shiftf_date(dt, date_window):
    return (dt + timedelta(days=date_window)).strftime('%Y%m%d')

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
    for query in cmove_queries(**config):
        cmd = ["movescu", "-v", "-aet", "AIMINER", "-aec", 
            config.get("pacs_aet"), "-aem", "AIMINER", "-S"]+ query.split() + [
            config.get("pacs_ip"), str(config.get("pacs_port"))]
        logging.info(" ".join(cmd))
        process = subprocess.Popen(cmd, stdout=logf, stderr=logf, text=True)
        process.wait()

def imageqr_func():
    # TODO: Create the output.xlsx file based on the images stored so far.
    pass

def imageqr_main(**config):
    save_ctp_filters(config.get("ctp_filters"))
    save_config(IMAGEQR_CONFIG)
    with ctp_workspace(imageqr_func, {}) as logf:
        cmove_images(logf, **config)

def imagedeid_func():
    linker_csv = ctp_post("idmap", {"p": 0, "s": 4, "keytype": "originalAN", "keys": "", "format": "csv"})
    with open(os.path.join("output", "linker.csv"), "w") as f:
        f.write(linker_csv)
    # TODO: Create the output.xlsx file based on the images stored so far.

def imagedeid_main(**config):
    save_ctp_filters(config.get("ctp_filters"))
    querying_pacs = os.path.exists(os.path.join("input", "input.xlsx"))
    save_config(IMAGEDEID_PACS_CONFIG if querying_pacs else IMAGEDEID_LOCAL_CONFIG)
    with ctp_workspace(imagedeid_func, {"querying_pacs": querying_pacs}) as logf:
        if querying_pacs:
            cmove_images(logf, **config)

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
    if config.get("module") not in ["imageqr", "imagedeid"]:
        error_and_exit("Module invalid or not implemented.")
    if not os.path.exists("input"):
        error_and_exit("Input directory not found.")
    if config.get("ctp_filters") is not None:
        validate_ctp_filters(config.get("ctp_filters"))
    if os.listdir("output") != ["log.txt"]:
        error_and_exit("Output directory must be empty.")
    if os.path.exists(os.path.join("input", "input.xlsx")):
        if not all([config.get("pacs_ip"), config.get("pacs_port"), config.get("pacs_aet")]):
            error_and_exit("Pacs details missing in config file.")
        if config.get("acc_col") is None and (config.get("mrn_col") is None or config.get("date_col") is None):
            error_and_exit("Either the accession column name or mrn + date column names are required.")
        if config.get("acc_col") is not None and config.get("mrn_col") is not None and config.get("date_col") is not None:
            error_and_exit("Can only query using one of accession or mrn + date. Not both.")
        if config.get("date_window") is not None and not isinstance(config.get("date_window"), int):
            error_and_exit("Date window must be an integer.")
        validate_excel(os.path.join("input", "input.xlsx"), **config)
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
        acc_col (str): Accession number column name in excel file.            
        mrn_col (str): MRN column name in excel file.
        date_col (str): Date column name in excel file.
        date_window (int): Number of days to search around the date.
        ctp_filters (str): Filters for the query in the CTP format.
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
        acc_col (str): Accession number column name in excel file.            
        mrn_col (str): MRN column name in excel file.
        date_col (str): Date column name in excel file.
        date_window (int): Number of days to search around the date.
        ctp_filters (str): Filters for the query in the CTP format.
    """
    imagedeid_main(**config)

def run_module(**config):
    globals()[config.get("module")](**config)

if __name__ == "__main__":
    if not os.path.exists("config.yml"):
        error_and_exit("File config.yml not found.")
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    os.makedirs("output", exist_ok=True)
    logging.basicConfig(filename=os.path.join("output", "log.txt"), level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    validate_config(config)
    run_module(**config)