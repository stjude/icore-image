import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from threading import Thread, Lock

import psutil
import requests


def _get_default_ctp_source_dir():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        source_ctp_dir = os.path.join(bundle_dir, '_internal', 'ctp')
    else:
        source_ctp_dir = "ctp"
    return source_ctp_dir


def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('localhost', port))
            return True
        except OSError:
            return False


def ctp_get(url, port, timeout=3):
    try:
        return requests.get(
            f"http://localhost:{port}/{url}",
            auth=("admin", "password"),
            timeout=timeout
        )
    except Exception:
        return None


def ctp_post(url, port, data, timeout=6):
    try:
        return requests.post(
            f"http://localhost:{port}/{url}",
            auth=("admin", "password"),
            data=data,
            headers={"Referer": f"http://localhost:{port}/{url}"},
            timeout=timeout
        )
    except Exception:
        return None


class CTPMetrics:
    def __init__(self):
        self.files_received = 0
        self.files_saved = 0
        self.files_quarantined = 0
        self.stable_count = 0
        self.last_change_time = time.time()
        self._lock = Lock()
    
    def is_stable(self):
        with self._lock:
            return self.files_received == (self.files_saved + self.files_quarantined)
    
    def update(self, received, saved, quarantined):
        with self._lock:
            metrics_changed = (
                received != self.files_received or
                saved != self.files_saved or
                quarantined != self.files_quarantined
            )
            
            if metrics_changed:
                self.last_change_time = time.time()
            
            self.files_received = received
            self.files_saved = saved
            self.files_quarantined = quarantined
            
            is_stable = self.files_received == (self.files_saved + self.files_quarantined)
            
            if is_stable:
                self.stable_count += 1
            else:
                self.stable_count = 0
    
    def time_since_last_change(self):
        with self._lock:
            return time.time() - self.last_change_time


class CTPServer:
    def __init__(self, ctp_dir, stall_timeout=300):
        self.ctp_dir = ctp_dir
        self.process = None
        self.monitor_thread = None
        self.metrics = CTPMetrics()
        self._running = False
        self._monitor_running = False
        self._monitor_exception = None
        self.stall_timeout = stall_timeout
        
        config_path = os.path.join(ctp_dir, "config.xml")
        tree = ET.parse(config_path)
        root = tree.getroot()
        
        server_elem = root.find("Server")
        self.port = int(server_elem.get("port", "50000"))
        
        quarantine_set = set()
        for elem in root.iter():
            quarantine = elem.get("quarantine")
            if quarantine:
                quarantine_set.add(quarantine)
        self.quarantine_dirs = list(quarantine_set)
    
    def start(self):
        self._cleanup_existing_server()
        
        java_home = os.environ.get('JAVA_HOME')
        if not java_home:
            raise RuntimeError("JAVA_HOME environment variable is not set")
        
        java_executable = os.path.join(java_home, "bin", "java")
        
        cmd = [
            java_executable,
            "-Djava.awt.headless=true",
            "-Dapple.awt.UIElement=true",
            "-Xms2048m",
            "-Xmx16384m",
            "-jar",
            "libraries/CTP.jar"
        ]
        
        env = {'JAVA_HOME': java_home}
        
        self.process = subprocess.Popen(
            cmd,
            cwd=self.ctp_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env
        )
        
        self._running = True
        
        time.sleep(3)
        
        if self.process.poll() is not None:
            raise RuntimeError("CTP process failed to start")
        
        self._monitor_running = True
        self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop(self):
        self._monitor_running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        if self.process:
            self._shutdown_process(self.process)
            self._running = False
    
    def is_complete(self):
        if self._monitor_exception:
            raise self._monitor_exception
        return self.metrics.stable_count > 3
    
    def _send_shutdown_request(self):
        try:
            requests.get(
                f"http://localhost:{self.port}/shutdown",
                headers={"servicemanager": "shutdown"},
                timeout=5
            )
        except Exception:
            pass
    
    def _shutdown_process(self, process):
        self._send_shutdown_request()
        if self._wait_for_process(process, 30):
            return
        
        process.send_signal(signal.SIGINT)
        if self._wait_for_process(process, 30):
            return
        
        process.terminate()
        if self._wait_for_process(process, 10):
            return
        
        process.kill()
        process.wait()
    
    def _wait_for_process(self, process, timeout):
        try:
            process.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            return False
    
    def _cleanup_existing_server(self):
        response = ctp_get("status", self.port, timeout=2)
        if response and response.status_code == 200:
            self._send_shutdown_request()
            time.sleep(3)
            
            check_response = ctp_get("status", self.port, timeout=2)
            if check_response and check_response.status_code == 200:
                self._force_kill_by_port()
    
    def _force_kill_by_port(self):
        for proc in psutil.process_iter(['pid', 'name', 'connections']):
            try:
                for conn in proc.connections():
                    if conn.laddr.port == self.port:
                        proc.kill()
                        proc.wait(timeout=5)
                        time.sleep(2)
                        return
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    
    def _update_metrics(self):
        response = ctp_get("status", self.port)
        if not response:
            return
        
        html = response.text
        
        saved_match = re.search(r"Files actually stored:\s*</td><td>(\d+)", html)
        saved = int(saved_match.group(1)) if saved_match else 0
        
        archive_received_match = re.search(r"Archive files supplied:\s*</td><td>(\d+)", html)
        dicom_received_match = re.search(r"Files received:\s*</td><td>(\d+)", html)
        
        received = 0
        if archive_received_match:
            received = int(archive_received_match.group(1))
        elif dicom_received_match:
            received = int(dicom_received_match.group(1))
        
        quarantined = 0
        exclude_files = {".", "..", "QuarantineIndex.db", "QuarantineIndex.lg"}
        
        for quarantine_dir in self.quarantine_dirs:
            if os.path.exists(quarantine_dir):
                for root, _, files in os.walk(quarantine_dir):
                    quarantined += len([f for f in files if not f.startswith('.') and f not in exclude_files])
        
        self.metrics.update(received, saved, quarantined)
    
    def _monitor_loop(self):
        try:
            while self._monitor_running:
                time.sleep(3)
                self._update_metrics()
                
                if self.metrics.time_since_last_change() > self.stall_timeout:
                    raise TimeoutError(f"CTP metrics have not changed for {self.stall_timeout} seconds")
        except Exception as e:
            self._monitor_exception = e


PIPELINE_TEMPLATES = {
    "imagecopy_local": """
    <Configuration>
        <Server maxThreads="20" port="{port}">
            <Log/>
        </Server>
        <Pipeline name="imagecopy">
            <ArchiveImportService
                class="org.rsna.ctp.stdstages.ArchiveImportService"
                name="ArchiveImportService"
                fsName="DICOM Image Directory"
                root="{tempdir}/roots/ArchiveImportService"
                treeRoot="{input_dir}"
                quarantine="{tempdir}/quarantine/ArchiveImportService"
                minAge="1000"
                acceptFileObjects="no"
                acceptXmlObjects="no"
                acceptZipObjects="no"
                expandTARs="no"/>
            <DirectoryStorageService
                class="org.rsna.ctp.stdstages.DirectoryStorageService"
                name="DirectoryStorageService"
                root="{output_dir}/"
                structure="{{StudyDate}}-{{Modality}}-{{PatientID}}/S{{SeriesNumber}}"
                setStandardExtensions="yes"
                acceptDuplicates="yes"
                returnStoredFile="yes"
                quarantine="{tempdir}/quarantine/DirectoryStorageService"
                whitespaceReplacement="_"/>
        </Pipeline>
    </Configuration>
    """,

    "imagedeid_local": """
    <Configuration>
        <Server maxThreads="20" port="{port}">
            <Log/>
        </Server>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="AuditLog" name="AuditLog"
                root="{tempdir}/roots/AuditLog"/>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="DeidAuditLog" name="DeidAuditLog"
                root="{tempdir}/roots/DeidAuditLog"/>
        <Pipeline name="imagedeid">
        <ArchiveImportService
            class="org.rsna.ctp.stdstages.ArchiveImportService"
            name="ArchiveImportService"
            fsName="DICOM Image Directory"
            root="{tempdir}/roots/ArchiveImportService"
            treeRoot="{input_dir}"
            quarantine="{tempdir}/quarantine/ArchiveImportService"
            minAge="1000"
            acceptFileObjects="no"
            acceptXmlObjects="no"
            acceptZipObjects="no"
            expandTARs="no"/>
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{tempdir}/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{tempdir}/quarantine/DicomFilter"/>
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            id="IDMap"
            name="IDMap"
            root="{tempdir}/roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="{tempdir}/roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="{tempdir}/quarantine/DicomAnonymizer" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}/"
            structure="{{StudyDate}}-{{Modality}}-{{PatientID}}/S{{SeriesNumber}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="{tempdir}/quarantine/DirectoryStorageService"
            whitespaceReplacement="_"/>
        </Pipeline>
    </Configuration>
    """,

    "imagedeid_pacs": """
    <Configuration>
        <Server maxThreads="20" port="{port}">
            <Log/>
        </Server>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="AuditLog" name="AuditLog"
                root="{tempdir}/roots/AuditLog"/>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="DeidAuditLog" name="DeidAuditLog"
                root="{tempdir}/roots/DeidAuditLog"/>
        <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="{dicom_port}"
            calledAETTag="{application_aet}"
            root="{tempdir}/roots/DicomImportService"
            quarantine="{tempdir}/quarantine"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{tempdir}/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{tempdir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            id="IDMap"
            name="IDMap"
            root="{tempdir}/roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="{tempdir}/roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="{tempdir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}"
            structure="{{StudyDate}}-{{Modality}}-{{PatientID}}/S{{SeriesNumber}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="{tempdir}/quarantine"
            whitespaceReplacement="_"/>
        </Pipeline>
    </Configuration>
    """,

    "imagedeid_local_pixel": """
    <Configuration>
        <Server maxThreads="20" port="{port}">
            <Log/>
        </Server>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="AuditLog" name="AuditLog"
                root="{tempdir}/roots/AuditLog"/>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="DeidAuditLog" name="DeidAuditLog"
                root="{tempdir}/roots/DeidAuditLog"/>
        <Pipeline name="imagedeid">
        <ArchiveImportService
            class="org.rsna.ctp.stdstages.ArchiveImportService"
            name="ArchiveImportService"
            fsName="DICOM Image Directory"
            root="{tempdir}/roots/ArchiveImportService"
            treeRoot="{input_dir}"
            quarantine="{tempdir}/quarantine/ArchiveImportService"
            minAge="1000"
            acceptFileObjects="no"
            acceptXmlObjects="no"
            acceptZipObjects="no"
            expandTARs="no"/>
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{tempdir}/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{tempdir}/quarantine/DicomFilter"/>
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DicomDecompressor
            class="org.rsna.ctp.stdstages.DicomDecompressor"
            name="DicomDecompressor"
            skipJPEGBaseline="yes"
            root="{tempdir}/roots/DicomDecompressor"
            script="scripts/DicomDecompressor.script"
            quarantine="{tempdir}/quarantine/DicomDecompressor"/>
        <DicomPixelAnonymizer
            name="DicomPixelAnonymizer"
            class="org.rsna.ctp.stdstages.DicomPixelAnonymizer"
            root="{tempdir}/roots/DicomPixelAnonymizer" 
            log="no"
            script="scripts/DicomPixelAnonymizer.script"
            setBurnedInAnnotation="no"
            test="no"
            quarantine="{tempdir}/quarantine/DicomPixelAnonymizer" />
        <DicomTranscoder
            name="DicomTranscoder"
            class="org.rsna.ctp.stdstages.DicomTranscoder"
            tsuid="1.2.840.10008.1.2.1"
            root="{tempdir}/roots/DicomTranscoder" 
            skipJPEGBaseline="yes"
            script="scripts/dicom-transcoder.script"
            quarantine="{tempdir}/quarantine/DicomTranscoder" />
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            id="IDMap"
            name="IDMap"
            root="{tempdir}/roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="{tempdir}/roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="{tempdir}/quarantine/DicomAnonymizer" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}/"
            structure="{{StudyDate}}-{{Modality}}-{{PatientID}}/S{{SeriesNumber}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="{tempdir}/quarantine/DirectoryStorageService"
            whitespaceReplacement="_"/>
        </Pipeline>
    </Configuration>
    """,

    "imagedeid_pacs_pixel": """
    <Configuration>
        <Server maxThreads="20" port="{port}">
            <Log/>
        </Server>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="AuditLog" name="AuditLog"
                root="{tempdir}/roots/AuditLog"/>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="DeidAuditLog" name="DeidAuditLog"
                root="{tempdir}/roots/DeidAuditLog"/>
        <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="{dicom_port}"
            calledAETTag="{application_aet}"
            root="{tempdir}/roots/DicomImportService"
            quarantine="{tempdir}/quarantine"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{tempdir}/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{tempdir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DicomDecompressor
            class="org.rsna.ctp.stdstages.DicomDecompressor"
            name="DicomDecompressor"
            skipJPEGBaseline="yes"
            root="{tempdir}/roots/DicomDecompressor"
            script="scripts/DicomDecompressor.script"
            quarantine="{tempdir}/quarantine"/>
        <DicomPixelAnonymizer
            name="DicomPixelAnonymizer"
            class="org.rsna.ctp.stdstages.DicomPixelAnonymizer"
            root="{tempdir}/roots/DicomPixelAnonymizer" 
            log="no"
            script="scripts/DicomPixelAnonymizer.script"
            setBurnedInAnnotation="no"
            test="no"
            quarantine="{tempdir}/quarantine/DicomPixelAnonymizer" />
        <DicomTranscoder
            name="DicomTranscoder"
            class="org.rsna.ctp.stdstages.DicomTranscoder"
            tsuid="1.2.840.10008.1.2.1"
            root="{tempdir}/roots/DicomTranscoder" 
            skipJPEGBaseline="yes"
            script="scripts/dicom-transcoder.script"
            quarantine="{tempdir}/quarantine/DicomTranscoder" />
        <IDMap
            class="org.rsna.ctp.stdstages.IDMap"
            id="IDMap"
            name="IDMap"
            root="{tempdir}/roots/IDMap" />
        <DicomAnonymizer
            class="org.rsna.ctp.stdstages.DicomAnonymizer"
            name="DicomAnonymizer"
            root="{tempdir}/roots/DicomAnonymizer"
            script="scripts/DicomAnonymizer.script"
            lookupTable="scripts/LookupTable.properties"
            quarantine="{tempdir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="DeidAuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}"
            structure="{{StudyDate}}-{{Modality}}-{{PatientID}}/S{{SeriesNumber}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="{tempdir}/quarantine"
            whitespaceReplacement="_"/>
        </Pipeline>
    </Configuration>
    """,

    "imageqr": """
    <Configuration>
        <Server maxThreads="20" port="{port}">
            <Log/>
        </Server>
        <Plugin class="org.rsna.ctp.stdplugins.AuditLog" id="AuditLog" name="AuditLog"
                root="{tempdir}/roots/AuditLog"/>
        <Pipeline name="imagedeid">
        <DicomImportService
            class="org.rsna.ctp.stdstages.DicomImportService"
            name="DicomImportService"
            port="{dicom_port}"
            calledAETTag="{application_aet}"
            root="{tempdir}/roots/DicomImportService"
            quarantine="{tempdir}/quarantine/DicomImportService"
            logConnections="no" />
        <DicomFilter
            class="org.rsna.ctp.stdstages.DicomFilter"
            name="DicomFilter"
            root="{tempdir}/roots/DicomFilter"
            script="scripts/dicom-filter.script"
            quarantine="{tempdir}/quarantine" />
        <DicomAuditLogger
            name="DicomAuditLogger"
            class="org.rsna.ctp.stdstages.DicomAuditLogger"
            root="{tempdir}/roots/DicomAuditLogger"
            auditLogID="AuditLog"
            auditLogTags="AccessionNumber;StudyInstanceUID;PatientName;PatientID;PatientSex;Manufacturer;ManufacturerModelName;StudyDescription;StudyDate;SeriesInstanceUID;SOPClassUID;Modality;SeriesDescription;Rows;Columns;InstitutionName;StudyTime"
            cacheID="ObjectCache"
            level="study" />
        <DirectoryStorageService
            class="org.rsna.ctp.stdstages.DirectoryStorageService"
            name="DirectoryStorageService"
            root="{output_dir}/images"
            structure="{{StudyDate}}-{{Modality}}-{{PatientID}}/S{{SeriesNumber}}"
            setStandardExtensions="yes"
            acceptDuplicates="yes"
            returnStoredFile="yes"
            quarantine="{tempdir}/quarantine/DirectoryStorageService"
            whitespaceReplacement="_"/>
        </Pipeline>
    </Configuration>
    """
}


class CTPPipeline:
    def __init__(self, pipeline_type, input_dir, output_dir,
                 filter_script=None, anonymizer_script=None, lookup_table=None,
                 application_aet=None, source_ctp_dir=None, stall_timeout=300):
        if pipeline_type not in PIPELINE_TEMPLATES:
            raise ValueError(f"Unknown pipeline_type: {pipeline_type}. Must be one of {list(PIPELINE_TEMPLATES.keys())}")
        
        self.source_ctp_dir = source_ctp_dir if source_ctp_dir is not None else _get_default_ctp_source_dir()
        self.pipeline_type = pipeline_type
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.filter_script = filter_script if filter_script is not None else "true."
        self.anonymizer_script = anonymizer_script
        self.lookup_table = lookup_table
        self.application_aet = application_aet
        self.stall_timeout = stall_timeout
        
        self.port = self._find_available_port()
        self._tempdir = tempfile.mkdtemp(prefix='ctp_')
        self._dicom_port = self.port + 1
        self.server = None
    
    def _find_available_port(self):
        start_port = 50000
        max_attempts = 10
        port_increment = 10
        
        for attempt in range(max_attempts):
            port = start_port + (attempt * port_increment)
            dicom_port = port + 1
            
            if is_port_available(port) and is_port_available(dicom_port):
                return port
        
        raise RuntimeError(f"Could not find available port after {max_attempts} attempts (tried ports {start_port} to {start_port + (max_attempts - 1) * port_increment})")
    
    def __enter__(self):
        os.makedirs(os.path.join(self._tempdir, "roots"), exist_ok=True)
        os.makedirs(os.path.join(self._tempdir, "quarantine"), exist_ok=True)
        
        ctp_workspace = os.path.join(self._tempdir, "ctp")
        source_ctp = self.source_ctp_dir
        
        for item in os.listdir(source_ctp):
            src_path = os.path.join(source_ctp, item)
            dst_path = os.path.join(ctp_workspace, item)
            if os.path.isdir(src_path):
                shutil.copytree(src_path, dst_path)
            else:
                os.makedirs(ctp_workspace, exist_ok=True)
                shutil.copy(src_path, dst_path)
        
        config_template = PIPELINE_TEMPLATES[self.pipeline_type]
        config_xml = config_template.format(
            input_dir=os.path.abspath(self.input_dir),
            output_dir=os.path.abspath(self.output_dir),
            tempdir=os.path.abspath(self._tempdir),
            port=self.port,
            dicom_port=self._dicom_port,
            application_aet=self.application_aet if self.application_aet else ""
        )
        
        with open(os.path.join(ctp_workspace, "config.xml"), "w") as f:
            f.write(config_xml)
        
        scripts_dir = os.path.join(ctp_workspace, "scripts")
        os.makedirs(scripts_dir, exist_ok=True)
        
        with open(os.path.join(scripts_dir, "dicom-filter.script"), "w") as f:
            f.write(self.filter_script)
        
        if self.anonymizer_script:
            with open(os.path.join(scripts_dir, "DicomAnonymizer.script"), "w") as f:
                f.write(self.anonymizer_script)
        
        if self.lookup_table:
            with open(os.path.join(scripts_dir, "LookupTable.properties"), "w") as f:
                f.write(self.lookup_table)
        else:
            with open(os.path.join(scripts_dir, "LookupTable.properties"), "w") as f:
                f.write("")
        
        self.server = CTPServer(ctp_workspace, stall_timeout=self.stall_timeout)
        self.server.start()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.server:
            self.server.stop()
        
        shutil.rmtree(self._tempdir, ignore_errors=True)
        
        return False
    
    def is_complete(self):
        return self.server.is_complete() if self.server else False
    
    @property
    def metrics(self):
        return self.server.metrics if self.server else None
    
    def get_audit_log_csv(self, plugin_id):
        response = ctp_get(f"{plugin_id}?export&csv&suppress", self.port)
        return response.text if response else None
    
    def get_idmap_csv(self, stage_id="IDMap"):
        stage_index = self._find_stage_index_by_id(stage_id)
        if stage_index is None:
            return None
        response = ctp_post("idmap", self.port, 
                           {"p": 0, "s": stage_index, "keytype": "trialAN", 
                            "keys": "", "format": "csv"})
        return response.text if response else None
    
    def _find_stage_index_by_id(self, stage_id):
        config_path = os.path.join(self.server.ctp_dir, "config.xml")
        tree = ET.parse(config_path)
        root = tree.getroot()
        
        pipeline = root.find("Pipeline")
        if pipeline is None:
            return None
        
        stage_index = 0
        for child in pipeline:
            if child.tag in ["Plugin", "Server"]:
                continue
            
            if child.get("id") == stage_id:
                return stage_index
            
            stage_index += 1
        
        return None