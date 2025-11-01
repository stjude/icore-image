import os
import re
import signal
import subprocess
import time
import xml.etree.ElementTree as ET
from threading import Thread, Lock

import psutil
import requests


def ctp_get(url, port, timeout=3):
    try:
        return requests.get(
            f"http://localhost:{port}/{url}",
            auth=("admin", "password"),
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
        self._lock = Lock()
    
    def is_stable(self):
        with self._lock:
            return self.files_received == (self.files_saved + self.files_quarantined)
    
    def update(self, received, saved, quarantined):
        with self._lock:
            self.files_received = received
            self.files_saved = saved
            self.files_quarantined = quarantined
            
            is_stable = self.files_received == (self.files_saved + self.files_quarantined)
            
            if is_stable:
                self.stable_count += 1
            else:
                self.stable_count = 0


class CTPServer:
    def __init__(self, ctp_dir):
        self.ctp_dir = ctp_dir
        self.process = None
        self.monitor_thread = None
        self.metrics = CTPMetrics()
        self._running = False
        self._monitor_running = False
        
        config_path = os.path.join(ctp_dir, "config.xml")
        tree = ET.parse(config_path)
        root = tree.getroot()
        
        server_elem = root.find("Server")
        self.port = int(server_elem.get("port", "50000"))
        
        self.quarantine_dirs = []
        for elem in root.iter():
            quarantine = elem.get("quarantine")
            if quarantine:
                self.quarantine_dirs.append(quarantine)
    
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
        
        received_match = re.search(r"Archive files supplied:\s*</td><td>(\d+)", html)
        received = int(received_match.group(1)) if received_match else 0
        
        quarantined = 0
        exclude_files = {".", "..", "QuarantineIndex.db", "QuarantineIndex.lg"}
        
        for quarantine_dir in self.quarantine_dirs:
            if os.path.exists(quarantine_dir):
                for root, _, files in os.walk(quarantine_dir):
                    quarantined += len([f for f in files if not f.startswith('.') and f not in exclude_files])
        
        self.metrics.update(received, saved, quarantined)
    
    def _monitor_loop(self):
        while self._monitor_running:
            time.sleep(3)
            self._update_metrics()
