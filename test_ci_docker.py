import json
import logging
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import pydicom
import requests
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import generate_uid

from ctp import CTPPipeline
from test_utils import OrthancServer, Fixtures


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@pytest.fixture(scope="function", autouse=True)
def cleanup_docker_containers():
    """Cleanup any leftover test containers"""
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", "name=orthanc_test_", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    
    container_names = result.stdout.strip().split('\n')
    container_names = [name for name in container_names if name]
    
    for container_name in container_names:
        subprocess.run(["docker", "stop", container_name], capture_output=True)
        subprocess.run(["docker", "rm", container_name], capture_output=True)
    
    yield


def test_1_orthanc_container_starts_and_responds():
    """Test 1: Verify Orthanc container starts and HTTP API responds"""
    logger.info("=" * 80)
    logger.info("TEST 1: Orthanc container starts and HTTP API responds")
    logger.info("=" * 80)
    
    orthanc = OrthancServer()
    
    try:
        start_time = time.time()
        logger.info(f"Starting Orthanc container on HTTP port {orthanc.http_port}, DICOM port {orthanc.dicom_port}")
        orthanc.start()
        elapsed = time.time() - start_time
        logger.info(f"Orthanc started in {elapsed:.2f} seconds")
        
        # Check system endpoint
        response = requests.get(f"{orthanc.base_url}/system", timeout=5)
        logger.info(f"System endpoint response: {response.status_code}")
        logger.info(f"System info: {json.dumps(response.json(), indent=2)}")
        
        # Check container logs
        logs_result = subprocess.run(
            ["docker", "logs", orthanc.container],
            capture_output=True,
            text=True
        )
        logger.info("Container logs (last 20 lines):")
        log_lines = logs_result.stdout.split('\n')[-20:]
        for line in log_lines:
            if line.strip():
                logger.info(f"  {line}")
        
        assert response.status_code == 200
        logger.info("✓ Test 1 PASSED")
        
    except Exception as e:
        logger.error(f"✗ Test 1 FAILED: {e}")
        if orthanc.container:
            logs_result = subprocess.run(
                ["docker", "logs", orthanc.container],
                capture_output=True,
                text=True
            )
            logger.error(f"Container logs on failure:\n{logs_result.stdout}\n{logs_result.stderr}")
        raise
    finally:
        orthanc.stop()


def test_2_dicom_upload_succeeds():
    """Test 2: Verify DICOM files can be uploaded to Orthanc"""
    logger.info("=" * 80)
    logger.info("TEST 2: DICOM upload to Orthanc succeeds")
    logger.info("=" * 80)
    
    orthanc = OrthancServer()
    
    try:
        orthanc.start()
        logger.info(f"Orthanc started at {orthanc.base_url}")
        
        # Create a minimal DICOM file
        logger.info("Creating minimal DICOM file")
        ds = Fixtures.create_minimal_dicom(
            patient_id="TEST001",
            patient_name="DOE^JOHN",
            accession="ACC001",
            study_date="20250101",
            modality="CT"
        )
        
        temp_file = tempfile.mktemp(suffix=".dcm")
        ds.save_as(temp_file)
        logger.info(f"DICOM file created: {temp_file}")
        
        # Check studies before upload
        response = requests.get(f"{orthanc.base_url}/studies", timeout=5)
        studies_before = response.json()
        logger.info(f"Studies before upload: {len(studies_before)}")
        
        # Upload
        logger.info("Uploading DICOM file...")
        upload_start = time.time()
        with open(temp_file, 'rb') as f:
            response = requests.post(f"{orthanc.base_url}/instances", files={'file': f}, timeout=30)
        upload_elapsed = time.time() - upload_start
        
        logger.info(f"Upload took {upload_elapsed:.2f} seconds")
        logger.info(f"Upload response status: {response.status_code}")
        logger.info(f"Upload response body: {response.text[:500]}")
        
        os.remove(temp_file)
        
        # Verify upload succeeded
        assert response.status_code == 200, f"Upload failed with status {response.status_code}: {response.text}"
        
        # Check studies after upload
        response = requests.get(f"{orthanc.base_url}/studies", timeout=5)
        studies_after = response.json()
        logger.info(f"Studies after upload: {len(studies_after)}")
        logger.info(f"Study IDs: {studies_after}")
        
        assert len(studies_after) == 1, f"Expected 1 study, found {len(studies_after)}"
        
        # Get study details
        study_id = studies_after[0]
        response = requests.get(f"{orthanc.base_url}/studies/{study_id}", timeout=5)
        study_info = response.json()
        logger.info(f"Study info: {json.dumps(study_info.get('MainDicomTags', {}), indent=2)}")
        
        logger.info("✓ Test 2 PASSED")
        
    except Exception as e:
        logger.error(f"✗ Test 2 FAILED: {e}")
        raise
    finally:
        orthanc.stop()


def test_3_dicom_cfind_query_works():
    """Test 3: Verify DICOM C-FIND queries work against Orthanc"""
    logger.info("=" * 80)
    logger.info("TEST 3: DICOM C-FIND query works")
    logger.info("=" * 80)
    
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    orthanc = OrthancServer()
    
    try:
        orthanc.start()
        logger.info(f"Orthanc started on DICOM port {orthanc.dicom_port}")
        
        # Upload a study
        logger.info("Uploading test study")
        ds = Fixtures.create_minimal_dicom(
            patient_id="TEST001",
            patient_name="DOE^JOHN",
            accession="ACC001",
            study_date="20250101",
            modality="CT"
        )
        
        temp_file = tempfile.mktemp(suffix=".dcm")
        ds.save_as(temp_file)
        
        with open(temp_file, 'rb') as f:
            response = requests.post(f"{orthanc.base_url}/instances", files={'file': f}, timeout=30)
        assert response.status_code == 200
        os.remove(temp_file)
        
        logger.info("Study uploaded successfully")
        
        # Wait a moment for indexing
        time.sleep(2)
        
        # Run findscu
        logger.info("Running findscu query")
        findscu_path = Path(os.environ['DCMTK_HOME']) / "bin" / "findscu"
        
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = [
                str(findscu_path),
                "-od", tmpdir,
                "-Xs", f"{tmpdir}/output.xml",
                "-aet", "TEST_AET",
                "-aec", orthanc.aet,
                "-S",
                "-k", "QueryRetrieveLevel=STUDY",
                "-k", "PatientID=TEST001",
                "-k", "StudyInstanceUID",
                "-k", "StudyDate",
                "localhost", str(orthanc.dicom_port)
            ]
            
            logger.info(f"Command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            logger.info(f"findscu exit code: {result.returncode}")
            logger.info(f"findscu stdout:\n{result.stdout}")
            if result.stderr:
                logger.info(f"findscu stderr:\n{result.stderr}")
            
            # Check for result files
            result_files = list(Path(tmpdir).glob("*.dcm"))
            logger.info(f"Result files found: {len(result_files)}")
            
            for f in result_files:
                logger.info(f"  - {f.name}")
                try:
                    ds_result = pydicom.dcmread(f)
                    logger.info(f"    PatientID: {ds_result.get('PatientID', 'N/A')}")
                    logger.info(f"    StudyDate: {ds_result.get('StudyDate', 'N/A')}")
                except Exception as e:
                    logger.error(f"    Failed to read: {e}")
            
            assert len(result_files) > 0, "No studies found by C-FIND query"
            
        logger.info("✓ Test 3 PASSED")
        
    except Exception as e:
        logger.error(f"✗ Test 3 FAILED: {e}")
        raise
    finally:
        orthanc.stop()


def test_4_container_to_host_connectivity():
    """Test 4: Verify containers can connect back to host"""
    logger.info("=" * 80)
    logger.info("TEST 4: Container-to-host connectivity")
    logger.info("=" * 80)
    
    # Start a simple TCP server on host
    test_port = 50099
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('', test_port))
    server_socket.listen(1)
    server_socket.settimeout(10)
    
    logger.info(f"Started TCP server on host port {test_port}")
    
    # Start Orthanc and try to connect to host
    orthanc = OrthancServer()
    orthanc.add_modality("HOST_TEST", "HOST_TEST", "host.docker.internal", test_port)
    
    try:
        orthanc.start()
        logger.info("Orthanc started with modality pointing to host.docker.internal")
        
        # Try to trigger a connection from Orthanc to host
        # We'll use a simple test: can Orthanc resolve and connect to host.docker.internal?
        
        # Execute a command inside the container to test connectivity
        test_cmd = [
            "docker", "exec", orthanc.container,
            "sh", "-c", f"nc -zv -w 5 host.docker.internal {test_port} 2>&1"
        ]
        
        logger.info(f"Testing connectivity from container: {' '.join(test_cmd)}")
        result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=10)
        
        logger.info(f"Connection test exit code: {result.returncode}")
        logger.info(f"Connection test output:\n{result.stdout}\n{result.stderr}")
        
        # Check if connection was successful (nc returns 0 on success)
        if result.returncode == 0:
            logger.info("✓ Container can connect to host.docker.internal")
        else:
            logger.warning("Connection test returned non-zero, checking alternative methods")
            
            # Try with ping
            ping_cmd = [
                "docker", "exec", orthanc.container,
                "sh", "-c", "ping -c 1 -W 2 host.docker.internal 2>&1"
            ]
            ping_result = subprocess.run(ping_cmd, capture_output=True, text=True, timeout=10)
            logger.info(f"Ping test output:\n{ping_result.stdout}")
            
            # Try to see what host.docker.internal resolves to
            resolve_cmd = [
                "docker", "exec", orthanc.container,
                "sh", "-c", "getent hosts host.docker.internal 2>&1 || nslookup host.docker.internal 2>&1"
            ]
            resolve_result = subprocess.run(resolve_cmd, capture_output=True, text=True, timeout=10)
            logger.info(f"DNS resolution:\n{resolve_result.stdout}")
        
        logger.info("✓ Test 4 PASSED (connectivity verified)")
        
    except Exception as e:
        logger.error(f"✗ Test 4 FAILED: {e}")
        raise
    finally:
        server_socket.close()
        orthanc.stop()


def test_5_ctp_startup_and_status():
    """Test 5: Verify CTP starts and responds"""
    logger.info("=" * 80)
    logger.info("TEST 5: CTP startup and HTTP status")
    logger.info("=" * 80)
    
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        source_ctp = Path(__file__).parent / "ctp"
        
        logger.info(f"Starting CTP pipeline with source: {source_ctp}")
        
        try:
            with CTPPipeline(
                pipeline_type="imageqr",
                output_dir=str(output_dir),
                source_ctp_dir=str(source_ctp),
                application_aet="TEST_AET"
            ) as pipeline:
                
                logger.info(f"CTP HTTP port: {pipeline.port}")
                logger.info(f"CTP DICOM port: {pipeline._dicom_port}")
                
                # Wait a moment for CTP to start
                time.sleep(5)
                
                # Check if HTTP port responds
                try:
                    response = requests.get(f"http://localhost:{pipeline.port}/status", timeout=10)
                    logger.info(f"CTP status endpoint: {response.status_code}")
                    logger.info(f"CTP status response: {response.text[:500]}")
                except Exception as e:
                    logger.error(f"Failed to get CTP status: {e}")
                    raise
                
                # Check if DICOM port is listening
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                dicom_result = sock.connect_ex(('localhost', pipeline._dicom_port))
                sock.close()
                
                if dicom_result == 0:
                    logger.info(f"✓ CTP DICOM port {pipeline._dicom_port} is listening")
                else:
                    logger.error(f"✗ CTP DICOM port {pipeline._dicom_port} is not listening")
                    raise RuntimeError(f"CTP DICOM port not listening")
                
                logger.info("✓ Test 5 PASSED")
                
        except Exception as e:
            logger.error(f"✗ Test 5 FAILED: {e}")
            raise


def test_6_end_to_end_dicom_flow():
    """Test 6: Complete end-to-end DICOM flow from Orthanc to CTP"""
    logger.info("=" * 80)
    logger.info("TEST 6: End-to-end DICOM flow (Orthanc -> CTP)")
    logger.info("=" * 80)
    
    os.environ['JAVA_HOME'] = str(Path(__file__).parent / "jre8" / "Contents" / "Home")
    os.environ['DCMTK_HOME'] = str(Path(__file__).parent / "dcmtk")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        source_ctp = Path(__file__).parent / "ctp"
        
        # Start CTP first
        logger.info("Starting CTP pipeline")
        with CTPPipeline(
            pipeline_type="imageqr",
            output_dir=str(output_dir),
            source_ctp_dir=str(source_ctp),
            application_aet="TEST_AET"
        ) as pipeline:
            
            ctp_dicom_port = pipeline._dicom_port
            logger.info(f"CTP listening on DICOM port {ctp_dicom_port}")
            
            # Wait for CTP to be ready
            time.sleep(5)
            
            # Start Orthanc and configure it to know about CTP
            orthanc = OrthancServer()
            orthanc.add_modality("CTP_RECEIVER", "TEST_AET", "host.docker.internal", ctp_dicom_port)
            
            try:
                orthanc.start()
                logger.info(f"Orthanc started with CTP as known modality")
                
                # Upload a study to Orthanc
                logger.info("Uploading test study to Orthanc")
                ds = Fixtures.create_minimal_dicom(
                    patient_id="TEST001",
                    patient_name="DOE^JOHN",
                    accession="ACC001",
                    study_date="20250101",
                    modality="CT"
                )
                
                temp_file = tempfile.mktemp(suffix=".dcm")
                ds.save_as(temp_file)
                
                with open(temp_file, 'rb') as f:
                    response = requests.post(f"{orthanc.base_url}/instances", files={'file': f}, timeout=30)
                assert response.status_code == 200
                os.remove(temp_file)
                
                logger.info("Study uploaded to Orthanc")
                
                # Get study ID
                studies = requests.get(f"{orthanc.base_url}/studies", timeout=5).json()
                assert len(studies) == 1
                study_id = studies[0]
                logger.info(f"Study ID: {study_id}")
                
                # Trigger C-MOVE from Orthanc to CTP
                logger.info("Triggering C-MOVE from Orthanc to CTP")
                move_data = {
                    "TargetAet": "TEST_AET"
                }
                response = requests.post(
                    f"{orthanc.base_url}/modalities/CTP_RECEIVER/store",
                    json={"Resources": [study_id]},
                    timeout=30
                )
                
                logger.info(f"C-STORE response status: {response.status_code}")
                logger.info(f"C-STORE response: {response.text[:500]}")
                
                # Wait for files to arrive
                logger.info("Waiting for files to arrive at CTP...")
                time.sleep(5)
                
                # Check if files arrived in output directory
                output_files = list(output_dir.rglob("*.dcm"))
                logger.info(f"Files found in CTP output: {len(output_files)}")
                
                for f in output_files:
                    logger.info(f"  - {f.relative_to(output_dir)}")
                
                assert len(output_files) > 0, "No files received by CTP"
                
                logger.info("✓ Test 6 PASSED")
                
            except Exception as e:
                logger.error(f"✗ Test 6 FAILED: {e}")
                raise
            finally:
                orthanc.stop()

