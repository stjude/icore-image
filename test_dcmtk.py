import os
from unittest import mock
import pytest
import time

from dcmtk import find_studies, get_study, echo_pacs, DCMTKCommandError, DCMTKParseError

FINDSCU_SINGLE_ACCESSION_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<responses type="C-FIND">
<data-set xfer="1.2.840.10008.1.2.1" name="Little Endian Explicit">
<element tag="0008,0005" vr="CS" vm="1" len="10" name="SpecificCharacterSet">ISO_IR 192</element>
<element tag="0008,0050" vr="SH" vm="1" len="8" name="AccessionNumber">ACC12345</element>
<element tag="0008,0052" vr="CS" vm="1" len="6" name="QueryRetrieveLevel">STUDY</element>
<element tag="0008,0054" vr="AE" vm="1" len="12" name="RetrieveAETitle">ORTHANC_TEST</element>
<element tag="0020,000d" vr="UI" vm="1" len="64" name="StudyInstanceUID">1.2.826.0.1.3680043.8.498.19219017759098709637263425563099910928</element>
</data-set>
</responses>'''

FINDSCU_NO_RESULTS_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<responses type="C-FIND">
</responses>'''

FINDSCU_MULTIPLE_PATIENT_DATE_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<responses type="C-FIND">
<data-set xfer="1.2.840.10008.1.2.1" name="Little Endian Explicit">
<element tag="0008,0005" vr="CS" vm="1" len="10" name="SpecificCharacterSet">ISO_IR 192</element>
<element tag="0008,0020" vr="DA" vm="1" len="8" name="StudyDate">20240120</element>
<element tag="0008,0052" vr="CS" vm="1" len="6" name="QueryRetrieveLevel">STUDY</element>
<element tag="0008,0054" vr="AE" vm="1" len="12" name="RetrieveAETitle">ORTHANC_TEST</element>
<element tag="0010,0020" vr="LO" vm="1" len="6" name="PatientID">PAT001</element>
<element tag="0020,000d" vr="UI" vm="1" len="64" name="StudyInstanceUID">1.2.826.0.1.3680043.8.498.69882352604142477905047235985890195048</element>
</data-set>
<data-set xfer="1.2.840.10008.1.2.1" name="Little Endian Explicit">
<element tag="0008,0005" vr="CS" vm="1" len="10" name="SpecificCharacterSet">ISO_IR 192</element>
<element tag="0008,0020" vr="DA" vm="1" len="8" name="StudyDate">20240115</element>
<element tag="0008,0052" vr="CS" vm="1" len="6" name="QueryRetrieveLevel">STUDY</element>
<element tag="0008,0054" vr="AE" vm="1" len="12" name="RetrieveAETitle">ORTHANC_TEST</element>
<element tag="0010,0020" vr="LO" vm="1" len="6" name="PatientID">PAT001</element>
<element tag="0020,000d" vr="UI" vm="1" len="64" name="StudyInstanceUID">1.2.826.0.1.3680043.8.498.19219017759098709637263425563099910928</element>
</data-set>
</responses>'''

FINDSCU_PATIENT_EXACT_DATE_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<responses type="C-FIND">
<data-set xfer="1.2.840.10008.1.2.1" name="Little Endian Explicit">
<element tag="0008,0005" vr="CS" vm="1" len="10" name="SpecificCharacterSet">ISO_IR 192</element>
<element tag="0008,0020" vr="DA" vm="1" len="8" name="StudyDate">20240115</element>
<element tag="0008,0052" vr="CS" vm="1" len="6" name="QueryRetrieveLevel">STUDY</element>
<element tag="0008,0054" vr="AE" vm="1" len="12" name="RetrieveAETitle">ORTHANC_TEST</element>
<element tag="0010,0020" vr="LO" vm="1" len="6" name="PatientID">PAT001</element>
<element tag="0020,000d" vr="UI" vm="1" len="64" name="StudyInstanceUID">1.2.826.0.1.3680043.8.498.19219017759098709637263425563099910928</element>
</data-set>
</responses>'''

FINDSCU_SERIES_LEVEL_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<responses type="C-FIND">
<data-set xfer="1.2.840.10008.1.2.1" name="Little Endian Explicit">
<element tag="0008,0005" vr="CS" vm="1" len="10" name="SpecificCharacterSet">ISO_IR 192</element>
<element tag="0008,0052" vr="CS" vm="1" len="6" name="QueryRetrieveLevel">SERIES</element>
<element tag="0008,0054" vr="AE" vm="1" len="12" name="RetrieveAETitle">ORTHANC_TEST</element>
<element tag="0020,000d" vr="UI" vm="1" len="64" name="StudyInstanceUID">1.2.826.0.1.3680043.8.498.19219017759098709637263425563099910928</element>
<element tag="0020,000e" vr="UI" vm="1" len="64" name="SeriesInstanceUID">1.2.826.0.1.3680043.8.498.38702350352998616920101262660579778889</element>
</data-set>
</responses>'''

GETSCU_SUCCESS_STDERR = '''I: Requesting Association
I: Association Accepted (Max Send PDV: 16372)
I: Sending Get Request (MsgID 1)
I: Request Identifiers:
I:
I: # Dicom-Data-Set
I: # Used TransferSyntax: Little Endian Explicit
I: (0008,0052) CS [STUDY]                                  #   6, 1 QueryRetrieveLevel
I: (0020,000d) UI [1.2.826.0.1.3680043.8.498.12345]       #  64, 1 StudyInstanceUID
I:
I: Get Response 1 (Pending)
I: Sub-Operations Remaining: 10, Completed: 0, Failed: 0, Warning: 0
I: Get Response 2 (Pending)
I: Sub-Operations Remaining: 5, Completed: 5, Failed: 0, Warning: 0
I: Received Final Get Response (Success)
I: Sub-Operations Complete: 10, Failed: 0, Warning: 0
I: Releasing Association'''

GETSCU_FAILURE_STDERR = '''I: Requesting Association
I: Association Accepted (Max Send PDV: 16372)
I: Sending Get Request (MsgID 1)
I: Request Identifiers:
I:
I: # Dicom-Data-Set
I: # Used TransferSyntax: Little Endian Explicit
I: (0008,0052) CS [STUDY]                                  #   6, 1 QueryRetrieveLevel
I: (0020,000d) UI [9.9.9.9.9.9.9.9]                        #  16, 1 StudyInstanceUID
I:
W: Get response with error status (Failed: UnableToProcess)
I: Received Final Get Response (Failed: UnableToProcess)
I: Releasing Association'''


def test_find_studies_single_result(tmp_path):
    def mock_run(*args, **kwargs):
        xml_path = None
        for i, arg in enumerate(args[0]):
            if arg == "-Xs":
                xml_path = args[0][i + 1]
                break
        
        with open(xml_path, 'w') as f:
            f.write(FINDSCU_SINGLE_ACCESSION_XML)
        
        return mock.Mock(returncode=0, stdout="", stderr="")
    
    with mock.patch('tempfile.NamedTemporaryFile') as mock_temp:
        mock_temp.return_value.__enter__.return_value.name = str(tmp_path / "response.xml")
        with mock.patch('subprocess.run', side_effect=mock_run):
            with mock.patch('time.sleep'):
                results = find_studies(
                    host="localhost",
                    port=11112,
                    calling_aet="TEST_SCU",
                    called_aet="ORTHANC_TEST",
                    query_params={"AccessionNumber": "ACC12345"},
                )
    
    assert len(results) == 1
    assert results[0]["AccessionNumber"] == "ACC12345"
    assert results[0]["StudyInstanceUID"] == "1.2.826.0.1.3680043.8.498.19219017759098709637263425563099910928"
    assert results[0]["QueryRetrieveLevel"] == "STUDY"


def test_find_studies_multiple_results(tmp_path):
    def mock_run(*args, **kwargs):
        xml_path = None
        for i, arg in enumerate(args[0]):
            if arg == "-Xs":
                xml_path = args[0][i + 1]
                break
        
        with open(xml_path, 'w') as f:
            f.write(FINDSCU_MULTIPLE_PATIENT_DATE_XML)
        
        return mock.Mock(returncode=0, stdout="", stderr="")
    
    with mock.patch('tempfile.NamedTemporaryFile') as mock_temp:
        mock_temp.return_value.__enter__.return_value.name = str(tmp_path / "response.xml")
        with mock.patch('subprocess.run', side_effect=mock_run):
            with mock.patch('time.sleep'):
                results = find_studies(
                    host="localhost",
                    port=11112,
                    calling_aet="TEST_SCU",
                    called_aet="ORTHANC_TEST",
                    query_params={"PatientID": "PAT001", "StudyDate": "20240101-20240131"},
                )
    
    assert len(results) == 2
    assert results[0]["PatientID"] == "PAT001"
    assert results[0]["StudyDate"] == "20240120"
    assert results[1]["StudyDate"] == "20240115"


def test_find_studies_no_results(tmp_path):
    def mock_run(*args, **kwargs):
        xml_path = None
        for i, arg in enumerate(args[0]):
            if arg == "-Xs":
                xml_path = args[0][i + 1]
                break
        
        with open(xml_path, 'w') as f:
            f.write(FINDSCU_NO_RESULTS_XML)
        
        return mock.Mock(returncode=0, stdout="", stderr="")
    
    with mock.patch('tempfile.NamedTemporaryFile') as mock_temp:
        mock_temp.return_value.__enter__.return_value.name = str(tmp_path / "response.xml")
        with mock.patch('subprocess.run', side_effect=mock_run):
            with mock.patch('time.sleep'):
                results = find_studies(
                    host="localhost",
                    port=11112,
                    calling_aet="TEST_SCU",
                    called_aet="ORTHANC_TEST",
                    query_params={"AccessionNumber": "NONEXISTENT"},
                )
    
    assert len(results) == 0


def test_find_studies_command_error(tmp_path):
    def mock_run(*args, **kwargs):
        return mock.Mock(returncode=1, stdout="", stderr="Error: command failed")
    
    with mock.patch('tempfile.NamedTemporaryFile') as mock_temp:
        mock_temp.return_value.__enter__.return_value.name = str(tmp_path / "response.xml")
        with mock.patch('subprocess.run', side_effect=mock_run):
            with mock.patch('time.sleep'):
                with pytest.raises(DCMTKCommandError, match="findscu command failed"):
                    find_studies(
                        host="localhost",
                        port=11112,
                        calling_aet="TEST_SCU",
                        called_aet="ORTHANC_TEST",
                        query_params={"AccessionNumber": "TEST"},
                    )


def test_get_study_success(tmp_path):
    def mock_run(*args, **kwargs):
        return mock.Mock(returncode=0, stdout="", stderr=GETSCU_SUCCESS_STDERR)

    with mock.patch('subprocess.run', side_effect=mock_run):
        with mock.patch('time.sleep'):
            result = get_study(
                host="localhost",
                port=11112,
                calling_aet="TEST_SCU",
                called_aet="ORTHANC_TEST",
                output_dir=str(tmp_path / "output"),
                study_uid="1.2.826.0.1.3680043.8.498.12345",
            )

    assert result["success"] is True
    assert result["num_completed"] == 10
    assert result["num_failed"] == 0
    assert result["num_warning"] == 0


def test_get_study_failure(tmp_path):
    def mock_run(*args, **kwargs):
        return mock.Mock(returncode=69, stdout="", stderr=GETSCU_FAILURE_STDERR)

    with mock.patch('subprocess.run', side_effect=mock_run):
        with mock.patch('time.sleep'):
            result = get_study(
                host="localhost",
                port=11112,
                calling_aet="TEST_SCU",
                called_aet="ORTHANC_TEST",
                output_dir=str(tmp_path / "output"),
                study_uid="9.9.9.9.9.9.9.9",
            )

    assert result["success"] is False
    assert "UnableToProcess" in result["message"]


def test_invalid_xml_response(tmp_path):
    def mock_run(*args, **kwargs):
        xml_path = None
        for i, arg in enumerate(args[0]):
            if arg == "-Xs":
                xml_path = args[0][i + 1]
                break
        
        with open(xml_path, 'w') as f:
            f.write("<invalid>xml</that><is>broken")
        
        return mock.Mock(returncode=0, stdout="", stderr="")
    
    with mock.patch('tempfile.NamedTemporaryFile') as mock_temp:
        mock_temp.return_value.__enter__.return_value.name = str(tmp_path / "response.xml")
        with mock.patch('subprocess.run', side_effect=mock_run):
            with mock.patch('time.sleep'):
                with pytest.raises(DCMTKParseError, match="Failed to parse"):
                    find_studies(
                        host="localhost",
                        port=11112,
                        calling_aet="TEST_SCU",
                        called_aet="ORTHANC_TEST",
                        query_params={"AccessionNumber": "TEST"},
                    )


def test_find_studies_retries_on_failure(tmp_path):
    attempt_count = {"count": 0}
    
    def mock_run(*args, **kwargs):
        attempt_count["count"] += 1
        xml_path = None
        for i, arg in enumerate(args[0]):
            if arg == "-Xs":
                xml_path = args[0][i + 1]
                break
        
        if attempt_count["count"] == 1:
            return mock.Mock(returncode=1, stdout="", stderr="Network timeout")
        
        with open(xml_path, 'w') as f:
            f.write(FINDSCU_SINGLE_ACCESSION_XML)
        
        return mock.Mock(returncode=0, stdout="", stderr="")
    
    with mock.patch('tempfile.NamedTemporaryFile') as mock_temp:
        mock_temp.return_value.__enter__.return_value.name = str(tmp_path / "response.xml")
        with mock.patch('subprocess.run', side_effect=mock_run):
            with mock.patch('time.sleep'):
                results = find_studies(
                    host="localhost",
                    port=11112,
                    calling_aet="TEST_SCU",
                    called_aet="ORTHANC_TEST",
                    query_params={"AccessionNumber": "ACC12345"},
                )
    
    assert len(results) == 1
    assert attempt_count["count"] == 2


def test_get_study_retries_on_failure(tmp_path):
    attempt_count = {"count": 0}

    def mock_run(*args, **kwargs):
        attempt_count["count"] += 1

        if attempt_count["count"] == 1:
            return mock.Mock(returncode=69, stdout="", stderr=GETSCU_FAILURE_STDERR)

        return mock.Mock(returncode=0, stdout="", stderr=GETSCU_SUCCESS_STDERR)

    with mock.patch('subprocess.run', side_effect=mock_run):
        with mock.patch('time.sleep'):
            result = get_study(
                host="localhost",
                port=11112,
                calling_aet="TEST_SCU",
                called_aet="ORTHANC_TEST",
                output_dir=str(tmp_path / "output"),
                study_uid="1.2.826.0.1.3680043.8.498.12345",
            )

    assert result["success"] is True
    assert attempt_count["count"] == 2


def test_echo_pacs_success():
    def mock_run(*args, **kwargs):
        return mock.Mock(returncode=0, stdout="", stderr="")
    
    with mock.patch('subprocess.run', side_effect=mock_run):
        result = echo_pacs(
            host="localhost",
            port=11112,
            calling_aet="TEST_SCU",
            called_aet="ORTHANC_TEST",
        )
    assert result["success"] is True


def test_echo_pacs_failure():
    def mock_run(*args, **kwargs):
        return mock.Mock(returncode=1, stdout="", stderr="Association Rejected")
    
    with mock.patch('subprocess.run', side_effect=mock_run):
        result = echo_pacs(
            host="localhost",
            port=11112,
            calling_aet="TEST_SCU",
            called_aet="ORTHANC_TEST",
        )
    assert result["success"] is False
    assert "Association Rejected" in result["message"]

