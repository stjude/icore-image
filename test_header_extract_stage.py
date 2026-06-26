import logging

import pandas as pd

from pipeline.context import PipelineContext
from pipeline.header_extract import DEFAULT_HEADERS_TO_EXTRACT
from pipeline.pipelines import ImagineWorkflowPipeline
from pipeline.stages.header_extract import HeaderExtractStage
from test_utils import _create_test_dicom
from utils import RunDirs


logging.basicConfig(level=logging.INFO)


def _make_ctx(tmp_path) -> PipelineContext:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir()
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    run_dirs = RunDirs(
        log_dir=str(log_dir),
        run_log_path=str(log_dir / "run.txt"),
        appdata_dir=str(appdata_dir),
    )
    return PipelineContext(
        run_dirs=run_dirs,
        output_dir=str(output_dir),
        appdata_dir=str(appdata_dir),
    )


def test_header_extract_stage_writes_metadata(tmp_path):
    ctx = _make_ctx(tmp_path)

    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds.save_as(str(tmp_path / "output" / "f001.dcm"), write_like_original=False)

    HeaderExtractStage(headers_to_extract=["PatientID", "Modality"]).execute(ctx)

    metadata_path = tmp_path / "output" / "metadata.xlsx"
    assert metadata_path.exists(), "metadata.xlsx should be written to output_dir"

    df = pd.read_excel(metadata_path)
    assert df.loc[0, "PatientID"] == "MRN001"
    assert df.loc[0, "Modality"] == "CT"
    assert "StudyInstanceUID" in df.columns

    assert ctx.header_files_processed == 1
    assert ctx.header_studies == 1


def test_header_extract_stage_extract_all(tmp_path):
    ctx = _make_ctx(tmp_path)

    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds.save_as(str(tmp_path / "output" / "f001.dcm"), write_like_original=False)

    HeaderExtractStage(extract_all_headers=True).execute(ctx)

    df = pd.read_excel(tmp_path / "output" / "metadata.xlsx")
    assert df.loc[0, "AccessionNumber"] == "ACC001"
    assert ctx.header_files_processed == 1


def test_default_headers_cover_requested_fields():
    assert DEFAULT_HEADERS_TO_EXTRACT == [
        "PatientSex",
        "PatientAge",
        "EthnicGroup",
        "InstitutionName",
        "Modality",
        "Manufacturer",
        "ManufacturerModelName",
    ]


def test_header_extract_stage_with_default_headers(tmp_path):
    ctx = _make_ctx(tmp_path)

    ds = _create_test_dicom("ACC001", "MRN001", "Smith^John", "CT", "0.5")
    ds.PatientSex = "M"
    ds.PatientAge = "045Y"
    ds.EthnicGroup = "TestGroup"
    ds.save_as(str(tmp_path / "output" / "f001.dcm"), write_like_original=False)

    HeaderExtractStage(headers_to_extract=list(DEFAULT_HEADERS_TO_EXTRACT)).execute(ctx)

    df = pd.read_excel(tmp_path / "output" / "metadata.xlsx")
    assert df.loc[0, "PatientSex"] == "M"
    assert df.loc[0, "PatientAge"] == "045Y"
    assert df.loc[0, "EthnicGroup"] == "TestGroup"
    assert df.loc[0, "InstitutionName"] == "Test Hospital"
    assert df.loc[0, "Modality"] == "CT"
    assert df.loc[0, "Manufacturer"] == "TestManufacturer"
    assert df.loc[0, "ManufacturerModelName"] == "TestModel"
    assert ctx.header_studies == 1


def _imagine_pipeline(**overrides):
    kwargs = dict(
        pacs_list=[],
        query_spreadsheet=None,
        application_aet="AET",
        sas_url=None,
        project_name="P",
        input_file="in.xlsx",
        output_dir="out",
        cmove_batch_size=1,
        skip_export=True,
    )
    kwargs.update(overrides)
    return ImagineWorkflowPipeline(**kwargs)


def test_imagineworkflow_skips_header_extract_when_unconfigured():
    pipeline = _imagine_pipeline()
    assert pipeline.build_header_extract_stage() is None


def test_imagineworkflow_builds_header_extract_when_configured():
    pipeline = _imagine_pipeline(headers_to_extract=["PatientID"])
    stage = pipeline.build_header_extract_stage()
    assert isinstance(stage, HeaderExtractStage)
    assert stage.headers_to_extract == ["PatientID"]

    pipeline = _imagine_pipeline(extract_all_headers=True)
    stage = pipeline.build_header_extract_stage()
    assert isinstance(stage, HeaderExtractStage)
    assert stage.extract_all_headers is True
