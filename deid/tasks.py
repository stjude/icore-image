from pydantic import BaseModel

from celery import shared_task

import pipeline
from utils import (
    DeidExportResult,
    HeaderExtractResult,
    ImageDeidLocalResult,
    PacsConfiguration,
    PacsQueryResult,
    RunDirs,
    ImagineWorkflowResult,
    Spreadsheet,
    TextDeidResult,
)


class PacsConfigurationArgs(BaseModel):
    host: str
    port: int
    aet: str

    def to_pacs_configuration(self) -> PacsConfiguration:
        return PacsConfiguration(host=self.host, port=self.port, aet=self.aet)


class SpreadsheetArgs(BaseModel):
    """A spreadsheet referenced by file path.

    ``utils.Spreadsheet`` holds a pandas DataFrame, which cannot be serialized
    as a Celery task argument, so tasks receive the source file path and load
    the spreadsheet on the worker.
    """

    path: str
    acc_col: str | None
    mrn_col: str | None
    date_col: str | None

    def to_spreadsheet(self) -> Spreadsheet:
        return Spreadsheet.from_file(
            self.path,
            acc_col=self.acc_col,
            mrn_col=self.mrn_col,
            date_col=self.date_col,
        )


class HeaderExtractLocalArgs(BaseModel):
    input_dir: str
    output_dir: str
    headers_to_extract: list[str] | None
    extract_all_headers: bool
    debug: bool
    run_dirs: RunDirs | None


class ImageExportArgs(BaseModel):
    input_dir: str
    sas_url: str
    project_name: str
    debug: bool
    run_dirs: RunDirs | None


class ImageDeidLocalArgs(BaseModel):
    input_dir: str
    output_dir: str
    filter_script: str | None
    anonymizer_script: str | None
    deid_pixels: bool
    lookup_table: str | None
    debug: bool
    run_dirs: RunDirs | None
    apply_default_filter_script: bool
    mapping_file_path: str | None
    sc_pdf_output_dir: str | None


class ImageQrArgs(BaseModel):
    pacs_list: list[PacsConfigurationArgs]
    query_spreadsheet: SpreadsheetArgs
    application_aet: str
    output_dir: str
    cmove_batch_size: int
    date_window_days: int
    debug: bool
    run_dirs: RunDirs | None
    use_fallback_query: bool
    storescp_port: int
    deferred_delivery: bool
    deferred_delivery_timeout: int


class ImageDeidPacsArgs(ImageQrArgs):
    filter_script: str | None
    anonymizer_script: str | None
    deid_pixels: bool
    lookup_table: str | None
    apply_default_filter_script: bool
    mapping_file_path: str | None
    sc_pdf_output_dir: str | None


class ImageDeidExportArgs(ImageDeidPacsArgs):
    sas_url: str
    project_name: str


class TextDeidArgs(BaseModel):
    input_file: str
    output_dir: str
    to_keep_list: list[str] | None
    to_remove_list: list[str] | None
    columns_to_drop: list[str] | None
    columns_to_deid: list[str] | None
    debug: bool
    run_dirs: RunDirs | None


class ImagineWorkflowArgs(ImageDeidPacsArgs):
    sas_url: str | None
    project_name: str
    input_file: str
    to_keep_list: list[str] | None
    to_remove_list: list[str] | None
    columns_to_drop: list[str] | None
    columns_to_deid: list[str] | None
    skip_export: bool
    headers_to_extract: list[str] | None
    extract_all_headers: bool


def _pacs_kwargs(args: ImageQrArgs) -> dict:
    kwargs = args.model_dump(exclude={"pacs_list", "query_spreadsheet"})
    kwargs["pacs_list"] = [p.to_pacs_configuration() for p in args.pacs_list]
    kwargs["query_spreadsheet"] = args.query_spreadsheet.to_spreadsheet()
    return kwargs


@shared_task(pydantic=True)
def headerextract_local(args: HeaderExtractLocalArgs) -> HeaderExtractResult:
    return pipeline.headerextract_local(**args.model_dump())


@shared_task(pydantic=True)
def image_export(args: ImageExportArgs) -> dict[str, str]:
    return pipeline.ImageExportPipeline(**args.model_dump()).run()


@shared_task(pydantic=True)
def imagedeid_local(args: ImageDeidLocalArgs) -> ImageDeidLocalResult:
    return pipeline.ImageDeidLocalPipeline(**args.model_dump()).run()


@shared_task(pydantic=True)
def imagedeid_pacs(args: ImageDeidPacsArgs) -> PacsQueryResult:
    return pipeline.ImageDeidPacsPipeline(**_pacs_kwargs(args)).run()


@shared_task(pydantic=True)
def imagedeidexport(args: ImageDeidExportArgs) -> DeidExportResult:
    return pipeline.ImageDeidExportPipeline(**_pacs_kwargs(args)).run()


@shared_task(pydantic=True)
def imageqr(args: ImageQrArgs) -> PacsQueryResult:
    return pipeline.imageqr(**_pacs_kwargs(args))


@shared_task(pydantic=True)
def imagineworkflow(args: ImagineWorkflowArgs) -> ImagineWorkflowResult:
    return pipeline.ImagineWorkflowPipeline(**_pacs_kwargs(args)).run()


@shared_task(pydantic=True)
def textdeid(args: TextDeidArgs) -> TextDeidResult:
    return pipeline.TextDeidPipeline(**args.model_dump()).run()
