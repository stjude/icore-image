import logging
import os
from typing import Any

from pipeline.base import Pipeline, PipelineStage
from pipeline.context import PipelineContext
from pipeline.stages.export import AzureBlobExport
from pipeline.stages.gather import (
    GatherStage,
    LocalFilesystemGather,
    PacsQueryGather,
)
from pipeline.stages.image_deid import ImageDeidExecutor
from pipeline.stages.text_deid import PresidioTextDeid
from utils import (
    DeidEngine,
    DeidExportResult,
    ImageDeidLocalResult,
    PacsConfiguration,
    PacsQueryResult,
    RunDirs,
    SingleClickResult,
    Spreadsheet,
    TextDeidResult,
    configure_run_logging,
    setup_run_directories,
)

# ---------------------------------------------------------------------------
# Shared preamble helper
# ---------------------------------------------------------------------------


def _prepare_run(
    run_dirs: RunDirs | None,
    debug: bool,
    appdata_dir: str | None,
    output_dir: str,
) -> tuple[RunDirs, str]:
    """Bootstrap logging + directory scaffolding shared by every pipeline."""
    if run_dirs is None:
        run_dirs = setup_run_directories()

    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)

    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)

    return run_dirs, appdata_dir


# ---------------------------------------------------------------------------
# ImageDeidLocalPipeline — local filesystem → CTP/Rust
# ---------------------------------------------------------------------------


class ImageDeidLocalPipeline(Pipeline):
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        appdata_dir: str | None = None,
        filter_script: str | None = None,
        anonymizer_script: str | None = None,
        deid_pixels: bool = False,
        lookup_table: str | None = None,
        debug: bool = False,
        run_dirs: RunDirs | None = None,
        apply_default_filter_script: bool = True,
        mapping_file_path: str | None = None,
        sc_pdf_output_dir: str | None = None,
        deid_engine: DeidEngine = "ctp",
    ) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.appdata_dir_arg = appdata_dir
        self.filter_script = filter_script
        self.anonymizer_script = anonymizer_script
        self.deid_pixels = deid_pixels
        self.lookup_table = lookup_table
        self.debug = debug
        self.run_dirs_arg = run_dirs
        self.apply_default_filter_script = apply_default_filter_script
        self.mapping_file_path = mapping_file_path
        self.sc_pdf_output_dir = sc_pdf_output_dir
        self.deid_engine = deid_engine

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.debug, self.appdata_dir_arg, self.output_dir
        )

        engine_label = (
            "dicom-deid-rs (Rust)" if self.deid_engine == "rust" else "CTP (Java)"
        )
        logging.info(f"Starting image deidentification using {engine_label} engine")
        logging.info(f"Output directory: {self.output_dir}")
        if self.filter_script:
            logging.info(f"Filter script: {self.filter_script}")
        if self.anonymizer_script:
            logging.info(f"Anonymizer script: {self.anonymizer_script}")
        if self.lookup_table:
            logging.info(f"Lookup table: {self.lookup_table}")
        if self.mapping_file_path:
            logging.info(f"Mapping file: {self.mapping_file_path}")
        logging.info(
            f"Pixel deidentification: {'enabled' if self.deid_pixels else 'disabled'}"
        )

        return PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.output_dir,
            appdata_dir=appdata_dir,
            debug=self.debug,
        )

    def build_gather_stage(self) -> GatherStage:
        return LocalFilesystemGather(self.input_dir)

    def build_image_deid_stage(self) -> PipelineStage:
        return ImageDeidExecutor(
            engine=self.deid_engine,
            anonymizer_script=self.anonymizer_script,
            filter_script=self.filter_script,
            lookup_table=self.lookup_table,
            mapping_file_path=self.mapping_file_path,
            deid_pixels=self.deid_pixels,
            apply_default_filter_script=self.apply_default_filter_script,
            sc_pdf_output_dir=self.sc_pdf_output_dir,
        )

    def _to_result(self, ctx: PipelineContext) -> Any:
        result: ImageDeidLocalResult = {
            "num_images_saved": ctx.images_saved,
            "num_images_quarantined": ctx.images_quarantined,
        }
        return result


# ---------------------------------------------------------------------------
# ImageDeidPacsPipeline — PACS query/retrieve → CTP/Rust
# ---------------------------------------------------------------------------


class ImageDeidPacsPipeline(Pipeline):
    def __init__(
        self,
        pacs_list: list[PacsConfiguration],
        query_spreadsheet: Spreadsheet,
        application_aet: str,
        output_dir: str,
        appdata_dir: str | None = None,
        filter_script: str | None = None,
        date_window_days: int = 0,
        anonymizer_script: str | None = None,
        deid_pixels: bool = False,
        lookup_table: str | None = None,
        debug: bool = False,
        run_dirs: RunDirs | None = None,
        apply_default_filter_script: bool = True,
        mapping_file_path: str | None = None,
        sc_pdf_output_dir: str | None = None,
        use_fallback_query: bool = False,
        storescp_port: int = 50001,
        deferred_delivery: bool = False,
        deferred_delivery_timeout: int = 172800,
        deid_engine: DeidEngine = "ctp",
    ) -> None:
        self.pacs_list = pacs_list
        self.query_spreadsheet = query_spreadsheet
        self.application_aet = application_aet
        self.output_dir = output_dir
        self.appdata_dir_arg = appdata_dir
        self.filter_script = filter_script
        self.date_window_days = date_window_days
        self.anonymizer_script = anonymizer_script
        self.deid_pixels = deid_pixels
        self.lookup_table = lookup_table
        self.debug = debug
        self.run_dirs_arg = run_dirs
        self.apply_default_filter_script = apply_default_filter_script
        self.mapping_file_path = mapping_file_path
        self.sc_pdf_output_dir = sc_pdf_output_dir
        self.use_fallback_query = use_fallback_query
        self.storescp_port = storescp_port
        self.deferred_delivery = deferred_delivery
        self.deferred_delivery_timeout = deferred_delivery_timeout
        self.deid_engine = deid_engine

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.debug, self.appdata_dir_arg, self.output_dir
        )

        engine_label = (
            "dicom-deid-rs (Rust)" if self.deid_engine == "rust" else "CTP (Java)"
        )
        logging.info(
            f"Running imagedeid_pacs using {engine_label} engine "
            f"(use_fallback_query={self.use_fallback_query})"
        )

        return PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.output_dir,
            appdata_dir=appdata_dir,
            debug=self.debug,
        )

    def build_gather_stage(self) -> GatherStage:
        return PacsQueryGather(
            pacs_list=self.pacs_list,
            query_spreadsheet=self.query_spreadsheet,
            application_aet=self.application_aet,
            date_window_days=self.date_window_days,
            use_fallback_query=self.use_fallback_query,
            storescp_port=self.storescp_port,
            deferred_delivery=self.deferred_delivery,
            deferred_delivery_timeout=self.deferred_delivery_timeout,
            filter_script_seed=self.filter_script,
        )

    def build_image_deid_stage(self) -> PipelineStage:
        # The gather stage writes its query-merged filter into
        # ``ctx.gather_filter_override`` during execute(); ImageDeidExecutor
        # picks it up from there at its own execute() time.
        return ImageDeidExecutor(
            engine=self.deid_engine,
            anonymizer_script=self.anonymizer_script,
            filter_script=self.filter_script,
            lookup_table=self.lookup_table,
            mapping_file_path=self.mapping_file_path,
            deid_pixels=self.deid_pixels,
            apply_default_filter_script=self.apply_default_filter_script,
            sc_pdf_output_dir=self.sc_pdf_output_dir,
            application_aet=self.application_aet,
        )

    def _to_result(self, ctx: PipelineContext) -> Any:
        result: PacsQueryResult = {
            "num_studies_found": ctx.gathered_studies,
            "num_images_saved": ctx.images_saved,
            "num_images_quarantined": ctx.images_quarantined,
            "failed_query_indices": ctx.failed_query_indices,
        }
        return result


# ---------------------------------------------------------------------------
# TextDeidPipeline — standalone text scrubber
# ---------------------------------------------------------------------------


class TextDeidPipeline(Pipeline):
    def __init__(
        self,
        input_file: str,
        output_dir: str,
        to_keep_list: list[str] | None = None,
        to_remove_list: list[str] | None = None,
        columns_to_drop: list[str] | None = None,
        columns_to_deid: list[str] | None = None,
        debug: bool = False,
        run_dirs: RunDirs | None = None,
        appdata_dir: str | None = None,
    ) -> None:
        self.input_file = input_file
        self.output_dir = output_dir
        self.to_keep_list = to_keep_list
        self.to_remove_list = to_remove_list
        self.columns_to_drop = columns_to_drop
        self.columns_to_deid = columns_to_deid
        self.debug = debug
        self.run_dirs_arg = run_dirs
        self.appdata_dir_arg = appdata_dir

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.debug, self.appdata_dir_arg, self.output_dir
        )
        ctx = PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.output_dir,
            appdata_dir=appdata_dir,
            debug=self.debug,
        )
        ctx.text_input_file = self.input_file
        return ctx

    def build_text_deid_stage(self) -> PipelineStage:
        return PresidioTextDeid(
            to_keep_list=self.to_keep_list,
            to_remove_list=self.to_remove_list,
            columns_to_drop=self.columns_to_drop,
            columns_to_deid=self.columns_to_deid,
        )

    def _to_result(self, ctx: PipelineContext) -> Any:
        result: TextDeidResult = {
            "num_rows_processed": ctx.text_rows_processed,
            "output_file": ctx.text_output_file or "",
        }
        return result


# ---------------------------------------------------------------------------
# ImageExportPipeline — standalone rclone push
# ---------------------------------------------------------------------------


class ImageExportPipeline(Pipeline):
    def __init__(
        self,
        input_dir: str,
        sas_url: str,
        project_name: str,
        appdata_dir: str | None = None,
        debug: bool = False,
        run_dirs: RunDirs | None = None,
    ) -> None:
        self.input_dir = input_dir
        self.sas_url = sas_url
        self.project_name = project_name
        self.appdata_dir_arg = appdata_dir
        self.debug = debug
        self.run_dirs_arg = run_dirs

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.debug, self.appdata_dir_arg, self.input_dir
        )
        # ExportStage reads ctx.output_dir, so set it to the input directory
        # (the "output of a prior pipeline" being exported).
        return PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.input_dir,
            appdata_dir=appdata_dir,
            debug=self.debug,
        )

    def build_export_stage(self) -> PipelineStage:
        # Standalone export should always run regardless of upstream counts.
        return AzureBlobExport(
            sas_url=self.sas_url,
            project_name=self.project_name,
            gate_on_content=False,
        )

    def _to_result(self, ctx: PipelineContext) -> Any:
        return {"status": "completed"}


# ---------------------------------------------------------------------------
# ImageDeidExportPipeline — PACS → deid → Azure
# ---------------------------------------------------------------------------


class ImageDeidExportPipeline(ImageDeidPacsPipeline):
    def __init__(
        self,
        pacs_list: list[PacsConfiguration],
        query_spreadsheet: Spreadsheet,
        application_aet: str,
        sas_url: str,
        project_name: str,
        output_dir: str,
        appdata_dir: str | None = None,
        filter_script: str | None = None,
        date_window_days: int = 0,
        anonymizer_script: str | None = None,
        deid_pixels: bool = False,
        lookup_table: str | None = None,
        debug: bool = False,
        run_dirs: RunDirs | None = None,
        apply_default_filter_script: bool = True,
        mapping_file_path: str | None = None,
        sc_pdf_output_dir: str | None = None,
        use_fallback_query: bool = False,
        storescp_port: int = 50001,
        deferred_delivery: bool = False,
        deferred_delivery_timeout: int = 172800,
        deid_engine: DeidEngine = "ctp",
    ) -> None:
        super().__init__(
            pacs_list=pacs_list,
            query_spreadsheet=query_spreadsheet,
            application_aet=application_aet,
            output_dir=output_dir,
            appdata_dir=appdata_dir,
            filter_script=filter_script,
            date_window_days=date_window_days,
            anonymizer_script=anonymizer_script,
            deid_pixels=deid_pixels,
            lookup_table=lookup_table,
            debug=debug,
            run_dirs=run_dirs,
            apply_default_filter_script=apply_default_filter_script,
            mapping_file_path=mapping_file_path,
            sc_pdf_output_dir=sc_pdf_output_dir,
            use_fallback_query=use_fallback_query,
            storescp_port=storescp_port,
            deferred_delivery=deferred_delivery,
            deferred_delivery_timeout=deferred_delivery_timeout,
            deid_engine=deid_engine,
        )
        self.sas_url = sas_url
        self.project_name = project_name

    def _build_context(self) -> PipelineContext:
        ctx = super()._build_context()
        logging.info("Running imagedeidexport")
        return ctx

    def build_export_stage(self) -> PipelineStage:
        return AzureBlobExport(
            sas_url=self.sas_url,
            project_name=self.project_name,
            gate_on_content=True,
        )

    def _to_result(self, ctx: PipelineContext) -> Any:
        logging.info("Deidentification and export complete")
        logging.info(f"Deidentified files preserved at: {ctx.output_dir}")
        result: DeidExportResult = {
            "num_studies_found": ctx.gathered_studies,
            "num_images_exported": ctx.images_saved,
            "num_images_quarantined": ctx.images_quarantined,
            "failed_query_indices": ctx.failed_query_indices,
        }
        return result


# ---------------------------------------------------------------------------
# SingleClickIcorePipeline — PACS → deid → text-deid → Azure
# ---------------------------------------------------------------------------


class SingleClickIcorePipeline(ImageDeidPacsPipeline):
    def __init__(
        self,
        pacs_list: list[PacsConfiguration],
        query_spreadsheet: Spreadsheet,
        application_aet: str,
        sas_url: str | None,
        project_name: str,
        input_file: str,
        output_dir: str,
        appdata_dir: str | None = None,
        filter_script: str | None = None,
        date_window_days: int = 0,
        anonymizer_script: str | None = None,
        deid_pixels: bool = False,
        lookup_table: str | None = None,
        debug: bool = False,
        run_dirs: RunDirs | None = None,
        apply_default_filter_script: bool = True,
        mapping_file_path: str | None = None,
        sc_pdf_output_dir: str | None = None,
        use_fallback_query: bool = False,
        storescp_port: int = 50001,
        deferred_delivery: bool = False,
        deferred_delivery_timeout: int = 172800,
        deid_engine: DeidEngine = "ctp",
        to_keep_list: list[str] | None = None,
        to_remove_list: list[str] | None = None,
        columns_to_drop: list[str] | None = None,
        columns_to_deid: list[str] | None = None,
        skip_export: bool = False,
    ) -> None:
        super().__init__(
            pacs_list=pacs_list,
            query_spreadsheet=query_spreadsheet,
            application_aet=application_aet,
            output_dir=output_dir,
            appdata_dir=appdata_dir,
            filter_script=filter_script,
            date_window_days=date_window_days,
            anonymizer_script=anonymizer_script,
            deid_pixels=deid_pixels,
            lookup_table=lookup_table,
            debug=debug,
            run_dirs=run_dirs,
            apply_default_filter_script=apply_default_filter_script,
            mapping_file_path=mapping_file_path,
            sc_pdf_output_dir=sc_pdf_output_dir,
            use_fallback_query=use_fallback_query,
            storescp_port=storescp_port,
            deferred_delivery=deferred_delivery,
            deferred_delivery_timeout=deferred_delivery_timeout,
            deid_engine=deid_engine,
        )
        self.sas_url = sas_url
        self.project_name = project_name
        self.input_file = input_file
        self.to_keep_list = to_keep_list
        self.to_remove_list = to_remove_list
        self.columns_to_drop = columns_to_drop
        self.columns_to_deid = columns_to_deid
        self.skip_export = skip_export

    def _build_context(self) -> PipelineContext:
        ctx = super()._build_context()
        logging.info("Running singleclickicore")
        ctx.text_input_file = self.input_file
        return ctx

    def build_text_deid_stage(self) -> PipelineStage:
        return PresidioTextDeid(
            to_keep_list=self.to_keep_list,
            to_remove_list=self.to_remove_list,
            columns_to_drop=self.columns_to_drop,
            columns_to_deid=self.columns_to_deid,
        )

    def build_export_stage(self) -> PipelineStage | None:
        if self.skip_export:
            logging.info("=" * 80)
            logging.info("Export to Azure Blob Storage - SKIPPED")
            logging.info("=" * 80)
            return None
        if self.sas_url is None:
            raise ValueError("sas_url is required when skip_export is False")
        return AzureBlobExport(
            sas_url=self.sas_url,
            project_name=self.project_name,
            gate_on_content=True,
        )

    def _to_result(self, ctx: PipelineContext) -> Any:
        logging.info("=" * 80)
        logging.info("singleclickicore complete")
        logging.info(f"Deidentified files preserved at: {ctx.output_dir}")
        logging.info("=" * 80)
        result: SingleClickResult = {
            "num_studies_found": ctx.gathered_studies,
            "num_images_exported": ctx.images_saved,
            "num_images_quarantined": ctx.images_quarantined,
            "failed_query_indices": ctx.failed_query_indices,
            "num_rows_processed": ctx.text_rows_processed,
            "output_file": ctx.text_output_file or "",
            "export_performed": not self.skip_export,
        }
        return result
