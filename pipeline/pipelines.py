import logging
import os
from typing import Any

from config import IcoreConfig
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
    DeidExportResult,
    ImageDeidLocalResult,
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
        config: IcoreConfig,
        *,
        input_dir: str,
        output_dir: str,
        appdata_dir: str | None = None,
        run_dirs: RunDirs | None = None,
    ) -> None:
        self.config = config
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.appdata_dir_arg = appdata_dir
        self.run_dirs_arg = run_dirs

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.config.debug, self.appdata_dir_arg, self.output_dir
        )

        engine_label = (
            "dicom-deid-rs (Rust)" if self.config.deid_engine == "rust" else "CTP (Java)"
        )
        logging.info(f"Starting image deidentification using {engine_label} engine")
        logging.info(f"Output directory: {self.output_dir}")
        if self.config.filter_script:
            logging.info(f"Filter script: {self.config.filter_script}")
        if self.config.anonymizer_script:
            logging.info(f"Anonymizer script: {self.config.anonymizer_script}")
        if self.config.lookup_table:
            logging.info(f"Lookup table: {self.config.lookup_table}")
        if self.config.mapping_file_path:
            logging.info(f"Mapping file: {self.config.mapping_file_path}")
        logging.info(
            f"Pixel deidentification: "
            f"{'enabled' if self.config.deid_pixels else 'disabled'}"
        )

        return PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.output_dir,
            appdata_dir=appdata_dir,
            debug=self.config.debug,
        )

    def build_gather_stage(self) -> GatherStage:
        return LocalFilesystemGather(self.input_dir)

    def build_image_deid_stage(self) -> PipelineStage:
        return ImageDeidExecutor(self.config)

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
        config: IcoreConfig,
        *,
        query_spreadsheet: Spreadsheet,
        output_dir: str,
        appdata_dir: str | None = None,
        run_dirs: RunDirs | None = None,
    ) -> None:
        self.config = config
        self.query_spreadsheet = query_spreadsheet
        self.output_dir = output_dir
        self.appdata_dir_arg = appdata_dir
        self.run_dirs_arg = run_dirs

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.config.debug, self.appdata_dir_arg, self.output_dir
        )

        engine_label = (
            "dicom-deid-rs (Rust)" if self.config.deid_engine == "rust" else "CTP (Java)"
        )
        logging.info(
            f"Running imagedeid_pacs using {engine_label} engine "
            f"(use_fallback_query={self.config.use_fallback_query})"
        )

        return PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.output_dir,
            appdata_dir=appdata_dir,
            debug=self.config.debug,
        )

    def build_gather_stage(self) -> GatherStage:
        return PacsQueryGather(self.config, query_spreadsheet=self.query_spreadsheet)

    def build_image_deid_stage(self) -> PipelineStage:
        # The gather stage writes its query-merged filter into
        # ``ctx.gather_filter_override`` during execute(); ImageDeidExecutor
        # picks it up from there at its own execute() time.
        return ImageDeidExecutor(self.config)

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
        config: IcoreConfig,
        *,
        input_file: str,
        output_dir: str,
        appdata_dir: str | None = None,
        run_dirs: RunDirs | None = None,
    ) -> None:
        self.config = config
        self.input_file = input_file
        self.output_dir = output_dir
        self.appdata_dir_arg = appdata_dir
        self.run_dirs_arg = run_dirs

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.config.debug, self.appdata_dir_arg, self.output_dir
        )
        ctx = PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.output_dir,
            appdata_dir=appdata_dir,
            debug=self.config.debug,
        )
        ctx.text_input_file = self.input_file
        return ctx

    def build_text_deid_stage(self) -> PipelineStage:
        return PresidioTextDeid(self.config)

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
        config: IcoreConfig,
        *,
        input_dir: str,
        appdata_dir: str | None = None,
        run_dirs: RunDirs | None = None,
    ) -> None:
        self.config = config
        self.input_dir = input_dir
        self.appdata_dir_arg = appdata_dir
        self.run_dirs_arg = run_dirs

    def _build_context(self) -> PipelineContext:
        run_dirs, appdata_dir = _prepare_run(
            self.run_dirs_arg, self.config.debug, self.appdata_dir_arg, self.input_dir
        )
        # ExportStage reads ctx.output_dir, so set it to the input directory
        # (the "output of a prior pipeline" being exported).
        return PipelineContext(
            run_dirs=run_dirs,
            output_dir=self.input_dir,
            appdata_dir=appdata_dir,
            debug=self.config.debug,
        )

    def build_export_stage(self) -> PipelineStage:
        # Standalone export should always run regardless of upstream counts.
        return AzureBlobExport(self.config, gate_on_content=False)

    def _to_result(self, ctx: PipelineContext) -> Any:
        return {"status": "completed"}


# ---------------------------------------------------------------------------
# ImageDeidExportPipeline — PACS → deid → Azure
# ---------------------------------------------------------------------------


class ImageDeidExportPipeline(ImageDeidPacsPipeline):
    def _build_context(self) -> PipelineContext:
        ctx = super()._build_context()
        logging.info("Running imagedeidexport")
        return ctx

    def build_export_stage(self) -> PipelineStage:
        return AzureBlobExport(self.config, gate_on_content=True)

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
        config: IcoreConfig,
        *,
        query_spreadsheet: Spreadsheet,
        input_file: str,
        output_dir: str,
        appdata_dir: str | None = None,
        run_dirs: RunDirs | None = None,
    ) -> None:
        super().__init__(
            config,
            query_spreadsheet=query_spreadsheet,
            output_dir=output_dir,
            appdata_dir=appdata_dir,
            run_dirs=run_dirs,
        )
        self.input_file = input_file

    def _build_context(self) -> PipelineContext:
        ctx = super()._build_context()
        logging.info("Running singleclickicore")
        ctx.text_input_file = self.input_file
        return ctx

    def build_text_deid_stage(self) -> PipelineStage:
        return PresidioTextDeid(self.config)

    def build_export_stage(self) -> PipelineStage | None:
        if self.config.skip_export:
            logging.info("=" * 80)
            logging.info("Export to Azure Blob Storage - SKIPPED")
            logging.info("=" * 80)
            return None
        if self.config.sas_url is None:
            raise ValueError("sas_url is required when skip_export is False")
        return AzureBlobExport(self.config, gate_on_content=True)

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
            "export_performed": not self.config.skip_export,
        }
        return result
