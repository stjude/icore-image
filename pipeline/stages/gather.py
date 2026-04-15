import logging
import os
import shutil
import time
from abc import ABC

from pipeline.base import PipelineStage
from pipeline.context import PipelineContext
from utils import (
    PacsConfiguration,
    Spreadsheet,
    combine_filters,
    count_dicom_files,
    find_studies_from_pacs_list,
    find_valid_pacs_list,
    format_number_with_commas,
    generate_queries_and_filter,
    move_studies_from_study_pacs_map,
    save_failed_queries_csv,
    validate_date_window_days,
)


class GatherStage(PipelineStage, ABC):
    """Stage 1: make DICOM files available on disk.

    On completion, ``ctx.dicom_input_dir`` points to a directory of DICOMs
    the downstream image-deid stage can consume.
    """


class LocalFilesystemGather(GatherStage):
    """Trivial gather: DICOM files are already on the local filesystem."""

    def __init__(self, input_dir: str) -> None:
        self.input_dir = input_dir

    def execute(self, ctx: PipelineContext) -> None:
        logging.info(f"Input directory: {self.input_dir}")
        logging.info("Counting input files...")
        total_files = count_dicom_files(self.input_dir)
        logging.info(f"Found {format_number_with_commas(total_files)} files to process")
        ctx.dicom_input_dir = self.input_dir
        ctx.total_files = total_files


class PacsQueryGather(GatherStage):
    """Query PACS and retrieve matching studies into a scratch directory.

    The scratch dir lives under ``ctx.appdata_dir`` and is removed by
    :meth:`cleanup` after the pipeline completes (success or failure).

    ``filter_script_seed`` is the user-provided CTP filter (before any
    query-derived filter is merged); this stage will AND-merge the
    spreadsheet-derived filter onto it and store the combined filter on
    ``ctx`` for the image-deid stage to consume via
    :attr:`gather_filter_override`.
    """

    def __init__(
        self,
        pacs_list: list[PacsConfiguration],
        query_spreadsheet: Spreadsheet,
        application_aet: str,
        date_window_days: int = 0,
        use_fallback_query: bool = False,
        storescp_port: int = 50001,
        deferred_delivery: bool = False,
        deferred_delivery_timeout: int = 172800,
        filter_script_seed: str | None = None,
    ) -> None:
        validate_date_window_days(date_window_days)
        self.pacs_list = pacs_list
        self.query_spreadsheet = query_spreadsheet
        self.application_aet = application_aet
        self.date_window_days = date_window_days
        self.use_fallback_query = use_fallback_query
        self.storescp_port = storescp_port
        self.deferred_delivery = deferred_delivery
        self.deferred_delivery_timeout = deferred_delivery_timeout
        self.filter_script_seed = filter_script_seed

        self._retrieval_dir: str | None = None

    def execute(self, ctx: PipelineContext) -> None:
        query_params_list, expected_values_list, generated_filter = (
            generate_queries_and_filter(
                self.query_spreadsheet,
                self.date_window_days,
                use_fallback_query=self.use_fallback_query,
            )
        )
        ctx.gather_filter_override = combine_filters(
            self.filter_script_seed, generated_filter
        )

        valid_pacs_list = find_valid_pacs_list(self.pacs_list, self.application_aet)

        study_pacs_map, failed_find_indices, failed_find_details = (
            find_studies_from_pacs_list(
                valid_pacs_list,
                query_params_list,
                self.application_aet,
                expected_values_list,
                fallback_spreadsheet=(
                    self.query_spreadsheet if self.use_fallback_query else None
                ),
                fallback_date_window_days=self.date_window_days,
            )
        )

        retrieval_dir = os.path.join(ctx.appdata_dir, "dicom_retrieval")
        self._retrieval_dir = retrieval_dir

        _, failed_move_indices, failed_move_details = move_studies_from_study_pacs_map(
            study_pacs_map,
            self.application_aet,
            retrieval_dir,
            self.storescp_port,
            deferred_delivery=self.deferred_delivery,
            deferred_delivery_timeout=self.deferred_delivery_timeout,
        )

        # Wait briefly to ensure all files are written
        time.sleep(2)

        failed_query_indices = list(set(failed_find_indices + failed_move_indices))
        combined_failure_details = {**failed_find_details, **failed_move_details}
        save_failed_queries_csv(
            failed_query_indices,
            self.query_spreadsheet,
            ctx.appdata_dir,
            combined_failure_details,
            use_fallback_query=self.use_fallback_query,
        )

        ctx.dicom_input_dir = retrieval_dir
        ctx.gathered_studies = len(study_pacs_map)
        ctx.failed_query_indices = failed_query_indices

    def cleanup(self, ctx: PipelineContext) -> None:
        if self._retrieval_dir is None:
            return
        try:
            shutil.rmtree(self._retrieval_dir)
        except OSError as e:
            logging.warning(
                "Failed to remove temporary retrieval directory '%s': %s",
                self._retrieval_dir,
                e,
            )
