import logging
import os
import shutil
import time

from ctp import CTPPipeline
from pipeline.progress import ProgressReporter
from utils import (
    PacsConfiguration,
    PacsQueryResult,
    RunDirs,
    Spreadsheet,
    count_dicom_files,
    generate_queries_and_filter,
    combine_filters,
    validate_date_window_days,
    query_and_retrieve_studies,
    setup_run_directories,
    configure_run_logging,
    format_number_with_commas,
    csv_string_to_xlsx,
    save_failed_queries_csv,
)


def _save_metadata_files(pipeline: CTPPipeline, appdata_dir: str) -> None:
    audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
    if audit_log_csv:
        csv_string_to_xlsx(audit_log_csv, os.path.join(appdata_dir, "metadata.xlsx"))


def _log_progress(pipeline: CTPPipeline) -> None:
    if pipeline.metrics:
        files_received = pipeline.metrics.files_received
        files_quarantined = pipeline.metrics.files_quarantined

        progress_msg = f"Processed {format_number_with_commas(files_received)} files"
        if files_quarantined > 0:
            progress_msg += (
                f" ({format_number_with_commas(files_quarantined)} quarantined)"
            )

        logging.info(progress_msg)


def _report_processing(
    progress: ProgressReporter, pipeline: CTPPipeline, total: int
) -> None:
    if not pipeline.metrics:
        return
    received = pipeline.metrics.files_received
    if total:
        status = (
            f"Processing {format_number_with_commas(received)} of "
            f"{format_number_with_commas(total)} images"
        )
        progress.update("process", received / total, status)
    else:
        progress.update("process", 0.0, "Processing images…")


def imageqr(
    pacs_list: list[PacsConfiguration],
    query_spreadsheet: Spreadsheet,
    application_aet: str,
    output_dir: str,
    cmove_batch_size: int,
    appdata_dir: str | None = None,
    filter_script: str | None = None,
    date_window_days: int = 0,
    debug: bool = False,
    run_dirs: RunDirs | None = None,
    use_fallback_query: bool = False,
    storescp_port: int = 50001,
    deferred_delivery: bool = False,
    deferred_delivery_timeout: int = 172800,
) -> PacsQueryResult:
    if run_dirs is None:
        run_dirs = setup_run_directories()

    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info(f"Running imageqr (use_fallback_query={use_fallback_query})")

    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)

    quarantine_dir = os.path.join(appdata_dir, "quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)

    validate_date_window_days(date_window_days)

    # Drives the task-progress bar: a retrieval stage (query 0→1/3, C-MOVE
    # 1/3→1) followed by the CTP processing pass over the retrieved files.
    progress = ProgressReporter(
        run_dirs["log_dir"],
        [
            ("gather", "Retrieving images from PACS"),
            ("process", "Processing images"),
        ],
    )

    query_params_list, expected_values_list, generated_filter = (
        generate_queries_and_filter(
            query_spreadsheet, date_window_days, use_fallback_query=use_fallback_query
        )
    )
    combined_filter = combine_filters(filter_script, generated_filter)

    # Directory for storescp to write retrieved DICOM files into.
    dicom_retrieval_dir = os.path.join(appdata_dir, "dicom_retrieval")
    os.makedirs(dicom_retrieval_dir, exist_ok=True)

    try:
        ctp_log_level = "DEBUG" if debug else None

        # Retrieve files BEFORE starting CTP so ArchiveImportService finds them
        # on initial scan.
        study_pacs_map, failed_query_indices, combined_failure_details = (
            query_and_retrieve_studies(
                pacs_list,
                query_params_list,
                expected_values_list,
                application_aet,
                dicom_retrieval_dir,
                storescp_port,
                cmove_batch_size,
                fallback_spreadsheet=query_spreadsheet if use_fallback_query else None,
                fallback_date_window_days=date_window_days,
                deferred_delivery=deferred_delivery,
                deferred_delivery_timeout=deferred_delivery_timeout,
                progress=progress,
            )
        )

        total_retrieved = count_dicom_files(dicom_retrieval_dir)

        with CTPPipeline(
            pipeline_type="imageqr",
            input_dir=dicom_retrieval_dir,
            output_dir=output_dir,
            application_aet=application_aet,
            filter_script=combined_filter,
            log_path=run_dirs["ctp_log_path"],
            log_level=ctp_log_level,
            quarantine_dir=quarantine_dir,
        ) as pipeline:
            save_interval = 5
            last_save_time = 0

            while not pipeline.is_complete():
                current_time = time.time()
                if current_time - last_save_time >= save_interval:
                    _save_metadata_files(pipeline, appdata_dir)
                    save_failed_queries_csv(
                        failed_query_indices,
                        query_spreadsheet,
                        appdata_dir,
                        combined_failure_details,
                        use_fallback_query=use_fallback_query,
                    )
                    _log_progress(pipeline)
                    _report_processing(progress, pipeline, total_retrieved)
                    last_save_time = current_time

                time.sleep(1)

            _save_metadata_files(pipeline, appdata_dir)
            save_failed_queries_csv(
                failed_query_indices,
                query_spreadsheet,
                appdata_dir,
                combined_failure_details,
                use_fallback_query=use_fallback_query,
            )

            num_saved = pipeline.metrics.files_saved if pipeline.metrics else 0
            num_quarantined = (
                pipeline.metrics.files_quarantined if pipeline.metrics else 0
            )

            logging.info("Query and retrieval complete")
            logging.info(
                f"Total files processed: {format_number_with_commas(num_saved + num_quarantined)}"
            )
            logging.info(f"Files saved: {format_number_with_commas(num_saved)}")
            logging.info(
                f"Files quarantined: {format_number_with_commas(num_quarantined)}"
            )

            return {
                "num_studies_found": len(study_pacs_map),
                "num_images_saved": pipeline.metrics.files_saved
                if pipeline.metrics
                else 0,
                "num_images_quarantined": pipeline.metrics.files_quarantined
                if pipeline.metrics
                else 0,
                "failed_query_indices": failed_query_indices,
            }
    finally:
        try:
            shutil.rmtree(dicom_retrieval_dir)
        except OSError as e:
            logging.warning(
                "Failed to remove temporary retrieval directory '%s': %s",
                dicom_retrieval_dir,
                e,
            )
