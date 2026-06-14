import logging
import os

from pipeline.progress import ProgressReporter
from utils import (
    PacsConfiguration,
    PacsQueryResult,
    RunDirs,
    Spreadsheet,
    count_dicom_files,
    generate_queries_and_filter,
    validate_date_window_days,
    query_and_retrieve_studies,
    setup_run_directories,
    configure_run_logging,
    format_number_with_commas,
    save_failed_queries_csv,
)


def imageqr(
    pacs_list: list[PacsConfiguration],
    query_spreadsheet: Spreadsheet,
    application_aet: str,
    output_dir: str,
    cmove_batch_size: int,
    appdata_dir: str | None = None,
    date_window_days: int = 0,
    debug: bool = False,
    run_dirs: RunDirs | None = None,
    use_fallback_query: bool = False,
    storescp_port: int = 50001,
    deferred_delivery: bool = False,
    deferred_delivery_timeout: int = 172800,
) -> PacsQueryResult:
    """Query a PACS and retrieve matching studies into ``output_dir``.

    This is a pure query/retrieve: the studies described by the spreadsheet are
    found via C-FIND and pulled via C-MOVE into ``output_dir`` unchanged. No
    filtering or de-identification is applied here — that happens in downstream
    de-id steps that operate on the retrieved directory.
    """
    if run_dirs is None:
        run_dirs = setup_run_directories()

    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info(f"Running imageqr (use_fallback_query={use_fallback_query})")

    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)

    validate_date_window_days(date_window_days)

    # Single-stage progress: the whole job is the PACS retrieval (query then
    # C-MOVE), reported under the "gather" stage.
    progress = ProgressReporter(
        run_dirs["log_dir"],
        [("gather", "Retrieving images from PACS")],
    )

    query_params_list, expected_values_list, _ = generate_queries_and_filter(
        query_spreadsheet, date_window_days, use_fallback_query=use_fallback_query
    )

    study_pacs_map, failed_query_indices, combined_failure_details = (
        query_and_retrieve_studies(
            pacs_list,
            query_params_list,
            expected_values_list,
            application_aet,
            output_dir,
            storescp_port,
            cmove_batch_size,
            fallback_spreadsheet=query_spreadsheet if use_fallback_query else None,
            fallback_date_window_days=date_window_days,
            deferred_delivery=deferred_delivery,
            deferred_delivery_timeout=deferred_delivery_timeout,
            progress=progress,
        )
    )

    save_failed_queries_csv(
        failed_query_indices,
        query_spreadsheet,
        appdata_dir,
        combined_failure_details,
        use_fallback_query=use_fallback_query,
    )

    num_retrieved = count_dicom_files(output_dir)

    logging.info("Query and retrieval complete")
    logging.info(f"Studies found: {format_number_with_commas(len(study_pacs_map))}")
    logging.info(f"Images retrieved: {format_number_with_commas(num_retrieved)}")

    return {
        "num_studies_found": len(study_pacs_map),
        "num_images_saved": num_retrieved,
        "num_images_quarantined": 0,
        "failed_query_indices": failed_query_indices,
    }
