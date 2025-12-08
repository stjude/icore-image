import logging
import os
import time

from ctp import CTPPipeline
from utils import (generate_queries_and_filter, combine_filters,
                   validate_date_window_days, find_valid_pacs_list,
                   find_studies_from_pacs_list, move_studies_from_study_pacs_map,
                   setup_run_directories, configure_run_logging,
                   format_number_with_commas, csv_string_to_xlsx)


def _save_metadata_files(pipeline, appdata_dir):
    audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
    if audit_log_csv:
        csv_string_to_xlsx(audit_log_csv, os.path.join(appdata_dir, "metadata.xlsx"))


def _log_progress(pipeline):
    if pipeline.metrics:
        files_received = pipeline.metrics.files_received
        files_quarantined = pipeline.metrics.files_quarantined
        
        progress_msg = f"Processed {format_number_with_commas(files_received)} files"
        if files_quarantined > 0:
            progress_msg += f" ({format_number_with_commas(files_quarantined)} quarantined)"
        
        logging.info(progress_msg)


def imageqr(pacs_list, query_spreadsheet, application_aet, 
            output_dir, appdata_dir=None, filter_script=None, date_window_days=0, debug=False, run_dirs=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info("Running imageqr")
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    quarantine_dir = os.path.join(appdata_dir, "quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)
    
    validate_date_window_days(date_window_days)
    
    query_params_list, generated_filter = generate_queries_and_filter(query_spreadsheet, date_window_days)
    combined_filter = combine_filters(filter_script, generated_filter)

    valid_pacs_list = find_valid_pacs_list(pacs_list, application_aet)
    
    study_pacs_map, failed_find_indices = find_studies_from_pacs_list(valid_pacs_list, query_params_list, application_aet)
    
    ctp_log_level = "DEBUG" if debug else None
    
    with CTPPipeline(
        pipeline_type="imageqr",
        output_dir=output_dir,
        application_aet=application_aet,
        filter_script=combined_filter,
        log_path=run_dirs["ctp_log_path"],
        log_level=ctp_log_level,
        quarantine_dir=quarantine_dir
    ) as pipeline:
        time.sleep(3)
        
        successful_moves, failed_move_indices = move_studies_from_study_pacs_map(study_pacs_map, application_aet)
        
        failed_query_indices = list(set(failed_find_indices + failed_move_indices))
        
        save_interval = 5
        last_save_time = 0
        
        while not pipeline.is_complete():
            current_time = time.time()
            if current_time - last_save_time >= save_interval:
                _save_metadata_files(pipeline, appdata_dir)
                _log_progress(pipeline)
                last_save_time = current_time
            
            time.sleep(1)
        
        _save_metadata_files(pipeline, appdata_dir)
        
        num_saved = pipeline.metrics.files_saved if pipeline.metrics else 0
        num_quarantined = pipeline.metrics.files_quarantined if pipeline.metrics else 0
        
        logging.info("Query and retrieval complete")
        logging.info(f"Total files processed: {format_number_with_commas(num_saved + num_quarantined)}")
        logging.info(f"Files saved: {format_number_with_commas(num_saved)}")
        logging.info(f"Files quarantined: {format_number_with_commas(num_quarantined)}")
        
        return {
            "num_studies_found": len(study_pacs_map),
            "num_images_saved": pipeline.metrics.files_saved if pipeline.metrics else 0,
            "num_images_quarantined": pipeline.metrics.files_quarantined if pipeline.metrics else 0,
            "failed_query_indices": failed_query_indices
        }

