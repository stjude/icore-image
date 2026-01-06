import logging
import os
import time

from ctp import CTPPipeline
from progress_tracker import ProgressTracker
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


def _imageqr_single_attempt(pacs_list, query_spreadsheet, application_aet, 
                            output_dir, appdata_dir, filter_script, date_window_days, 
                            debug, run_dirs, progress_tracker, query_params_list, 
                            combined_filter, quarantine_dir, move_delay_seconds):
    """Single attempt at running imageqr with CTP pipeline"""
    valid_pacs_list = find_valid_pacs_list(pacs_list, application_aet)
    
    study_pacs_map, failed_find_indices = find_studies_from_pacs_list(
        valid_pacs_list, query_params_list, application_aet, progress_tracker=progress_tracker
    )
    
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
        
        successful_moves, failed_move_indices = move_studies_from_study_pacs_map(
            study_pacs_map, application_aet, 
            progress_tracker=progress_tracker,
            move_delay_seconds=move_delay_seconds
        )
        
        failed_query_indices = list(set(failed_find_indices + failed_move_indices))
        
        save_interval = 5
        last_save_time = 0
        
        while not pipeline.is_complete():
            current_time = time.time()
            if current_time - last_save_time >= save_interval:
                _save_metadata_files(pipeline, appdata_dir)
                _log_progress(pipeline)
                
                # Save progress periodically
                progress_tracker.save_progress(appdata_dir)
                
                last_save_time = current_time
            
            # Check CTP health explicitly to catch monitor thread exceptions
            pipeline.server.check_health()
            
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


def imageqr(pacs_list, query_spreadsheet, application_aet, 
            output_dir, appdata_dir=None, filter_script=None, date_window_days=0, debug=False, run_dirs=None, max_restart_attempts=10, move_delay_seconds=2.0):
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
    
    total_rows = len(query_params_list)
    
    # Automatic restart loop
    for attempt in range(max_restart_attempts):
        # Load progress tracker to support resume
        progress_tracker = ProgressTracker.load_progress(appdata_dir)
        stats = progress_tracker.get_stats()
        
        if stats["total_rows_completed"] > 0:
            pending_rows = progress_tracker.get_pending_rows(total_rows)
            logging.info(f"Attempt {attempt + 1}: Resuming from previous run - "
                        f"{stats['total_rows_completed']}/{total_rows} rows completed, "
                        f"{len(pending_rows)} rows remaining, "
                        f"{stats['total_studies_downloaded']} studies downloaded, "
                        f"{stats['total_files_downloaded']} files downloaded")
        else:
            logging.info(f"Attempt {attempt + 1}: Starting fresh")
        
        # Check if all rows are complete
        if stats["total_rows_completed"] >= total_rows:
            logging.info("All rows completed successfully!")
            
            # Clean up progress file
            progress_file = os.path.join(appdata_dir, ".icore_progress.json")
            if os.path.exists(progress_file):
                os.remove(progress_file)
                logging.info("Progress file cleaned up after successful completion")
            
            # Return final results
            return {
                "num_studies_found": stats["total_studies_downloaded"],
                "num_images_saved": stats["total_files_downloaded"],
                "num_images_quarantined": 0,
                "failed_query_indices": []
            }
        
        try:
            result = _imageqr_single_attempt(
                pacs_list, query_spreadsheet, application_aet, 
                output_dir, appdata_dir, filter_script, date_window_days, 
                debug, run_dirs, progress_tracker, query_params_list, 
                combined_filter, quarantine_dir, move_delay_seconds
            )
            
            # If we get here, the attempt completed successfully
            # Clean up progress file
            progress_file = os.path.join(appdata_dir, ".icore_progress.json")
            if os.path.exists(progress_file):
                os.remove(progress_file)
                logging.info("Progress file cleaned up after successful completion")
            
            return result
        
        except TimeoutError as e:
            # CTP stalled - save progress and restart automatically
            logging.warning(f"Attempt {attempt + 1} timed out: {e}")
            progress_tracker.save_progress(appdata_dir)
            logging.info(f"Progress saved. Automatically restarting (attempt {attempt + 2}/{max_restart_attempts})...")
            time.sleep(5)  # Brief pause before restart
            continue
        
        except Exception as e:
            # Unexpected error - save progress and restart
            logging.error(f"Attempt {attempt + 1} failed with error: {e}")
            progress_tracker.save_progress(appdata_dir)
            logging.info(f"Progress saved. Automatically restarting (attempt {attempt + 2}/{max_restart_attempts})...")
            time.sleep(5)  # Brief pause before restart
            continue
    
    # Max attempts reached
    logging.error(f"Failed to complete after {max_restart_attempts} attempts")
    raise RuntimeError(f"imageqr failed to complete after {max_restart_attempts} automatic restart attempts")

