import logging
import os
import time

from ctp import CTPPipeline
from module_imagedeid_local import _save_metadata_files
from utils import (PacsConfiguration, Spreadsheet, generate_queries_and_filter, 
                   combine_filters, validate_date_window_days, find_studies_from_pacs_list,
                   move_studies_from_study_pacs_map, setup_run_directories, configure_run_logging)


def imagedeid_pacs(pacs_list, query_spreadsheet, application_aet, 
                   output_dir, appdata_dir=None, filter_script=None, 
                   date_window_days=0, anonymizer_script=None, deid_pixels=False,
                   lookup_table=None, debug=False):
    run_dirs = setup_run_directories()
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    validate_date_window_days(date_window_days)
    
    query_params_list, generated_filter = generate_queries_and_filter(query_spreadsheet, date_window_days)
    combined_filter = combine_filters(filter_script, generated_filter)
    
    study_pacs_map, failed_find_indices = find_studies_from_pacs_list(pacs_list, query_params_list, application_aet)
    
    pipeline_type = "imagedeid_pacs_pixel" if deid_pixels else "imagedeid_pacs"
    ctp_log_level = "DEBUG" if debug else None
    
    with CTPPipeline(
        pipeline_type=pipeline_type,
        output_dir=output_dir,
        application_aet=application_aet,
        filter_script=combined_filter,
        anonymizer_script=anonymizer_script,
        lookup_table=lookup_table,
        log_path=run_dirs["ctp_log_path"],
        log_level=ctp_log_level
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
                last_save_time = current_time
            
            time.sleep(1)
        
        _save_metadata_files(pipeline, appdata_dir)
        
        return {
            "num_studies_found": len(study_pacs_map),
            "num_images_saved": pipeline.metrics.files_saved if pipeline.metrics else 0,
            "num_images_quarantined": pipeline.metrics.files_quarantined if pipeline.metrics else 0,
            "failed_query_indices": failed_query_indices
        }

