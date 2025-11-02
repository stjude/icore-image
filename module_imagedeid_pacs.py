import os
import time

from ctp import CTPPipeline
from utils import (PacsConfiguration, Spreadsheet, generate_queries_and_filter, 
                   combine_filters, validate_date_window_days, find_studies_from_pacs_list,
                   move_studies_from_study_pacs_map)


def _save_metadata_files(pipeline, appdata_dir):
    audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
    if audit_log_csv:
        with open(os.path.join(appdata_dir, "metadata.csv"), "w") as f:
            f.write(audit_log_csv)
    
    deid_audit_log_csv = pipeline.get_audit_log_csv("DeidAuditLog")
    if deid_audit_log_csv:
        with open(os.path.join(appdata_dir, "deid_metadata.csv"), "w") as f:
            f.write(deid_audit_log_csv)
    
    linker_csv = pipeline.get_idmap_csv()
    if linker_csv:
        with open(os.path.join(appdata_dir, "linker.csv"), "w") as f:
            f.write(linker_csv)


def imagedeid_pacs(pacs_list, query_spreadsheet, application_aet, 
                   output_dir, appdata_dir, filter_script=None, 
                   anonymizer_script=None, date_window_days=0):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    validate_date_window_days(date_window_days)
    
    query_params_list, generated_filter = generate_queries_and_filter(query_spreadsheet, date_window_days)
    combined_filter = combine_filters(filter_script, generated_filter)
    
    study_pacs_map, failed_find_indices = find_studies_from_pacs_list(pacs_list, query_params_list, application_aet)
    
    input_dir = os.path.join(appdata_dir, "temp_input")
    os.makedirs(input_dir, exist_ok=True)
    
    with CTPPipeline(
        pipeline_type="imagedeid_pacs",
        input_dir=input_dir,
        output_dir=output_dir,
        application_aet=application_aet,
        filter_script=combined_filter,
        anonymizer_script=anonymizer_script
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

