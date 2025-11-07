import logging
import os
import time

from ctp import CTPPipeline
from utils import setup_run_directories, configure_run_logging, format_number_with_commas, count_dicom_files, csv_string_to_xlsx


def _save_metadata_files(pipeline, appdata_dir):
    audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
    if audit_log_csv:
        csv_string_to_xlsx(audit_log_csv, os.path.join(appdata_dir, "metadata.xlsx"))
    
    deid_audit_log_csv = pipeline.get_audit_log_csv("DeidAuditLog")
    if deid_audit_log_csv:
        csv_string_to_xlsx(deid_audit_log_csv, os.path.join(appdata_dir, "deid_metadata.xlsx"))
    
    linker_csv = pipeline.get_idmap_csv()
    if linker_csv:
        csv_string_to_xlsx(linker_csv, os.path.join(appdata_dir, "linker.xlsx"))


def _log_progress(total_files, pipeline):
    if pipeline.metrics:
        files_received = pipeline.metrics.files_received
        files_quarantined = pipeline.metrics.files_quarantined
        
        progress_msg = f"Processed {format_number_with_commas(files_received)} / {format_number_with_commas(total_files)} files"
        if files_quarantined > 0:
            progress_msg += f" ({format_number_with_commas(files_quarantined)} quarantined)"
        
        logging.info(progress_msg)


def imagedeid_local(input_dir, output_dir, appdata_dir=None, filter_script=None, 
                   anonymizer_script=None, deid_pixels=False, lookup_table=None, 
                   debug=False, run_dirs=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    logging.info("Starting image deidentification")
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Output directory: {output_dir}")
    if filter_script:
        logging.info(f"Filter script: {filter_script}")
    if anonymizer_script:
        logging.info(f"Anonymizer script: {anonymizer_script}")
    if lookup_table:
        logging.info(f"Lookup table: {lookup_table}")
    logging.info(f"Pixel deidentification: {'enabled' if deid_pixels else 'disabled'}")
    
    logging.info("Counting input files...")
    total_files = count_dicom_files(input_dir)
    logging.info(f"Found {format_number_with_commas(total_files)} files to process")
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    quarantine_dir = os.path.join(appdata_dir, "quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)
    
    pipeline_type = "imagedeid_local_pixel" if deid_pixels else "imagedeid_local"
    ctp_log_level = "DEBUG" if debug else None
    
    with CTPPipeline(
        pipeline_type=pipeline_type,
        output_dir=output_dir,
        input_dir=input_dir,
        filter_script=filter_script,
        anonymizer_script=anonymizer_script,
        lookup_table=lookup_table,
        log_path=run_dirs["ctp_log_path"],
        log_level=ctp_log_level,
        quarantine_dir=quarantine_dir
    ) as pipeline:
        time.sleep(3)
        
        save_interval = 5
        last_save_time = 0
        
        while not pipeline.is_complete():
            current_time = time.time()
            if current_time - last_save_time >= save_interval:
                _save_metadata_files(pipeline, appdata_dir)
                _log_progress(total_files, pipeline)
                last_save_time = current_time
            
            time.sleep(1)
        
        _save_metadata_files(pipeline, appdata_dir)
        
        num_saved = pipeline.metrics.files_saved if pipeline.metrics else 0
        num_quarantined = pipeline.metrics.files_quarantined if pipeline.metrics else 0
        
        logging.info("Deidentification complete")
        logging.info(f"Total files processed: {format_number_with_commas(num_saved + num_quarantined)}")
        logging.info(f"Files saved: {format_number_with_commas(num_saved)}")
        logging.info(f"Files quarantined: {format_number_with_commas(num_quarantined)}")
        
        return {
            "num_images_saved": num_saved,
            "num_images_quarantined": num_quarantined
        }

