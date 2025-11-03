import logging
import os
import time

from ctp import CTPPipeline
from utils import setup_run_directories, configure_run_logging


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


def imagedeid_local(input_dir, output_dir, appdata_dir=None, filter_script=None, 
                   anonymizer_script=None, deid_pixels=False, lookup_table=None, 
                   debug=False):
    run_dirs = setup_run_directories()
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
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
        log_level=ctp_log_level
    ) as pipeline:
        time.sleep(3)
        
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
            "num_images_saved": pipeline.metrics.files_saved if pipeline.metrics else 0,
            "num_images_quarantined": pipeline.metrics.files_quarantined if pipeline.metrics else 0
        }

