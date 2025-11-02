import os
import time

from ctp import CTPPipeline


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


def imagedeid_local(input_dir, output_dir, appdata_dir, filter_script=None, 
                   anonymizer_script=None, deid_pixels=False):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    pipeline_type = "imagedeid_local_pixel" if deid_pixels else "imagedeid_local"
    
    with CTPPipeline(
        pipeline_type=pipeline_type,
        input_dir=input_dir,
        output_dir=output_dir,
        filter_script=filter_script,
        anonymizer_script=anonymizer_script
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

