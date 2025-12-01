import logging
import os
import shutil
import tempfile

from module_imagedeid_pacs import imagedeid_pacs
from module_image_export import image_export
from utils import setup_run_directories, configure_run_logging


def imagedeidexport(pacs_list, query_spreadsheet, application_aet,
                    sas_url, project_name, appdata_dir=None,
                    filter_script=None, date_window_days=0,
                    anonymizer_script=None, deid_pixels=False,
                    lookup_table=None, debug=False, run_dirs=None,
                    apply_default_filter_script=True, mapping_file_path=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info("Running imagedeidexport")
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(appdata_dir, exist_ok=True)
    
    temp_output_dir = tempfile.mkdtemp(prefix="imagedeidexport_")
    
    try:
        deid_result = imagedeid_pacs(
            pacs_list=pacs_list,
            query_spreadsheet=query_spreadsheet,
            application_aet=application_aet,
            output_dir=temp_output_dir,
            appdata_dir=appdata_dir,
            filter_script=filter_script,
            date_window_days=date_window_days,
            anonymizer_script=anonymizer_script,
            deid_pixels=deid_pixels,
            lookup_table=lookup_table,
            debug=debug,
            run_dirs=run_dirs,
            apply_default_filter_script=apply_default_filter_script,
            mapping_file_path=mapping_file_path
        )
        
        if deid_result["num_images_saved"] > 0:
            image_export(
                input_dir=temp_output_dir,
                sas_url=sas_url,
                project_name=project_name,
                appdata_dir=appdata_dir,
                debug=debug,
                run_dirs=run_dirs
            )
        
        logging.info("Export complete, cleaning up local files")
        shutil.rmtree(temp_output_dir)
        
        return {
            "num_studies_found": deid_result["num_studies_found"],
            "num_images_exported": deid_result["num_images_saved"],
            "num_images_quarantined": deid_result["num_images_quarantined"],
            "failed_query_indices": deid_result["failed_query_indices"]
        }
    except Exception as e:
        logging.error(f"Error during imagedeidexport: {str(e)}")
        if os.path.exists(temp_output_dir):
            shutil.rmtree(temp_output_dir)
        raise

