import logging
import os

from module_imagedeid_pacs import imagedeid_pacs
from module_image_export import image_export
from utils import setup_run_directories, configure_run_logging


def imagedeidexport(pacs_list, query_spreadsheet, application_aet,
                    sas_url, project_name, output_dir, appdata_dir=None,
                    filter_script=None, date_window_days=0,
                    anonymizer_script=None, deid_pixels=False,
                    lookup_table=None, debug=False, run_dirs=None,
                    apply_default_filter_script=True, mapping_file_path=None,
                    use_fallback_query=False):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info("Running imagedeidexport")
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(appdata_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    deid_result = imagedeid_pacs(
        pacs_list=pacs_list,
        query_spreadsheet=query_spreadsheet,
        application_aet=application_aet,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        filter_script=filter_script,
        date_window_days=date_window_days,
        anonymizer_script=anonymizer_script,
        deid_pixels=deid_pixels,
        lookup_table=lookup_table,
        debug=debug,
        run_dirs=run_dirs,
        apply_default_filter_script=apply_default_filter_script,
        mapping_file_path=mapping_file_path,
        use_fallback_query=use_fallback_query
    )
    
    if deid_result["num_images_saved"] > 0:
        image_export(
            input_dir=output_dir,
            sas_url=sas_url,
            project_name=project_name,
            appdata_dir=appdata_dir,
            debug=debug,
            run_dirs=run_dirs
        )
    
    logging.info("Deidentification and export complete")
    logging.info(f"Deidentified files preserved at: {output_dir}")
    
    return {
        "num_studies_found": deid_result["num_studies_found"],
        "num_images_exported": deid_result["num_images_saved"],
        "num_images_quarantined": deid_result["num_images_quarantined"],
        "failed_query_indices": deid_result["failed_query_indices"]
    }

