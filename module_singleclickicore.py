import logging
import os

from module_imagedeid_pacs import imagedeid_pacs
from module_textdeid import textdeid
from module_image_export import image_export
from utils import setup_run_directories, configure_run_logging


def singleclickicore(pacs_list, query_spreadsheet, application_aet,
                     sas_url, project_name, output_dir, input_file,
                     appdata_dir=None,
                     filter_script=None, date_window_days=0,
                     anonymizer_script=None, deid_pixels=False,
                     lookup_table=None, apply_default_filter_script=True,
                     mapping_file_path=None,
                     to_keep_list=None, to_remove_list=None,
                     columns_to_drop=None, columns_to_deid=None,
                     debug=False, run_dirs=None):
    """
    Combined module that performs:
    1. Image deidentification from PACS
    2. Text deidentification on input Excel file
    3. Export of all output to Azure Blob Storage
    
    Args:
        pacs_list: List of PacsConfiguration objects
        query_spreadsheet: Spreadsheet object for PACS querying
        application_aet: Application AE Title for DICOM communication
        sas_url: SAS URL for Azure container
        project_name: Project name (used as folder prefix in blob storage)
        output_dir: Output directory for deidentified files
        input_file: Path to input Excel file (for text deid)
        appdata_dir: Application data directory for logs/metadata
        filter_script: CTP filter script for image deid
        date_window_days: Date window for PACS queries
        anonymizer_script: CTP anonymizer script
        deid_pixels: Whether to deidentify pixels
        lookup_table: CTP lookup table
        apply_default_filter_script: Whether to apply default CTP filter
        mapping_file_path: Path to mapping file for value substitution
        to_keep_list: List of terms to preserve during text deid
        to_remove_list: List of terms to redact during text deid
        columns_to_drop: List of columns to drop from Excel
        columns_to_deid: List of columns to deidentify in Excel
        debug: Enable debug logging
        run_dirs: Run directories dictionary
        
    Returns:
        dict: Combined results from all steps
    """
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info("Running singleclickicore")
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(appdata_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Image deid from PACS
    logging.info("="*80)
    logging.info("STEP 1: Image Deidentification from PACS")
    logging.info("="*80)
    
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
        mapping_file_path=mapping_file_path
    )
    
    # Step 2: Text deid on input Excel
    logging.info("="*80)
    logging.info("STEP 2: Text Deidentification on Input File")
    logging.info("="*80)
    
    text_result = textdeid(
        input_file=input_file,
        output_dir=output_dir,
        to_keep_list=to_keep_list,
        to_remove_list=to_remove_list,
        columns_to_drop=columns_to_drop,
        columns_to_deid=columns_to_deid,
        debug=debug,
        run_dirs=run_dirs
    )
    
    # Step 3: Export to Azure (only if we have content to export)
    logging.info("="*80)
    logging.info("STEP 3: Export to Azure Blob Storage")
    logging.info("="*80)
    
    if deid_result["num_images_saved"] > 0 or text_result["num_rows_processed"] > 0:
        image_export(
            input_dir=output_dir,
            sas_url=sas_url,
            project_name=project_name,
            appdata_dir=appdata_dir,
            debug=debug,
            run_dirs=run_dirs
        )
    else:
        logging.info("No content to export - skipping Azure upload")
    
    logging.info("="*80)
    logging.info("singleclickicore complete")
    logging.info(f"Deidentified files preserved at: {output_dir}")
    logging.info("="*80)
    
    return {
        "num_studies_found": deid_result["num_studies_found"],
        "num_images_exported": deid_result["num_images_saved"],
        "num_images_quarantined": deid_result["num_images_quarantined"],
        "failed_query_indices": deid_result["failed_query_indices"],
        "num_rows_processed": text_result["num_rows_processed"],
        "output_file": text_result["output_file"]
    }
