import json
import logging
import os

import pandas as pd
import pydicom

from utils import configure_run_logging, format_number_with_commas, setup_run_directories


def _find_dicom_files(input_dir):
    dicom_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.dcm'):
                file_path = os.path.join(root, file)
                dicom_files.append(file_path)
    return dicom_files


def _extract_header_value(ds, header_name):
    try:
        value = getattr(ds, header_name, "")
        if value is None:
            return ""
        return str(value)
    except:
        return ""


def _extract_headers_from_file(file_path, headers_to_extract):
    try:
        ds = pydicom.dcmread(file_path, stop_before_pixels=True)
        
        header_data = {}
        for header in headers_to_extract:
            header_data[header] = _extract_header_value(ds, header)
        
        return header_data
    except Exception as e:
        logging.warning(f"Failed to read DICOM file {file_path}: {e}")
        return None


def _aggregate_by_study(all_headers):
    df = pd.DataFrame(all_headers)
    
    if df.empty:
        return df
    
    if "StudyInstanceUID" not in df.columns:
        return df
    
    study_groups = df.groupby("StudyInstanceUID", dropna=False)
    aggregated_data = []
    
    for study_uid, group in study_groups:
        study_data = {}
        for col in group.columns:
            values = group[col].dropna().unique()
            if len(values) > 0:
                if len(values) > 1:
                    study_data[col] = json.dumps([str(v) for v in values])
                else:
                    study_data[col] = str(values[0])
            else:
                study_data[col] = ""
        aggregated_data.append(study_data)
    
    return pd.DataFrame(aggregated_data)


def headerextraction_local(input_dir, output_dir, headers_to_extract=None,
                     extract_all_headers=False, debug=False, run_dirs=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    logging.info("Starting header extraction")
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Extract all headers: {extract_all_headers}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info("Finding DICOM files...")
    dicom_files = _find_dicom_files(input_dir)
    total_files = len(dicom_files)
    logging.info(f"Found {format_number_with_commas(total_files)} DICOM files")
    
    if headers_to_extract:
        logging.info(f"Extracting custom headers: {headers_to_extract}")
    elif extract_all_headers:
        logging.info("Extracting all headers")
        headers_to_extract = None
    else:
        raise ValueError("Must provide either headers_to_extract or set extract_all_headers=True")
    
    if headers_to_extract and "StudyInstanceUID" not in headers_to_extract:
        headers_to_extract.append("StudyInstanceUID")
    
    all_headers = []
    files_processed = 0
    
    for i, file_path in enumerate(dicom_files):
        if extract_all_headers:
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                header_data = {}
                for attr_name in ds.dir():
                    try:
                        if not attr_name.startswith('_'):
                            value = getattr(ds, attr_name, None)
                            if value is not None:
                                header_data[attr_name] = str(value)
                    except:
                        pass
                if header_data:
                    all_headers.append(header_data)
                    files_processed += 1
            except Exception as e:
                logging.warning(f"Failed to read DICOM file {file_path}: {e}")
        else:
            header_data = _extract_headers_from_file(file_path, headers_to_extract)
            if header_data:
                all_headers.append(header_data)
                files_processed += 1
        
        if (i + 1) % 100 == 0:
            logging.info(f"Processed {format_number_with_commas(i + 1)} / {format_number_with_commas(total_files)} files")
    
    logging.info("Aggregating headers by study...")
    df = _aggregate_by_study(all_headers)
    
    metadata_path = os.path.join(output_dir, "metadata.xlsx")
    df.to_excel(metadata_path, index=False, engine='openpyxl')
    logging.info(f"Saved metadata to {metadata_path}")
    
    logging.info("Header extraction complete")
    logging.info(f"Total files processed: {format_number_with_commas(files_processed)}")
    logging.info(f"Total studies: {len(df)}")
    
    return {
        "num_files_processed": files_processed,
        "num_studies": len(df)
    }

