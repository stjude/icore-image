import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd

from ctp import CTPPipeline
from dcmtk import find_studies, move_study


@dataclass
class PacsConfiguration:
    host: str
    port: int
    aet: str


@dataclass
class Spreadsheet:
    dataframe: pd.DataFrame
    acc_col: str = None
    mrn_col: str = None
    date_col: str = None
    
    @classmethod
    def from_file(cls, path, acc_col=None, mrn_col=None, date_col=None):
        if path.endswith('.xlsx'):
            df = pd.read_excel(path)
        elif path.endswith('.csv'):
            df = pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported file format: {path}")
        
        return cls(dataframe=df, acc_col=acc_col, mrn_col=mrn_col, date_col=date_col)


def _generate_queries_and_filter(spreadsheet, date_window_days=0):
    query_params_list = []
    filter_conditions = []
    
    for _, row in spreadsheet.dataframe.iterrows():
        if spreadsheet.acc_col and pd.notna(row.get(spreadsheet.acc_col)):
            acc = str(row[spreadsheet.acc_col])
            query_params = {"AccessionNumber": f"*{acc}*"}
            query_params_list.append(query_params)
            filter_conditions.append(f'AccessionNumber.contains("{acc}")')
        elif (spreadsheet.mrn_col and spreadsheet.date_col and 
              pd.notna(row.get(spreadsheet.mrn_col)) and 
              pd.notna(row.get(spreadsheet.date_col))):
            mrn = str(row[spreadsheet.mrn_col])
            study_date = row[spreadsheet.date_col]
            if not isinstance(study_date, pd.Timestamp):
                raise ValueError(f"StudyDate must be in Excel date format (pd.Timestamp), got {type(study_date).__name__}: {study_date}")
            
            start_date = study_date - timedelta(days=date_window_days)
            end_date = study_date + timedelta(days=date_window_days)
            
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")
            
            query_params = {
                "PatientID": mrn,
                "StudyDate": f"{start_date_str}-{end_date_str}"
            }
            query_params_list.append(query_params)
            
            start_minus_one = (start_date - timedelta(days=1)).strftime("%Y%m%d")
            end_plus_one = (end_date + timedelta(days=1)).strftime("%Y%m%d")
            filter_conditions.append(f'(PatientID.contains("{mrn}") * StudyDate.isGreaterThan("{start_minus_one}") * StudyDate.isLessThan("{end_plus_one}"))')
        else:
            raise ValueError(f"Row must have either acc_col or both mrn_col and date_col with valid values")
    
    generated_filter = " + ".join(filter_conditions) if filter_conditions else None
    
    return query_params_list, generated_filter


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
    
    if date_window_days < 0 or date_window_days > 10:
        raise ValueError(f"date_window_days must be between 0 and 10, got {date_window_days}")
    
    query_params_list, generated_filter = _generate_queries_and_filter(query_spreadsheet, date_window_days)
    
    combined_filter = None
    if filter_script and generated_filter:
        combined_filter = f"({filter_script}) * ({generated_filter})"
    elif filter_script or generated_filter:
        combined_filter = filter_script or generated_filter
    
    study_to_pacs = {}
    failed_query_indices = []
    
    for pacs in pacs_list:
        logging.info(f"Querying PACS: {pacs.host}:{pacs.port} (AE: {pacs.aet})")
        
        for i, query_params in enumerate(query_params_list):
            try:
                results = find_studies(
                    host=pacs.host,
                    port=pacs.port,
                    calling_aet=application_aet,
                    called_aet=pacs.aet,
                    query_params=query_params,
                    return_tags=["StudyInstanceUID", "StudyDate"]
                )
                
                for result in results:
                    study_uid = result.get("StudyInstanceUID")
                    if study_uid and study_uid not in study_to_pacs:
                        study_to_pacs[study_uid] = (pacs, i)
                        logging.info(f"Found study {study_uid} on PACS {pacs.host}:{pacs.port}")
                
                if not results:
                    logging.warning(f"No studies found for query {i}: {query_params}")
            except Exception as e:
                logging.error(f"Query {i} failed: {e}")
                if i not in failed_query_indices:
                    failed_query_indices.append(i)
    
    logging.info(f"Found {len(study_to_pacs)} unique studies total")
    
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
        
        successful_moves = 0
        for study_uid, (pacs, query_index) in study_to_pacs.items():
            result = move_study(
                host=pacs.host,
                port=pacs.port,
                calling_aet=application_aet,
                called_aet=pacs.aet,
                move_destination=application_aet,
                study_uid=study_uid
            )
            
            if result["success"]:
                successful_moves += 1
                logging.info(f"Successfully moved study {study_uid} from {pacs.host}:{pacs.port}")
            else:
                logging.warning(f"Failed to move study {study_uid} from {pacs.host}:{pacs.port}")
        
        start_time = time.time()
        timeout = 300
        save_interval = 5
        last_save_time = 0
        
        while not pipeline.is_complete():
            if time.time() - start_time > timeout:
                raise TimeoutError("Pipeline did not complete within timeout")
            
            current_time = time.time()
            if current_time - last_save_time >= save_interval:
                _save_metadata_files(pipeline, appdata_dir)
                last_save_time = current_time
            
            time.sleep(1)
        
        _save_metadata_files(pipeline, appdata_dir)
        
        return {
            "num_studies_found": len(study_to_pacs),
            "num_images_saved": pipeline.metrics.files_saved if pipeline.metrics else 0,
            "num_images_quarantined": pipeline.metrics.files_quarantined if pipeline.metrics else 0,
            "failed_query_indices": failed_query_indices
        }

