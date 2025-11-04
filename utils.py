import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

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


def generate_queries_and_filter(spreadsheet, date_window_days=0):
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


def combine_filters(user_filter, generated_filter):
    combined_filter = None
    if user_filter and generated_filter:
        combined_filter = f"({user_filter}) * ({generated_filter})"
    elif user_filter or generated_filter:
        combined_filter = user_filter or generated_filter
    return combined_filter


def validate_date_window_days(date_window_days):
    if date_window_days < 0 or date_window_days > 10:
        raise ValueError(f"date_window_days must be between 0 and 10, got {date_window_days}")


def find_studies_from_pacs_list(pacs_list, query_params_list, application_aet):
    study_pacs_map = {}
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
                    if study_uid and study_uid not in study_pacs_map:
                        study_pacs_map[study_uid] = (pacs, i)
                        logging.info(f"Found study {study_uid} on PACS {pacs.host}:{pacs.port}")
                
                if not results:
                    logging.warning(f"No studies found for query {i}: {query_params}")
            except Exception as e:
                logging.error(f"Query {i} failed: {e}")
                if i not in failed_query_indices:
                    failed_query_indices.append(i)
    
    logging.info(f"Found {len(study_pacs_map)} unique studies total")
    
    return study_pacs_map, failed_query_indices


def move_studies_from_study_pacs_map(study_pacs_map, application_aet):
    successful_moves = 0
    failed_query_indices = []
    
    for study_uid, (pacs, query_index) in study_pacs_map.items():
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
            if query_index not in failed_query_indices:
                failed_query_indices.append(query_index)
    
    return successful_moves, failed_query_indices


def setup_run_directories():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    icore_base = os.path.expanduser("~/Documents/iCore")
    log_dir = os.path.join(icore_base, "logs", timestamp)
    appdata_dir = os.path.join(icore_base, "appdata", timestamp)
    
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    ctp_log_path = os.path.join(log_dir, "ctp.txt")
    run_log_path = os.path.join(log_dir, "run.txt")
    
    return {
        "log_dir": log_dir,
        "ctp_log_path": ctp_log_path,
        "run_log_path": run_log_path,
        "appdata_dir": appdata_dir
    }


def configure_run_logging(log_file_path, log_level=logging.INFO):
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file_path, mode='w'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )


def format_number_with_commas(num):
    return f"{num:,}"


def count_dicom_files(directory):
    count = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.dcm'):
                count += 1
    return count

