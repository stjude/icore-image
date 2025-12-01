import csv
import io
import logging
import os
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from openpyxl import Workbook

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
    total_queries = len(query_params_list)
    
    for pacs in pacs_list:
        logging.info(f"Querying PACS: {pacs.host}:{pacs.port} (AE: {pacs.aet})")
        
        for i, query_params in enumerate(query_params_list):
            logging.info(f"Queried {i} / {total_queries} rows")
            logging.debug(f"Processing Excel row {i + 1}")
            
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
                        logging.debug(f"Found study {study_uid} on PACS {pacs.host}:{pacs.port}")
                
                if not results:
                    logging.warning(f"No studies found for query {i}: {query_params}")
            except Exception as e:
                logging.error(f"Excel row {i + 1} failed after 4 retries. Moving on.")
                if i not in failed_query_indices:
                    failed_query_indices.append(i)
        
        logging.info(f"Queried {total_queries} / {total_queries} rows")
    
    logging.info(f"Found {len(study_pacs_map)} unique studies total")
    
    return study_pacs_map, failed_query_indices


def move_studies_from_study_pacs_map(study_pacs_map, application_aet):
    successful_moves = 0
    failed_query_indices = []
    total_studies = len(study_pacs_map)
    processed = 0
    
    for study_uid, (pacs, query_index) in study_pacs_map.items():
        logging.info(f"Moved {processed} / {total_studies} studies")
        logging.debug(f"Processing study from Excel row {query_index + 1}")
        
        result = move_study(
            host=pacs.host,
            port=pacs.port,
            calling_aet=application_aet,
            called_aet=pacs.aet,
            move_destination=application_aet,
            study_uid=study_uid
        )
        
        processed += 1
        
        if result["success"]:
            successful_moves += 1
            logging.debug(f"Successfully moved study {study_uid} from {pacs.host}:{pacs.port}")
        else:
            logging.error(f"Excel row {query_index + 1} failed after 4 retries. Moving on.")
            if query_index not in failed_query_indices:
                failed_query_indices.append(query_index)
    
    logging.info(f"Moved {processed} / {total_studies} studies")
    
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
            logging.FileHandler(log_file_path, mode='a'),
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


def csv_string_to_xlsx(csv_string, output_path):
    if not csv_string or csv_string.strip() == "":
        pd.DataFrame().to_excel(output_path, index=False, engine='openpyxl')
        return
    
    cleaned_csv = _clean_ctp_csv_format(csv_string)
    headers, data_rows = _parse_csv_to_rows(cleaned_csv)
    
    if not data_rows:
        pd.DataFrame(columns=headers).to_excel(output_path, index=False, engine='openpyxl')
        return
    
    df = _create_dataframe_with_dates(headers, data_rows)
    _write_excel_with_text_format(df, output_path)


def _clean_ctp_csv_format(csv_string):
    csv_string = csv_string.replace('=\"', '"').replace('\"', '"')
    csv_string = csv_string.replace('=("', '"').replace('")', '"')
    return csv_string


def _parse_csv_to_rows(csv_string):
    reader = csv.reader(io.StringIO(csv_string))
    rows = list(reader)
    
    if not rows:
        return [], []
    
    headers = rows[0]
    data_rows = rows[1:]
    
    normalized_rows = []
    for row in data_rows:
        normalized_row = [row[i].strip() if i < len(row) else '' for i in range(len(headers))]
        normalized_rows.append(normalized_row)
    
    return headers, normalized_rows


def _create_dataframe_with_dates(headers, data_rows):
    df = pd.DataFrame(data_rows, columns=headers, dtype=str)
    
    for col in df.columns:
        if _is_date_column(col):
            df[col] = df[col].apply(_parse_date_value)
    
    return df


def _is_date_column(column_name):
    return 'date' in column_name.lower()


def _parse_date_value(value):
    if pd.isna(value) or value == '' or value == 'nan':
        return value
    
    value_str = str(value).strip()
    date_formats = [
        ('%Y%m%d', 8),
        ('%Y-%m-%d', 10),
        ('%m/%d/%Y', 10)
    ]
    
    for date_format, expected_length in date_formats:
        if len(value_str) == expected_length:
            try:
                parsed_date = datetime.strptime(value_str, date_format)
                return pd.Timestamp(parsed_date)
            except ValueError:
                continue
    
    return value_str


def _write_excel_with_text_format(df, output_path):
    wb = Workbook()
    ws = wb.active
    
    ws.append(list(df.columns))
    
    for row_idx, row_data in df.iterrows():
        for col_idx, col_name in enumerate(df.columns, start=1):
            cell = ws.cell(row=row_idx + 2, column=col_idx)
            cell.value = row_data[col_name]
            
            if not _is_date_column(col_name):
                cell.number_format = '@'
    
    wb.save(output_path)


def validate_dicom_tags(tag_names):
    dictionary_path = os.path.join(os.path.dirname(__file__), "resources", "dictionary.xml")
    
    if not os.path.exists(dictionary_path):
        raise ValueError(f"DICOM dictionary not found at {dictionary_path}")
    
    tree = ET.parse(dictionary_path)
    root = tree.getroot()
    
    valid_keywords = set()
    for element in root.findall(".//element[@key]"):
        keyword = element.get("key")
        if keyword:
            valid_keywords.add(keyword)
    
    invalid_tags = []
    for tag_name in tag_names:
        if tag_name not in valid_keywords:
            invalid_tags.append(tag_name)
    
    if invalid_tags:
        raise ValueError(f"Invalid DICOM tag names: {', '.join(invalid_tags)}")


def detect_and_validate_dates(df, tag_name):
    if tag_name not in df.columns:
        return False
    
    sample_values = df[tag_name].dropna().head(5)
    if len(sample_values) == 0:
        return False
    
    is_date_column = False
    for value in sample_values:
        if isinstance(value, (pd.Timestamp, datetime)):
            is_date_column = True
            break
    
    if is_date_column:
        for idx, value in df[tag_name].items():
            if pd.notna(value) and not isinstance(value, (pd.Timestamp, datetime)):
                raise ValueError(f"Column {tag_name} has inconsistent date types at row {idx}: expected datetime, got {type(value).__name__}")
    
    return is_date_column


def format_dicom_date(date_value):
    if isinstance(date_value, pd.Timestamp):
        return date_value.strftime("%Y%m%d")
    elif isinstance(date_value, datetime):
        return date_value.strftime("%Y%m%d")
    else:
        raise ValueError(f"Cannot format non-date value: {date_value} (type: {type(date_value).__name__})")

