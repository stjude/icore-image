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

from dcmtk import find_studies, get_study, echo_pacs


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


def _build_mrn_date_query_and_filter(mrn, study_date, date_window_days):
    start_date = study_date - timedelta(days=date_window_days)
    end_date = study_date + timedelta(days=date_window_days)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    query_params = {
        "PatientID": mrn,
        "StudyDate": f"{start_date_str}-{end_date_str}"
    }

    start_minus_one = (start_date - timedelta(days=1)).strftime("%Y%m%d")
    end_plus_one = (end_date + timedelta(days=1)).strftime("%Y%m%d")
    filter_condition = (
        f'(PatientID.contains("{mrn}") * '
        f'StudyDate.isGreaterThan("{start_minus_one}") * '
        f'StudyDate.isLessThan("{end_plus_one}"))'
    )

    return query_params, filter_condition


def generate_queries_and_filter(spreadsheet, date_window_days=0, use_fallback_query=False):
    query_params_list = []
    expected_values_list = []
    filter_conditions = []

    for i, row in spreadsheet.dataframe.iterrows():
        if spreadsheet.acc_col and pd.notna(row.get(spreadsheet.acc_col)):
            acc = str(row[spreadsheet.acc_col]).strip()
            query_params = {"AccessionNumber": f"*{acc}*"}
            query_params_list.append(query_params)
            expected_values_list.append((acc, len(query_params_list) - 1))
            filter_conditions.append(f'AccessionNumber.contains("{acc}")')

            if (use_fallback_query and spreadsheet.mrn_col and spreadsheet.date_col):
                mrn = row.get(spreadsheet.mrn_col)
                study_date = row.get(spreadsheet.date_col)
                if pd.notna(mrn) and pd.notna(study_date) and isinstance(study_date, pd.Timestamp):
                    _, filter_condition = _build_mrn_date_query_and_filter(str(mrn), study_date, date_window_days)
                    filter_conditions.append(filter_condition)
        elif (spreadsheet.mrn_col and spreadsheet.date_col and
              pd.notna(row.get(spreadsheet.mrn_col)) and
              pd.notna(row.get(spreadsheet.date_col))):
            mrn = str(row[spreadsheet.mrn_col])
            study_date = row[spreadsheet.date_col]
            if not isinstance(study_date, pd.Timestamp):
                raise ValueError(f"StudyDate must be in Excel date format (pd.Timestamp), got {type(study_date).__name__}: {study_date}")

            query_params, filter_condition = _build_mrn_date_query_and_filter(mrn, study_date, date_window_days)
            query_params_list.append(query_params)
            filter_conditions.append(filter_condition)
        else:
            raise ValueError(f"Row must have either acc_col or both mrn_col and date_col with valid values")

    generated_filter = " + ".join(filter_conditions) if filter_conditions else None

    return query_params_list, expected_values_list, generated_filter


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


def save_failed_queries_csv(failed_query_indices, query_spreadsheet, appdata_dir, failure_reasons,
                            use_fallback_query=False):
    csv_path = os.path.join(appdata_dir, "failed_queries.csv")

    has_accession = query_spreadsheet.acc_col is not None
    has_mrn = query_spreadsheet.mrn_col is not None
    has_date = query_spreadsheet.date_col is not None

    if use_fallback_query and has_accession and has_mrn and has_date:
        headers = ["Accession Number", "MRN", "Date", "Failure Reason"]
    elif has_accession and has_mrn:
        headers = ["Accession Number", "MRN", "Failure Reason"]
    elif has_accession:
        headers = ["Accession Number", "Failure Reason"]
    elif has_mrn and has_date:
        headers = ["MRN", "Date", "Failure Reason"]
    else:
        raise ValueError("Spreadsheet must have either acc_col or both mrn_col and date_col")
    
    rows = []
    for index in failed_query_indices:
        if index < 0 or index >= len(query_spreadsheet.dataframe):
            logging.warning(f"Invalid query index {index}, skipping")
            continue
        
        row_data = query_spreadsheet.dataframe.iloc[index]
        csv_row = []
        
        if has_accession:
            acc = row_data.get(query_spreadsheet.acc_col)
            csv_row.append(str(acc) if pd.notna(acc) else "")
        
        if has_mrn:
            mrn = row_data.get(query_spreadsheet.mrn_col)
            csv_row.append(str(mrn) if pd.notna(mrn) else "")
        
        if has_date:
            date_val = row_data.get(query_spreadsheet.date_col)
            if pd.notna(date_val):
                if isinstance(date_val, pd.Timestamp):
                    csv_row.append(date_val.strftime("%Y-%m-%d"))
                else:
                    csv_row.append(str(date_val))
            else:
                csv_row.append("")
        
        failure_reason = failure_reasons.get(index, "Unknown failure")
        csv_row.append(failure_reason)
        
        rows.append(csv_row)
    
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def find_valid_pacs_list(pacs_list, application_aet):
    valid_pacs_list = []
    for pacs in pacs_list:
        try:
            result = echo_pacs(pacs.host, pacs.port, application_aet, pacs.aet)
            if result["success"]:
                valid_pacs_list.append(pacs)
            else:
                logging.warning(f"Failed to ping PACS {pacs.host}:{pacs.port} (AE: {pacs.aet}): {result['message']}")
                continue
        except Exception as e:
            logging.warning(f"Failed to ping PACS {pacs.host}:{pacs.port} (AE: {pacs.aet}): {e}")
            continue
    return valid_pacs_list


def find_studies_from_pacs_list(pacs_list, query_params_list, application_aet,
                                expected_values_list=None, fallback_spreadsheet=None,
                                fallback_date_window_days=0):
    study_pacs_map = {}
    failed_query_indices = []
    failure_details = {}
    total_queries = len(query_params_list)

    expected_accessions_map = {}
    if expected_values_list:
        for expected_acc, query_index in expected_values_list:
            expected_accessions_map[query_index] = expected_acc

    if len(pacs_list) == 0:
        logging.warning("No valid PACS found")
        failed_query_indices = list(range(total_queries))
        for i in failed_query_indices:
            failure_details[i] = "Failed to find images"
        return study_pacs_map, failed_query_indices, failure_details

    for pacs in pacs_list:
        logging.info(f"Querying PACS: {pacs.host}:{pacs.port} (AE: {pacs.aet})")

        for i, query_params in enumerate(query_params_list):
            logging.info(f"Queried {i} / {total_queries} rows")
            logging.debug(f"Processing Excel row {i + 1}")

            try:
                return_tags = ["StudyInstanceUID", "StudyDate"]
                if i in expected_accessions_map:
                    return_tags.append("AccessionNumber")

                results = find_studies(
                    host=pacs.host,
                    port=pacs.port,
                    calling_aet=application_aet,
                    called_aet=pacs.aet,
                    query_params=query_params,
                    return_tags=return_tags
                )

                for result in results:
                    study_uid = result.get("StudyInstanceUID")
                    if not study_uid or study_uid in study_pacs_map:
                        continue

                    if i in expected_accessions_map:
                        result_acc = result.get("AccessionNumber", "").strip()
                        expected_acc = expected_accessions_map[i]
                        if result_acc != expected_acc:
                            logging.debug(f"Rejecting study {study_uid}: AccessionNumber '{result_acc}' does not match expected '{expected_acc}'")
                            continue

                    study_pacs_map[study_uid] = (pacs, i)
                    logging.debug(f"Found study {study_uid} on PACS {pacs.host}:{pacs.port}")

                    # Clear any prior failure for this query index since we found a study
                    if i in failed_query_indices:
                        failed_query_indices.remove(i)
                        failure_details.pop(i, None)

                if not results:
                    logging.warning(f"No studies found for query {i + 1}: {query_params}")
            except Exception as e:
                logging.exception(f"Failed to find studies for query {i + 1}: {str(e)}. Moving on.")
                if i not in failed_query_indices:
                    failed_query_indices.append(i)
                    failure_details[i] = f"Failed to find images: {str(e)}"

        logging.info(f"Queried {total_queries} / {total_queries} rows")

    query_indices_with_studies = set(query_index for _, query_index in study_pacs_map.values())
    for i in range(total_queries):
        if i not in query_indices_with_studies and i not in failed_query_indices:
            failed_query_indices.append(i)
            failure_details[i] = "Failed to find images"

    logging.info(f"Found {len(study_pacs_map)} unique studies total")

    if fallback_spreadsheet is None or not failed_query_indices:
        return study_pacs_map, failed_query_indices, failure_details

    return _attempt_fallback_queries(
        pacs_list, application_aet, study_pacs_map,
        failed_query_indices, failure_details,
        fallback_spreadsheet, fallback_date_window_days)


def _attempt_fallback_queries(pacs_list, application_aet, study_pacs_map,
                               failed_indices, failure_details,
                               spreadsheet, date_window_days):
    logging.info(f"Fallback: {len(failed_indices)} accession queries failed, checking fallback data (mrn_col='{spreadsheet.mrn_col}', date_col='{spreadsheet.date_col}')")

    if not (spreadsheet.mrn_col and spreadsheet.date_col):
        logging.warning("Fallback: mrn_col or date_col is empty/None, cannot attempt fallback")
        for i in failed_indices:
            failure_details[i] = failure_details.get(i, "Failed to find images") + " (no fallback data available)"
        return study_pacs_map, failed_indices, failure_details

    df_columns = list(spreadsheet.dataframe.columns)
    if spreadsheet.mrn_col not in df_columns:
        logging.warning(f"Fallback: mrn_col '{spreadsheet.mrn_col}' not found in spreadsheet columns: {df_columns}")
    if spreadsheet.date_col not in df_columns:
        logging.warning(f"Fallback: date_col '{spreadsheet.date_col}' not found in spreadsheet columns: {df_columns}")

    fallback_queries = []
    fallback_index_map = {}

    for original_index in failed_indices:
        if original_index >= len(spreadsheet.dataframe):
            continue
        row = spreadsheet.dataframe.iloc[original_index]
        mrn = row.get(spreadsheet.mrn_col)
        study_date = row.get(spreadsheet.date_col)

        if pd.notna(mrn) and pd.notna(study_date) and isinstance(study_date, pd.Timestamp):
            mrn = str(mrn)
            fallback_query, _ = _build_mrn_date_query_and_filter(mrn, study_date, date_window_days)
            fallback_index = len(fallback_queries)
            fallback_queries.append(fallback_query)
            fallback_index_map[fallback_index] = original_index
            logging.debug(f"Fallback: row {original_index} → MRN={mrn}, DateRange={fallback_query['StudyDate']}")
        else:
            logging.warning(f"Fallback: row {original_index} skipped - mrn={mrn} (notna={pd.notna(mrn)}), date={study_date} (notna={pd.notna(study_date)}, is_timestamp={isinstance(study_date, pd.Timestamp)})")
            failure_details[original_index] = failure_details.get(original_index, "Failed to find images") + " (no fallback data available)"

    if not fallback_queries:
        logging.warning("Fallback: no valid fallback queries could be generated from failed rows")
        return study_pacs_map, failed_indices, failure_details

    logging.info(f"Attempting fallback MRN+date queries for {len(fallback_queries)} failed accession queries")

    fallback_study_map, fallback_failed, fallback_details = find_studies_from_pacs_list(
        pacs_list, fallback_queries, application_aet
    )

    for study_uid, (pacs, fallback_idx) in fallback_study_map.items():
        original_index = fallback_index_map[fallback_idx]
        if study_uid not in study_pacs_map:
            study_pacs_map[study_uid] = (pacs, original_index)

    final_failed = []
    final_details = {}

    recovered_indices = set()
    for study_uid, (pacs, fallback_idx) in fallback_study_map.items():
        recovered_indices.add(fallback_index_map[fallback_idx])

    fallback_failed_original = {fallback_index_map[fi] for fi in fallback_failed}

    for original_index in failed_indices:
        if original_index in recovered_indices:
            continue
        final_failed.append(original_index)
        if original_index in fallback_failed_original:
            final_details[original_index] = "Accession query failed, fallback MRN+date query also failed"
        elif original_index not in failure_details or "no fallback data" in failure_details.get(original_index, ""):
            final_details[original_index] = failure_details.get(original_index, "Failed to find images")
        else:
            final_details[original_index] = "Accession query failed, fallback MRN+date query also failed"

    return study_pacs_map, final_failed, final_details


def get_studies_from_study_pacs_map(study_pacs_map, application_aet, output_dir):
    """Retrieve multiple studies from PACS using C-GET based on a study-to-PACS mapping."""
    successful_gets = 0
    failed_query_indices = []
    failure_details = {}
    total_studies = len(study_pacs_map)
    processed = 0

    for study_uid, (pacs, query_index) in study_pacs_map.items():
        logging.info(f"Retrieved {processed} / {total_studies} studies")
        logging.debug(f"Processing study from Excel row {query_index + 1}")

        try:
            result = get_study(
                host=pacs.host,
                port=pacs.port,
                calling_aet=application_aet,
                called_aet=pacs.aet,
                output_dir=output_dir,
                study_uid=study_uid
            )

            processed += 1

            if result["success"]:
                # Check if any files were actually retrieved
                if result["num_completed"] == 0:
                    num_failed = result.get("num_failed", 0)
                    num_warning = result.get("num_warning", 0)
                    logging.warning(
                        f"C-GET for query {query_index + 1} (study {study_uid}) reported success "
                        f"but retrieved 0 files from {pacs.host}:{pacs.port}. "
                        f"Failed: {num_failed}, Warning: {num_warning}. Moving on."
                    )
                    if query_index not in failed_query_indices:
                        failed_query_indices.append(query_index)
                        if num_failed > 0 or num_warning > 0:
                            failure_details[query_index] = (
                                f"C-GET succeeded but retrieved 0 files (failed: {num_failed}, warning: {num_warning})"
                            )
                        else:
                            failure_details[query_index] = "C-GET succeeded but retrieved 0 files (no sub-operations)"
                else:
                    successful_gets += 1
                    logging.debug(
                        f"Successfully retrieved study {study_uid} from {pacs.host}:{pacs.port} "
                        f"({result['num_completed']} files)"
                    )
            else:
                logging.error(
                    f"Failed to retrieve study for query {query_index + 1}: {result['message']}. Moving on."
                )
                if query_index not in failed_query_indices:
                    failed_query_indices.append(query_index)
                    failure_details[query_index] = f"Failed to retrieve images: {result['message']}"

        except Exception as e:
            logging.exception(
                f"Exception while retrieving study for query {query_index + 1} "
                f"(study {study_uid}): {str(e)}. Moving on."
            )
            processed += 1
            if query_index not in failed_query_indices:
                failed_query_indices.append(query_index)
                failure_details[query_index] = f"Exception during retrieval: {str(e)}"

    logging.info(f"Retrieved {processed} / {total_studies} studies")

    return successful_gets, failed_query_indices, failure_details


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
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'rb') as f:
                    f.seek(128)
                    if f.read(4) == b'DICM':
                        count += 1
            except (OSError, IOError) as e:
                logging.warning("Failed to read file '%s' while counting DICOM files: %s", file_path, e)
                continue
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

