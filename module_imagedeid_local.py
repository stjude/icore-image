import logging
import os
import re
import time
import xml.etree.ElementTree as ET

import pandas as pd

from ctp import CTPPipeline
from utils import setup_run_directories, configure_run_logging, format_number_with_commas, count_dicom_files, csv_string_to_xlsx, combine_filters, validate_dicom_tags, detect_and_validate_dates, format_dicom_date


def _save_metadata_files(pipeline, appdata_dir):
    audit_log_csv = pipeline.get_audit_log_csv("AuditLog")
    if audit_log_csv:
        csv_string_to_xlsx(audit_log_csv, os.path.join(appdata_dir, "metadata.xlsx"))
    
    deid_audit_log_csv = pipeline.get_audit_log_csv("DeidAuditLog")
    if deid_audit_log_csv:
        csv_string_to_xlsx(deid_audit_log_csv, os.path.join(appdata_dir, "deid_metadata.xlsx"))
    
    linker_csv = pipeline.get_idmap_csv()
    if linker_csv:
        csv_string_to_xlsx(linker_csv, os.path.join(appdata_dir, "linker.xlsx"))


def _log_progress(total_files, pipeline):
    if pipeline.metrics:
        files_received = pipeline.metrics.files_received
        files_quarantined = pipeline.metrics.files_quarantined
        
        progress_msg = f"Processed {format_number_with_commas(files_received)} / {format_number_with_commas(total_files)} files"
        if files_quarantined > 0:
            progress_msg += f" ({format_number_with_commas(files_quarantined)} quarantined)"
        
        logging.info(progress_msg)


def _apply_default_filter_script(filter_script, apply_default_filter_script):
    if not apply_default_filter_script:
        return filter_script
    
    stanford_filter_path = os.path.join(os.path.dirname(__file__), "ctp", "scripts", "stanford-filter.script")
    if os.path.exists(stanford_filter_path):
        with open(stanford_filter_path, 'r') as f:
            stanford_filter_content = f.read()
        return combine_filters(filter_script, stanford_filter_content)
    else:
        logging.warning(f"Stanford filter script not found at {stanford_filter_path}")
        return filter_script


def _generate_lookup_table_content(mapping_file_path):
    df = pd.read_excel(mapping_file_path)
    
    tag_mappings = {}
    for col in df.columns:
        if col.startswith("New-"):
            original_tag = col[4:]
            if original_tag in df.columns:
                tag_mappings[original_tag] = col
    
    if not tag_mappings:
        raise ValueError("Mapping file must have at least one New-{TagName} column with corresponding TagName column")
    
    all_tags = list(tag_mappings.keys())
    validate_dicom_tags(all_tags)
    
    date_tags = {}
    for tag in all_tags:
        if detect_and_validate_dates(df, tag):
            date_tags[tag] = True
    
    lookup_lines = []
    for tag, new_tag_col in tag_mappings.items():
        is_date = tag in date_tags
        
        for _, row in df.iterrows():
            original_value = row[tag]
            new_value = row[new_tag_col]
            
            if pd.notna(original_value) and pd.notna(new_value):
                if is_date:
                    original_value = format_dicom_date(original_value)
                    new_value = format_dicom_date(new_value)
                else:
                    original_value = str(original_value).strip()
                    new_value = str(new_value).strip()
                
                lookup_lines.append(f"{tag}/{original_value} = {new_value}")
    
    return "\n".join(lookup_lines)


def _parse_anonymizer_script_actions(anonymizer_script):
    tag_actions = {}
    
    if not anonymizer_script:
        return tag_actions
    
    try:
        root = ET.fromstring(anonymizer_script)
        
        for elem in root.findall(".//e[@n]"):
            tag_name = elem.get("n")
            tag_text = elem.text
            
            if tag_name and tag_text:
                tag_actions[tag_name] = tag_text.strip()
        
        return tag_actions
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse anonymizer script XML: {e}")


def _get_tag_hex_from_keyword(tag_keyword):
    dictionary_path = os.path.join(os.path.dirname(__file__), "resources", "dictionary.xml")
    tree = ET.parse(dictionary_path)
    root = tree.getroot()
    
    for element in root.findall(".//element[@key]"):
        if element.get("key") == tag_keyword:
            tag_attr = element.get("tag")
            if tag_attr:
                tag_hex = tag_attr.replace("(", "").replace(")", "").replace(",", "")
                return tag_hex
    
    raise ValueError(f"Tag {tag_keyword} not found in DICOM dictionary")


def _extract_simple_action(action_string):
    simple_actions = ["@keep()", "@remove()", "@empty()"]
    
    for simple_action in simple_actions:
        if action_string == simple_action:
            return simple_action[1:-2]
    
    return "quarantine"


def _merge_mapping_with_script(mapping_file_path, anonymizer_script):
    df = pd.read_excel(mapping_file_path)
    
    tag_mappings = {}
    for col in df.columns:
        if col.startswith("New-"):
            original_tag = col[4:]
            if original_tag in df.columns:
                tag_mappings[original_tag] = col
    
    existing_actions = _parse_anonymizer_script_actions(anonymizer_script)
    
    try:
        root = ET.fromstring(anonymizer_script)
    except ET.ParseError:
        root = ET.Element("script")
    
    for tag_name in tag_mappings.keys():
        tag_hex = _get_tag_hex_from_keyword(tag_name)
        
        existing_elem = None
        for elem in root.findall(f".//e[@n='{tag_name}']"):
            existing_elem = elem
            break
        
        if existing_elem is not None:
            original_action = existing_actions.get(tag_name, "@keep()")
            simple_fallback = _extract_simple_action(original_action)
            new_action = f"@lookup(this,{tag_name},{simple_fallback})"
            existing_elem.text = new_action
        else:
            new_elem = ET.Element("e")
            new_elem.set("en", "T")
            new_elem.set("t", tag_hex)
            new_elem.set("n", tag_name)
            new_elem.text = f"@lookup(this,{tag_name},keep)"
            root.append(new_elem)
    
    return ET.tostring(root, encoding="unicode")


def _process_mapping_file(mapping_file_path, anonymizer_script, lookup_table):
    if lookup_table is not None:
        return lookup_table, anonymizer_script
    
    if not mapping_file_path:
        return None, anonymizer_script
    
    lookup_content = _generate_lookup_table_content(mapping_file_path)
    modified_script = _merge_mapping_with_script(mapping_file_path, anonymizer_script)
    
    return lookup_content, modified_script


def imagedeid_local(input_dir, output_dir, appdata_dir=None, filter_script=None,
                   anonymizer_script=None, deid_pixels=False, lookup_table=None,
                   debug=False, run_dirs=None, apply_default_filter_script=True,
                   mapping_file_path=None, sc_pdf_output_dir=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    
    logging.info("Starting image deidentification")
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Output directory: {output_dir}")
    if filter_script:
        logging.info(f"Filter script: {filter_script}")
    if anonymizer_script:
        logging.info(f"Anonymizer script: {anonymizer_script}")
    if lookup_table:
        logging.info(f"Lookup table: {lookup_table}")
    if mapping_file_path:
        logging.info(f"Mapping file: {mapping_file_path}")
    logging.info(f"Pixel deidentification: {'enabled' if deid_pixels else 'disabled'}")
    
    logging.info("Counting input files...")
    total_files = count_dicom_files(input_dir)
    logging.info(f"Found {format_number_with_commas(total_files)} files to process")
    
    if appdata_dir is None:
        appdata_dir = run_dirs["appdata_dir"]
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(appdata_dir, exist_ok=True)
    
    quarantine_dir = os.path.join(appdata_dir, "quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)
    
    if anonymizer_script is None and mapping_file_path:
        default_script_path = os.path.join(os.path.dirname(__file__), "ctp", "scripts", "DicomAnonymizer.script")
        if os.path.exists(default_script_path):
            with open(default_script_path, 'r') as f:
                anonymizer_script = f.read()
        else:
            raise ValueError(f"Default anonymizer script not found at {default_script_path}")
    
    processed_lookup_table, processed_anonymizer_script = _process_mapping_file(
        mapping_file_path, anonymizer_script, lookup_table
    )
    
    if processed_lookup_table is not None:
        lookup_table = processed_lookup_table
    if processed_anonymizer_script is not None:
        anonymizer_script = processed_anonymizer_script
    
    final_filter_script = _apply_default_filter_script(filter_script, apply_default_filter_script)
    
    pipeline_type = "imagedeid_local_pixel" if deid_pixels else "imagedeid_local"
    ctp_log_level = "DEBUG" if debug else None
    
    with CTPPipeline(
        pipeline_type=pipeline_type,
        output_dir=output_dir,
        input_dir=input_dir,
        filter_script=final_filter_script,
        anonymizer_script=anonymizer_script,
        lookup_table=lookup_table,
        log_path=run_dirs["ctp_log_path"],
        log_level=ctp_log_level,
        quarantine_dir=quarantine_dir,
        sc_pdf_output_dir=sc_pdf_output_dir,
    ) as pipeline:
        time.sleep(3)
        
        save_interval = 5
        last_save_time = 0
        
        while not pipeline.is_complete():
            current_time = time.time()
            if current_time - last_save_time >= save_interval:
                _save_metadata_files(pipeline, appdata_dir)
                _log_progress(total_files, pipeline)
                last_save_time = current_time
            
            time.sleep(1)
        
        _save_metadata_files(pipeline, appdata_dir)
        
        num_saved = pipeline.metrics.files_saved if pipeline.metrics else 0
        num_quarantined = pipeline.metrics.files_quarantined if pipeline.metrics else 0
        
        logging.info("Deidentification complete")
        logging.info(f"Total files processed: {format_number_with_commas(num_saved + num_quarantined)}")
        logging.info(f"Files saved: {format_number_with_commas(num_saved)}")
        logging.info(f"Files quarantined: {format_number_with_commas(num_quarantined)}")
        
        return {
            "num_images_saved": num_saved,
            "num_images_quarantined": num_quarantined
        }

