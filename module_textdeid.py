import os
import re
import string
import sys
import pandas as pd
import logging
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from utils import setup_run_directories, configure_run_logging

logging.getLogger("presidio-analyzer").setLevel(logging.ERROR)
logging.getLogger("presidio-anonymizer").setLevel(logging.ERROR)


def create_analyzer_engine():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        model_path = os.path.join(bundle_dir, '_internal', 'en_core_web_sm', 'en_core_web_sm-3.7.1')
        import spacy
        from presidio_analyzer.nlp_engine import SpacyNlpEngine
        nlp_engine = SpacyNlpEngine(models=[{"lang_code": "en", "model_name": model_path}])
    else:
        configuration = {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
        }
        provider = NlpEngineProvider(nlp_configuration=configuration)
        nlp_engine = provider.create_engine()
    
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
    
    mrn_recognizer = PatternRecognizer(
        supported_entity="MRN",
        name="mrn_recognizer",
        patterns=[
            Pattern(name="mrn_pattern", regex=r"\b(?!0{7,10}\b)\d{7,10}\b", score=0.5),
            Pattern(name="mrn_prefix_pattern", regex=r"\b[A-Z]{2,6}-\d{4,10}\b", score=0.7),
        ],
    )
    
    alphanumeric_id_recognizer = PatternRecognizer(
        supported_entity="ALPHANUMERIC_ID",
        name="alphanumeric_id_recognizer",
        patterns=[
            Pattern(name="date_id_pattern", regex=r"\b\d{4}-\d{2}-\d{2}\b", score=0.95),
        ],
    )
    
    title_name_recognizer = PatternRecognizer(
        supported_entity="PERSON",
        name="title_name_recognizer",
        patterns=[
            Pattern(
                name="dr_name_pattern",
                regex=r"(?<=Dr\.\s)([A-Z][A-Z]+)\b",
                score=0.85
            ),
            Pattern(
                name="dr_no_period_pattern",
                regex=r"(?<=Dr\s)([A-Z][A-Z]+)\b",
                score=0.85
            ),
        ],
    )
    
    last_name_recognizer = PatternRecognizer(
        supported_entity="PERSON",
        name="last_name_recognizer",
        patterns=[
            Pattern(
                name="patient_full_name_pattern",
                regex=r"(?<=Patient:\s)([A-Z][a-z]+\s+[A-Z]{2,})\b",
                score=0.85
            ),
        ],
    )
    
    ssn_recognizer = PatternRecognizer(
        supported_entity="US_SSN",
        name="ssn_recognizer",
        patterns=[
            Pattern(
                name="ssn_pattern",
                regex=r"\b\d{3}-\d{2}-\d{4}\b",
                score=0.95
            ),
        ],
    )
    
    phone_recognizer = PatternRecognizer(
        supported_entity="PHONE_NUMBER",
        name="phone_recognizer",
        patterns=[
            Pattern(
                name="phone_with_parens_pattern",
                regex=r"\(\d{3}\)\s*\d{3}-\d{4}",
                score=0.85
            ),
            Pattern(
                name="phone_dashes_pattern",
                regex=r"\b\d{3}-\d{3}-\d{4}\b",
                score=0.85
            ),
        ],
    )
    
    analyzer.registry.add_recognizer(mrn_recognizer)
    analyzer.registry.add_recognizer(alphanumeric_id_recognizer)
    analyzer.registry.add_recognizer(title_name_recognizer)
    analyzer.registry.add_recognizer(last_name_recognizer)
    analyzer.registry.add_recognizer(ssn_recognizer)
    analyzer.registry.add_recognizer(phone_recognizer)
    
    return analyzer


def scrub(data, whitelist, blacklist):
    analyzer = create_analyzer_engine()
    anonymizer = AnonymizerEngine()
    
    medical_terms_deny_list = {
        'cardiomediastinal', 'ventricles', 'medullaris', 'conus', 'calvarium',
        'paraspinal', 'mediastinum', 'pleura', 'parenchyma', 'foramina',
        'mucosal', 'multiplanar', 'heterogeneously', 'schmorl',
        'md', 'pneumonia', 'pneumothorax', 'effusion', 'opacity', 
        'consolidation', 'calcification', 'abnormality', 'silhouette',
        'technique', 'ap', 'lateral', 'ct', 'mri', 'radiograph', 'examination',
        'copd', 'emg', 'npi', 'acr', 'lmp', 'afi',
        'hu', 'ed', 'npo', 'iv', 'or', 'er', 'icu', 'po', 'im', 'sc',
        'degrees', 'cm', 'mm', 'ml'
    }
    
    medical_person_deny_list = {
        'ventricles', 'mucosal', 'multiplanar', 'schmorl', 'medullaris', 'conus',
        'standard', 'g2p1', 'referring',
        'son', 'daughter', 'wife', 'husband', 'mother', 'father', 'parent',
        'pine', 'cedar', 'oak', 'maple',
        'diverticulosis', 'diverticulitis'
    }
    
    if whitelist:
        medical_terms_deny_list.update(whitelist)
        medical_person_deny_list.update(whitelist)
    
    for item in blacklist:
        blacklist_recognizer = PatternRecognizer(
            supported_entity="CUSTOM_BLACKLIST",
            name=f"blacklist_{hash(item)}",
            patterns=[Pattern(name=f"blacklist_pattern_{hash(item)}", regex=re.escape(item), score=0.95)],
        )
        analyzer.registry.add_recognizer(blacklist_recognizer)
    
    entities_to_detect = [
        "PERSON", "DATE_TIME", "MRN", "ALPHANUMERIC_ID", "PHONE_NUMBER",
        "EMAIL_ADDRESS", "LOCATION", "US_SSN", "MEDICAL_LICENSE",
        "US_DRIVER_LICENSE", "US_PASSPORT", "CREDIT_CARD", "US_ITIN",
        "NRP", "IBAN_CODE", "CUSTOM_BLACKLIST"
    ]
    
    alphanumeric_date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
    age_pattern = re.compile(r'\b\d{1,3}[-\s]year[-\s]old\b', re.IGNORECASE)
    duration_pattern = re.compile(r'\b\d{1,3}\s+(weeks?|months?|days?|hours?|minutes?|seconds?|mins?|secs?)\b', re.IGNORECASE)
    time_pattern = re.compile(r'\b\d{1,2}:\d{2}(\s*[AP]M)?\b', re.IGNORECASE)
    gestational_age_pattern = re.compile(r'\b\d{1,2}\s*weeks?\s*\d*\s*days?\b', re.IGNORECASE)
    time_reference_pattern = re.compile(r'\b(midnight|noon|morning|evening|afternoon)\b', re.IGNORECASE)
    complex_age_pattern = re.compile(r'\b\d{1,3}\s+years?,\s*\d{1,2}\s+(months?|days?)\s+old\b', re.IGNORECASE)
    
    operators = {
        "PERSON": OperatorConfig("replace", {"new_value": "[PERSONALNAME]"}),
        "DATE_TIME": OperatorConfig("replace", {"new_value": "[DATE]"}),
        "MRN": OperatorConfig("replace", {"new_value": "[MRN]"}),
        "ALPHANUMERIC_ID": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[PHONE]"}),
        "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[EMAIL]"}),
        "LOCATION": OperatorConfig("replace", {"new_value": "[LOCATION]"}),
        "US_SSN": OperatorConfig("replace", {"new_value": "[SSN]"}),
        "MEDICAL_LICENSE": OperatorConfig("replace", {"new_value": "[MEDICALID]"}),
        "US_DRIVER_LICENSE": OperatorConfig("replace", {"new_value": "[DRIVERSLICENSE]"}),
        "US_PASSPORT": OperatorConfig("replace", {"new_value": "[PASSPORT]"}),
        "CREDIT_CARD": OperatorConfig("replace", {"new_value": "[CREDITCARD]"}),
        "US_ITIN": OperatorConfig("replace", {"new_value": "[ITIN]"}),
        "NRP": OperatorConfig("replace", {"new_value": "[NRP]"}),
        "IBAN_CODE": OperatorConfig("replace", {"new_value": "[IBAN]"}),
        "CUSTOM_BLACKLIST": OperatorConfig("replace", {"new_value": "[REDACTED]"}),
    }
    
    results = []
    total_rows = len(data)
    for i, text_item in enumerate(data):
        logging.info(f"Processing row {i+1}/{total_rows}")
        text = str(text_item) if text_item is not None else "Empty"
        text = ''.join(c for c in text if c in string.printable)
        
        results_analysis = analyzer.analyze(
            text=text,
            entities=entities_to_detect,
            language='en',
            score_threshold=0.5
        )
        
        filtered_results = []
        for result in results_analysis:
            detected_text = text[result.start:result.end]
            detected_lower = detected_text.lower()
            
            if result.entity_type == "LOCATION":
                if detected_lower in medical_terms_deny_list:
                    continue
            
            if result.entity_type == "PERSON":
                if detected_lower in medical_person_deny_list:
                    continue
            
            if result.entity_type == "DATE_TIME":
                if alphanumeric_date_pattern.match(detected_text):
                    continue
                if age_pattern.search(detected_text):
                    continue
                if complex_age_pattern.search(detected_text):
                    continue
                if duration_pattern.search(detected_text):
                    continue
                if time_pattern.match(detected_text):
                    continue
                if gestational_age_pattern.match(detected_text):
                    continue
                if time_reference_pattern.search(detected_text):
                    continue
            
            filtered_results.append(result)
        
        filtered_results = sorted(filtered_results, key=lambda x: x.start)
        
        anonymized_result = anonymizer.anonymize(
            text=text,
            analyzer_results=filtered_results,
            operators=operators
        )
        
        results.append(anonymized_result.text)
    
    return results


def textdeid(input_file, output_dir, to_keep_list=None, to_remove_list=None, columns_to_drop=None, columns_to_deid=None, debug=False, run_dirs=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    log_level = logging.DEBUG if debug else logging.INFO
    configure_run_logging(run_dirs["run_log_path"], log_level)
    logging.info("Running textdeid")
    logging.info(f"Input file: {input_file}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Debug mode: {debug}")
    
    if to_keep_list is None:
        to_keep_list = []
    if to_remove_list is None:
        to_remove_list = []
    
    logging.info(f"Whitelist items to keep: {len(to_keep_list)}")
    logging.info(f"Blacklist items to remove: {len(to_remove_list)}")
    
    df = pd.read_excel(input_file, header=0)
    logging.info(f"Read Excel file with {len(df)} rows and columns: {list(df.columns)}")
    
    if columns_to_drop is not None and len(columns_to_drop) > 0:
        logging.info(f"Dropping columns: {columns_to_drop}")
        df = df.drop(columns=columns_to_drop, errors='ignore')
    
    if columns_to_deid is None:
        columns_to_process = list(df.columns)
        logging.info(f"No specific columns specified for de-identification, processing all columns: {columns_to_process}")
    else:
        columns_to_process = [col for col in columns_to_deid if col in df.columns]
        logging.info(f"De-identifying specific columns: {columns_to_process}")
    
    result_df = df.copy()
    
    for column in columns_to_process:
        logging.info(f"Processing column: {column}")
        column_data = df[column].tolist()
        deid_data = scrub(column_data, to_keep_list, to_remove_list)
        result_df[column] = deid_data
    
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output.xlsx")
    result_df.to_excel(output_file, index=False, header=True)
    
    logging.info("Text deidentification complete")
    return {"num_rows_processed": len(result_df)}

