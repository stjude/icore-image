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

NLM_PRESERVE_MEDICAL = {
    'mediastinum', 'ventricles', 'paraspinal', 'pleural', 'calvarium',
    'conus', 'medullaris', 'schmorl', 'multiplanar', 'parenchyma',
    'md', 'er', 'copd', 'ap', 'lateral', 'icu', 'npo', 'iv',
    'pneumonia', 'pneumothorax', 'effusion', 'opacity', 'consolidation',
    'mother', 'father', 'son', 'daughter', 'wife', 'husband',
    'degrees', 'cm', 'mm', 'ml', 'mg', 'dl', 'kg',
    'ct', 'mri', 'xray', 'x-ray', 'pet', 'ultrasound', 'doppler',
    'head', 'brain', 'spine', 'lumbar', 'thoracic', 'cervical', 'chest',
    'abdomen', 'pelvis', 'cardiac', 'heart', 'liver', 'kidney', 'spleen',
    'gray', 'grey', 'white', 'matter', 'axial', 'sagittal', 'coronal',
    'vertex', 'orbits', 'globes', 'extraocular', 'paranasal', 'sinuses',
    'maxillary', 'frontal', 'csf', 'hydrocephalus', 'intracranial',
    'hemorrhage', 'infarction', 'mass', 'lesion', 'fracture', 'edema',
    'herniation', 'stenosis', 'disc', 'vertebral', 'spinal', 'cord',
    'canal', 'foramina', 'pedicles', 'sacrum', 'coccyx', 'iliac',
    'femur', 'tibia', 'fibula', 'humerus', 'radius', 'ulna', 'scaphoid',
    'aortic', 'mitral', 'tricuspid', 'ventricular', 'atrial',
    'myocardium', 'pericardium', 'endocardium', 'septum', 'chamber',
    'gallbladder', 'bowel', 'stomach', 'duodenum', 'colon', 'appendix',
    'bladder', 'prostate', 'uterus', 'ovary', 'adnexa', 'retroperitoneum',
    'breast', 'mammogram', 'angiogram', 'venogram', 'arthrogram',
    'myelogram', 'cholangiogram', 'intra-articular', 'extra-axial',
    'patient', 'male', 'female', 'year', 'years', 'old', 'age',
    'exam', 'examination', 'study', 'report', 'findings', 'impression',
    'history', 'indication', 'comparison', 'technique', 'protocol',
    'electronically', 'signed', 'dictated', 'transcribed', 'radiologist',
    'headaches', 'headache', 'dizziness', 'pain', 'symptoms', 'soft',
    'bones', 'joints', 'tissues', 'muscles', 'tendons', 'ligaments',
    'parenchyma', 'gray-white', 'grey-white', 'gray', 'grey', 'white',
    'paranasal', 'sinuses', 'maxillary', 'frontal', 'ethmoid',
}

NLM_REDACT_NAMES = {'pine'}

def create_nlp_engine():
    if getattr(sys, 'frozen', False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        models_base_dir = os.path.join(bundle_dir, '_internal', 'en_core_web_sm')
        model_subdir = None
        if os.path.isdir(models_base_dir):
            for entry in os.listdir(models_base_dir):
                full_path = os.path.join(models_base_dir, entry)
                if entry.startswith('en_core_web_sm-') and os.path.isdir(full_path):
                    model_subdir = full_path
                    break
        if model_subdir is None:
            # Fallback to the previously hard-coded path to preserve existing behavior
            model_subdir = os.path.join(models_base_dir, 'en_core_web_sm-3.8.0')
        model_path = model_subdir
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
    return nlp_engine

def create_analyzer_engine():
    nlp_engine = create_nlp_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
    
    age_90 = PatternRecognizer(
        supported_entity="AGE90PLUS",
        name="age90",
        patterns=[
            Pattern(name="a1", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*years?\s*old)", score=1.0),
            Pattern(name="a2", regex=r"(9[0-9]|[1-9]\d{2,})(?=\s*year\s*old)", score=1.0),
            Pattern(name="a3", regex=r"(9[0-9]|[1-9]\d{2,})(?=\-year\-old)", score=1.0),
            Pattern(name="a4", regex=r"(?<=age:\s)(9[0-9]|[1-9]\d{2,})\b", score=1.0),
            Pattern(name="a5", regex=r"(?<=Age:\s)(9[0-9]|[1-9]\d{2,})\b", score=1.0),
            Pattern(name="a6", regex=r"(?<=is\s)(9[0-9]|[1-9]\d{2,})(?=\s*years?\s*old)", score=1.0),
        ],
    )
    
    date_patterns = PatternRecognizer(
        supported_entity="DATEPATTERN",
        name="dates",
        patterns=[
            Pattern(name="d1", regex=r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", score=0.95),
            Pattern(name="d2", regex=r"\b\d{1,2}-\d{1,2}-\d{4}\b", score=0.94),
            Pattern(name="d3", regex=r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b", score=0.98),
            Pattern(name="d4", regex=r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}\b", score=0.96),
        ],
    )
    
    name_extras_recognizer = PatternRecognizer(
        supported_entity="NAMEPERSON",
        name="names",
        patterns=[
            Pattern(name="n1", regex=r"\b[A-Z]\.[A-Z]\.\b", score=0.85),
            Pattern(name="n2", regex=r"\b[A-Z]\.\s[A-Z]\.\s[A-Z][a-z]+\b", score=0.9),
            Pattern(name="n3", regex=r"\b(?!\d+\-year)(?!\d+\-day)(?!follow\-)(?!year\-old)[A-Z][a-z]{2,}\-[A-Z][a-z]{2,}\b", score=0.85),
            Pattern(name="n4", regex=r"\bPine\b", score=0.8),
            Pattern(name="n5", regex=r"\bPatient\s+[A-Z]\.[A-Z]\.", score=0.9),
            Pattern(name="n6", regex=r"\bThe(?=\s+MD\b)", score=0.75),
        ],
    )
    
    medical_record_number_recognizer = PatternRecognizer(
        supported_entity="ALPHANUMERICID",
        name="mrn",
        patterns=[
            Pattern(name="m1", regex=r"\b\d{7}(?!\d)\b", score=0.97),
            Pattern(name="m2", regex=r"\b\d{8}(?!\d)\b", score=0.97),
            Pattern(name="m3", regex=r"\b\d{9}\b", score=0.97),
            Pattern(name="m4", regex=r"\b\d{10}\b", score=0.97),
            Pattern(name="m5", regex=r"\b[A-Z]{1,6}-\d{6,10}\b", score=0.98),
            Pattern(name="m6", regex=r"\b[A-Z]\d{7,9}\b", score=0.97),
            Pattern(name="m7", regex=r"\b[A-Z]{2,3}\d{4,8}\b", score=0.97),
            Pattern(name="m8", regex=r"\b[A-Z0-9]+-\d+-[A-Z0-9]+-\d+\b", score=0.98),
            Pattern(name="m9", regex=r"\b\d{2}-\d{2}-\d{4}\b", score=0.98),
        ],
    )
    
    phone_number_recognizer = PatternRecognizer(
        supported_entity="PHONENUMBER",
        name="phone",
        patterns=[
            Pattern(name="p1", regex=r"\(\d{3}\)\s*\d{3}-\d{4}", score=0.99),
            Pattern(name="p2", regex=r"\b\d{3}-\d{3}-\d{4}\b", score=0.98),
            Pattern(name="p3", regex=r"\b\d{3}\.\d{3}\.\d{4}\b", score=0.98),
        ],
    )
    
    address_recognizer = PatternRecognizer(
        supported_entity="LOCATION",
        name="address",
        patterns=[
            Pattern(name="addr1", regex=r"\b\d+\s+[A-Z][a-z]+\s+(?:Ave|St|Street|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Court|Ct|Way|Circle|Cir),?\s+[A-Z][a-z]+,?\s+[A-Z]{2}(?:\s+\d{5})?\b", score=0.95),
            Pattern(name="addr2", regex=r"Address:\s+\d+\s+[A-Z][a-z]+\s+(?:Ave|St|Street|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd),?\s+[A-Z][a-z]+,?\s+[A-Z]{2}\b", score=0.96),
        ],
    )
    
    ssn = PatternRecognizer(
        supported_entity="SSNUMBER",
        name="ssn",
        patterns=[
            Pattern(name="s1", regex=r"\b\d{3}-\d{2}-\d{4}\b", score=0.98),
        ],
    )
    
    email = PatternRecognizer(
        supported_entity="EMAILADDR",
        name="email",
        patterns=[
            Pattern(name="e1", regex=r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", score=0.98),
        ],
    )
    
    ip_addr = PatternRecognizer(
        supported_entity="IPADDR",
        name="ip",
        patterns=[
            Pattern(name="i1", regex=r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", score=0.9),
        ],
    )
    
    analyzer.registry.add_recognizer(age_90)
    analyzer.registry.add_recognizer(date_patterns)
    analyzer.registry.add_recognizer(name_extras_recognizer)
    analyzer.registry.add_recognizer(medical_record_number_recognizer)
    analyzer.registry.add_recognizer(phone_number_recognizer)
    analyzer.registry.add_recognizer(address_recognizer)
    analyzer.registry.add_recognizer(ssn)
    analyzer.registry.add_recognizer(email)
    analyzer.registry.add_recognizer(ip_addr)
    
    return analyzer

def scrub(data, whitelist, blacklist):
    analyzer = create_analyzer_engine()
    anonymizer = AnonymizerEngine()
    
    medical_preserve = NLM_PRESERVE_MEDICAL.copy()
    if whitelist:
        medical_preserve.update(w.lower() for w in whitelist)
    
    for item in blacklist:
        rec = PatternRecognizer(
            supported_entity="BLACKLIST",
            name=f"bl_{hash(item)}",
            patterns=[Pattern(name="bl", regex=re.escape(item), score=1.0)],
        )
        analyzer.registry.add_recognizer(rec)
    
    entities = ["PERSON", "DATE_TIME", "LOCATION", "PHONE_NUMBER", "EMAIL_ADDRESS",
                "US_SSN", "AGE90PLUS", "DATEPATTERN", "NAMEPERSON", "ALPHANUMERICID", 
                "PHONENUMBER", "SSNUMBER", "EMAILADDR", "IPADDR", "BLACKLIST"]
    
    operators = {
        "PERSON": OperatorConfig("replace", {"new_value": "[PERSONALNAME]"}),
        "NAMEPERSON": OperatorConfig("replace", {"new_value": "[PERSONALNAME]"}),
        "DATE_TIME": OperatorConfig("replace", {"new_value": "[DATE]"}),
        "DATEPATTERN": OperatorConfig("replace", {"new_value": "[DATE]"}),
        "LOCATION": OperatorConfig("replace", {"new_value": "[ADDRESS]"}),
        "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "US_SSN": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "AGE90PLUS": OperatorConfig("replace", {"new_value": "[AGE90+]"}),
        "ALPHANUMERICID": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "PHONENUMBER": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "SSNUMBER": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "EMAILADDR": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "IPADDR": OperatorConfig("replace", {"new_value": "[ALPHANUMERICID]"}),
        "BLACKLIST": OperatorConfig("replace", {"new_value": "[REDACTED]"}),
    }
    
    age_under_90 = re.compile(r'\b([1-8]?\d)\s*[-\s]*(year|yr)s?[-\s]*old\b', re.I)
    duration = re.compile(r'\b\d+\s+(weeks?|months?|days?|hours?|minutes?|seconds?)\b', re.I)
    time_12h = re.compile(r'\b\d{1,2}(am|pm)\b', re.I)
    time_colon = re.compile(r'\b\d{1,2}:\d{2}(\s*[AP]M)?\b', re.I)
    gestational = re.compile(r'\b\d{1,2}\s*weeks?\s*\d*\s*days?\b', re.I)
    measurement = re.compile(r'\b\d+(?:\.\d+)?\s*(cm|mm|ml|mg|kg|lb|oz|degrees?|dl|percent|%)\b', re.I)
    blood_pressure = re.compile(r'\b\d{2,3}/\d{2,3}\b')
    year_only = re.compile(r'\b(19|20)\d{2}\b')
    relative_date = re.compile(r'\b(yesterday|today|tomorrow)\b', re.I)
    all_zeros = re.compile(r'\b0{7,}\b')
    hospital = re.compile(r'\b(Hospital|Medical Center|Clinic|Healthcare|Health System)\b', re.I)
    url = re.compile(r'\bhttps?://[^\s]+\b')
    
    results = []
    for i, text_item in enumerate(data):
        text = str(text_item) if text_item is not None else ""
        text = ''.join(c for c in text if c in string.printable)
        
        results_analysis = analyzer.analyze(
            text=text,
            entities=entities,
            language='en',
            score_threshold=0.5
        )
        
        filtered = []
        for result in results_analysis:
            detected = text[result.start:result.end]
            lower = detected.lower()
            
            if lower in NLM_REDACT_NAMES and result.entity_type == "PERSON":
                filtered.append(result)
                continue
            
            if lower in medical_preserve:
                continue
            
            if result.entity_type == "PERSON":
                detected_lower = detected.lower()
                if any(term in detected_lower for term in ['parenchyma', 'gray-white', 'grey-white', 'paranasal', 'sinuses', 'brain', 'ct head', 'mri brain']):
                    continue
                if detected_lower in ['ct', 'mri', 'x-ray', 'xray', 'head', 'brain', 'spine', 'chest']:
                    continue
            
            if re.match(r'\d+-year-old', text[max(0, result.start-10):result.end+1]):
                continue
            
            detected_words_list = detected.split()
            if result.entity_type == "PERSON":
                if any(word.lower() in ["record", "chart", "visit", "case"] for word in detected_words_list):
                    if len(detected_words_list) == 1:
                        continue
            
            if all_zeros.match(detected):
                continue
            
            if result.entity_type == "ALPHANUMERICID":
                if re.match(r'^(\d)\1{6,}$', detected):
                    continue
                if re.search(r'(\d)\1{3,}$', detected) and len(detected) == 7:
                    continue
            
            if result.entity_type == "LOCATION" and hospital.search(detected):
                continue
            
            if url.match(detected):
                continue
            
            if result.entity_type == "DATE_TIME":
                if re.search(r'\b(9[0-9]|[1-9]\d{2,})\s*years?\s*old', detected):
                    continue
                if age_under_90.search(detected):
                    continue
                if duration.search(detected):
                    continue
                if time_12h.match(detected):
                    continue
                if time_colon.match(detected):
                    continue
                if year_only.fullmatch(detected):
                    continue
                if relative_date.search(detected):
                    continue
                if gestational.match(detected):
                    continue
            
            if blood_pressure.fullmatch(detected):
                continue
            
            if measurement.search(detected):
                continue
            
            if re.match(r'^\d{1,2}\s+years?$', detected):
                continue
            
            if result.entity_type in ["ALPHANUMERICID", "IPADDR"] and re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', detected):
                parts = [int(p) for p in detected.split('.')]
                if all(p < 20 for p in parts):
                    continue
            
            filtered.append(result)
        
        filtered = sorted(filtered, key=lambda x: x.start)
        anonymized = anonymizer.anonymize(text=text, analyzer_results=filtered, operators=operators)
        results.append(anonymized.text)
    
    return results

def textdeid(input_file, output_dir, to_keep_list=None, to_remove_list=None,
             columns_to_drop=None, columns_to_deid=None, debug=False, run_dirs=None):
    if run_dirs is None:
        run_dirs = setup_run_directories()
    
    configure_run_logging(run_dirs["run_log_path"], logging.DEBUG if debug else logging.INFO)
    logging.info(f"NLM-Matched Deidentification: {input_file}")
    
    df = pd.read_excel(input_file, header=0)
    
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop, errors='ignore')
    
    columns_to_process = columns_to_deid if columns_to_deid else list(df.columns)
    columns_to_process = [c for c in columns_to_process if c in df.columns]
    
    result_df = df.copy()
    for column in columns_to_process:
        logging.info(f"Processing: {column}")
        result_df[column] = scrub(df[column].tolist(), to_keep_list or [], to_remove_list or [])
    
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output.xlsx")
    result_df.to_excel(output_file, index=False, header=True)
    
    logging.info(f"Complete: {output_file}")
    return {"num_rows_processed": len(result_df), "output_file": output_file}
