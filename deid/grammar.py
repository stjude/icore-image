import os
import sys
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path


# Hash method mapping using tag names as keys
HASH_METHODS = {
    # Simple hash with length
    "AccessionNumber": "@hash(this,16)",
    "PatientID": "@hash(this,10)",
    
    # Name hash
    "PatientName": "@hashname(this,6,2)",
    
    # Clinical trial ID
    "ClinicalTrialSubjectID": "@hashptid(@SITEID,PatientID)",
    
    # UIDs - all use the same method
    "InstanceCreatorUID": "@hashuid(@UIDROOT,this)",
    "SOPInstanceUID": "@hashuid(@UIDROOT,this)",
    "FailedSOPInstanceUIDList": "@hashuid(@UIDROOT,this)",
    "RefSOPInstanceUID": "@hashuid(@UIDROOT,this)",
    "TransactionUID": "@hashuid(@UIDROOT,this)",
    "IrradiationEventUID": "@hashuid(@UIDROOT,this)",
    "CreatorVersionUID": "@hashuid(@UIDROOT,this)",
    "StudyInstanceUID": "@hashuid(@UIDROOT,this)",
    "SeriesInstanceUID": "@hashuid(@UIDROOT,this)",
    "FrameOfReferenceUID": "@hashuid(@UIDROOT,this)",
    "SynchronizationFrameOfReferenceUID": "@hashuid(@UIDROOT,this)",
    "ConcatenationUID": "@hashuid(@UIDROOT,this)",
    "DimensionOrganizationUID": "@hashuid(@UIDROOT,this)",
    "PaletteColorLUTUID": "@hashuid(@UIDROOT,this)",
    "LargePaletteColorLUTUID": "@hashuid(@UIDROOT,this)",
    "RefGenPurposeSchedProcStepTransUID": "@hashuid(@UIDROOT,this)",
    "UID": "@hashuid(@UIDROOT,this)",
    "TemplateExtensionOrganizationUID": "@hashuid(@UIDROOT,this)",
    "TemplateExtensionCreatorUID": "@hashuid(@UIDROOT,this)",
    "FiducialUID": "@hashuid(@UIDROOT,this)",
    "StorageMediaFilesetUID": "@hashuid(@UIDROOT,this)",
    "ReferencedFrameOfReferenceUID": "@hashuid(@UIDROOT,this)",
    "RelatedFrameOfReferenceUID": "@hashuid(@UIDROOT,this)",
    "DoseReferenceUID": "@hashuid(@UIDROOT,this)"
}

LOOKUP_ELEMENT_NAMES = {
    "AccessionNumber": 'accnum',
    "PatientID": 'ptid',
    "PatientName": 'ptname',
    "ClinicalTrialSubjectID": 'clinid',
    "SOPInstanceUID": 'sopuid',
    "StudyInstanceUID": 'studyuid',
    "SeriesInstanceUID": 'seriesuid',
    "FrameOfReferenceUID": 'frameuid',
}

def build_tag_dict():
    """
    Parse the DICOM dictionary XML files and build mappings of tag names to their hex codes.
    Returns a dictionary where keys are tag names and values are tag hex codes.
    """
    if getattr(sys, 'frozen', False):
        # Running in a PyInstaller bundle
        bundle_dir = sys._MEIPASS
        dict_path = Path(bundle_dir) / 'resources' / 'dictionary.xml'
        mapping_path = Path(bundle_dir) / 'resources' / 'pydicom_ctp_tag_dictionary.xml'
    else:
        # Running in normal Python
        dict_path = Path(__file__).parent.parent / 'resources' / 'dictionary.xml'
        mapping_path = Path(__file__).parent.parent / 'resources' / 'pydicom_ctp_tag_dictionary.xml'
    
    tree = ET.parse(dict_path)
    root = tree.getroot()
    
    tag_dict = {}
    
    for element in root.findall('.//element'):
        tag = element.get('tag')
        key = element.get('key')
        tag_dict[key] = tag
    
    if mapping_path.exists():
        tree = ET.parse(mapping_path)
        root = tree.getroot()
        
        for element in root.findall('.//element'):
            tag = element.get('tag')
            pydicom_key = element.get('pydicom_key')
            ctp_key = element.get('ctp_key')
            tag_dict[pydicom_key] = tag
            tag_dict[ctp_key] = tag
    
    return tag_dict

tag_dict = build_tag_dict()

def generate_filters_string(general_filters, modality_filters):
    for f in general_filters:
        if 'not' in f['action']:
            f['tag'] = f'!{f["tag"]}'
            f['action'] = f['action'].replace("not_", "")
    for _, filters in modality_filters.items():
        for f in filters:
            if 'not' in f['action']:
                f['tag'] = f'!{f["tag"]}'
            f['action'] = f['action'].replace("not_", "")
    general_str = "\n* ".join(
        f'{f["tag"]}.{f["action"]}("{f["value"]}")' for f in general_filters
    )

    modality_strs = []
    for _, filters in modality_filters.items():
        modality_expr = "\n* ".join(
            f'{f["tag"]}.{f["action"]}("{f["value"]}")' for f in filters
        )
        modality_strs.append(f"({modality_expr})")
    
    modalities_str = "\n+ ".join(modality_strs)

    if general_str and modalities_str:
        return f"{general_str} \n* ({modalities_str})"
    elif general_str:
        return general_str
    else:
        return modalities_str


def generate_anonymizer_script(tags_to_keep, tags_to_dateshift, tags_to_randomize, date_shift_days, site_id, lookup_lines=None, remove_unspecified=True, remove_overlays=True, remove_curves=True, remove_private=True):
    tags_to_keep = [tag.strip() for tag in tags_to_keep.split('\n') if tag.strip()]
    tags_to_dateshift = [tag.strip() for tag in tags_to_dateshift.split('\n') if tag.strip()]
    tags_to_randomize = [tag.strip() for tag in tags_to_randomize.split('\n') if tag.strip()]

    script = ['<script>']
    
    script.extend([
        f'   <p t="DATEINC">{date_shift_days}</p>',
        '   <p t="NOTICE1">IMPORTANT: Be sure to review Series Descriptions for PHI!!</p>',
        '   <p t="NOTICE2">IMPORTANT: Tags inside of sequences may contain PHI.</p>',
        '   <p t="PROFILENAME">CTP Clinical Trial Default</p>',
        '   <p t="PROJECTNAME">Project</p>',
        '   <p t="SITENAME">SiteName</p>',
        '   <p t="SITEID">1</p>',
        '   <p t="TRIALNAME">Trial</p>',
        '   <p t="SUBJECT">Subject</p>',
        '   <p t="UIDROOT">1.2.840.113654.2.70.1</p>',
        '   <e en="T" t="00200011" n="SeriesNumber">@always()@integer(SeriesInstanceUID,seriesnum,5)</e>',
        '   <e en="T" t="00120062" n="PatientIdentityRemoved">@always()YES</e>',
        '   <e en="T" t="00120063" n="DeIdentificationMethod">@always()CTP Default: based on DICOM PS3.15 AnnexE. Details in 0012,0064</e>',
        '   <e en="T" t="00120064" n="DeIdentificationMethodCodeSeq">113100/113105/113107/113108/113109</e>',
        '   <e en="T" t="00280303" n="LongitudinalTemporalInformationModified">@always()MODIFIED</e>'
    ])

    for tag_name in sorted(set(tags_to_keep + tags_to_dateshift + tags_to_randomize)):
        name = tag_name.strip()
        try:
            tag = tag_dict[tag_name].replace('(', '').replace(',', '').replace(')', '')
        except KeyError:
            return f"Tag {tag_name} not found in DICOM dictionary."

        if tag_name in tags_to_keep:
            script.append(f'   <e en="T" t="{tag}" n="{name}">@keep()</e>')
        elif tag_name in tags_to_dateshift:
            script.append(f'   <e en="T" t="{tag}" n="{name}">@incrementdate(this,@DATEINC)</e>')
        elif tag_name in tags_to_randomize:
            hash_method = HASH_METHODS.get(name, "@hash(this)")
            script.append(f'   <e en="T" t="{tag}" n="{name}">{hash_method}</e>')
    
    if lookup_lines:
        script.extend(lookup_lines)
    script.extend([
        f'   <e en="T" t="00120030" n="ClinicalTrialSiteID">@always(){site_id}</e>',
        f'   <r en="{"T" if remove_curves else "F"}" t="curves">Remove curves</r>',
        f'   <r en="{"T" if remove_overlays else "F"}" t="overlays">Remove overlays</r>',
        f'   <r en="{"T" if remove_private else "F"}" t="privategroups">Remove private groups</r>',
        f'   <r en="{"T" if remove_unspecified else "F"}" t="unspecifiedelements">Remove unchecked elements</r>',
        '</script>'
    ])
    
    return '\n'.join(script)

def generate_lookup_table(lookup_lines=None):
    if not lookup_lines:
        return None
    return "\n".join(lookup_lines)


def tag_keyword(tag: str) -> str:
    """Return a safe keyType for the lookup table (lower-case, no spaces)."""
    return tag.strip().replace(" ", "").lower()


class LookupConfig:
    def __init__(self, trigger_col, mappings):
        """
        Configuration for lookup table generation.
        
        Args:
            trigger_col (str): The column name that contains the trigger/key values
            mappings (list): List of dicts with:
                - 'col': Excel column name for new values
                - 'tag': DICOM tag name for the field
                - 'keytype': Key type for lookup (from LOOKUP_ELEMENT_NAMES)
                - 'trigger_tag': Optional - different tag to trigger lookup from
        """
        self.trigger_col = trigger_col
        self.mappings = mappings

def generate_lookup_contents(lookup_file):
    if not lookup_file:
        return None, None
    df = pd.read_excel(lookup_file)
    if "AccessionNumber" in df.columns:
        return generate_accession_number_lookup_contents(df)
    elif "MRN" in df.columns:
        return generate_mrn_lookup_contents(df)
    else:
        raise ValueError("Lookup file must contain either AccessionNumber or MRN column")

def generate_lookup_contents_base(df, config):
    """
    Base function for generating lookup table contents based on configuration.
    
    Args:
        lookup_file: Excel file with required columns
        config (LookupConfig): Configuration specifying mappings and requirements
    
    Returns:
        tuple: (lookup_lines, script_lines) for the CTP anonymizer
    """


    lookup_lines = []
    script_lines = []
    
    for mapping in config.mappings:
        keytype = mapping.get('keytype') or LOOKUP_ELEMENT_NAMES[mapping['tag']]
        
        for _, row in df.iterrows():
            if isinstance(config.trigger_col, list):
                trigger_val = '|'.join([str(row[t]).strip() for t in config.trigger_col])
            else:
                trigger_val = str(row[config.trigger_col]).strip()
            new_val = str(row[mapping['col']]).strip()
            lookup_lines.append(f"{keytype}/{trigger_val} = {new_val}")
        
        trigger_tag = mapping.get('trigger_tag', config.trigger_col)
        tag_hex = tag_dict[mapping['tag']].replace('(', '').replace(',', '').replace(')', '')
        if isinstance(trigger_tag, list):
            trigger_hex = [tag_dict[t].replace('(', '').replace(',', '').replace(')', '') for t in trigger_tag]
            trigger_hex = '|'.join(trigger_hex)
            script_lines.append(
                f'   <e en="T" t="{tag_hex}" n="{mapping["tag"]}">@lookup({trigger_hex}, {keytype})</e>'
            )
        elif trigger_tag == mapping['tag']:
            script_lines.append(
                f'   <e en="T" t="{tag_hex}" n="{mapping["tag"]}">@lookup(this, {keytype})</e>'
            )
        else:
            trigger_hex = tag_dict[trigger_tag].replace('(', '').replace(',', '').replace(')', '')
            script_lines.append(
                f'   <e en="T" t="{tag_hex}" n="{mapping["tag"]}">@lookup({trigger_hex}, {keytype})</e>'
            )

    return lookup_lines, script_lines

def generate_mrn_lookup_contents(df):
    """Generate lookup table contents based on MRN + StudyDate as the key."""
    config = LookupConfig(
        trigger_col=["MRN", "StudyDate"],
        mappings=[
            {
                'col': 'New-PatientName',
                'tag': 'PatientName',
                'keytype': 'patientname',
                'trigger_tag': ['PatientID', 'StudyDate']
            },
            {
                'col': 'New-PatientID',
                'tag': 'PatientID',
                'keytype': 'patientid',
                'trigger_tag': ['PatientID', 'StudyDate']
            },
            {
                'col': 'New-AccessionNumber',
                'tag': 'AccessionNumber',
                'keytype': 'accessionnumber',
                'trigger_tag': ['PatientID', 'StudyDate']
            },
            {
                'col': 'New-StudyDate',
                'tag': 'StudyDate',
                'keytype': 'studydate',
                'trigger_tag': ['PatientID', 'StudyDate']
            }
        ]
    )

    return generate_lookup_contents_base(df, config)

def generate_accession_number_lookup_contents(lookup_file):
    """Generate lookup table contents based on accession number as the key."""
    config = LookupConfig(
        trigger_col="AccessionNumber",
        mappings=[
            {
                'col': 'New-PatientName',
                'tag': 'PatientName',
                'keytype': 'patientname',
                'trigger_tag': ['AccessionNumber']
            },
            {
                'col': 'New-PatientID',
                'tag': 'PatientID',
                'keytype': 'patientid',
                'trigger_tag': ['AccessionNumber']
            },
            {
                'col': 'New-AccessionNumber',
                'tag': 'AccessionNumber',
                'keytype': 'accessionnumber',
                'trigger_tag': ['AccessionNumber']
            },
            {
                'col': 'New-StudyDate',
                'tag': 'StudyDate',
                'keytype': 'studydate',
                'trigger_tag': ['AccessionNumber']
            }
        ]
    )
    df = pd.read_excel(lookup_file)
    return generate_lookup_contents_base(df, config)

def generate_lookup_contents_legacy(lookup_file):
    """Legacy function that handles generic lookup table generation."""
    if not lookup_file:
        return None, None
    df = pd.read_excel(lookup_file)
    cols = {c.strip(): c for c in df.columns}
    required = ["InputTag", "OriginalValue", "OutputTag", "NewValue"]
    if any(col not in cols.values() for col in required):
        raise ValueError(f"Excel must contain columns: {', '.join(required)}")

    lookup_lines = []
    script_lines = []

    for output_tag, chunk in df.groupby("OutputTag", sort=False):
        trigger_tags = chunk["InputTag"].unique()
        if len(trigger_tags) != 1:
            raise ValueError(
                f"Output tag '{output_tag}' has more than one trigger tag "
                f"({', '.join(trigger_tags)}). CTP supports only one per tag."
            )
        trigger_tag = trigger_tags[0]
        keytype = tag_keyword(output_tag)

        for _, row in chunk.iterrows():
            trig_val = str(row["OriginalValue"]).strip()
            out_val = str(row["NewValue"]).strip()
            lookup_lines.append(f"{keytype}/{trig_val} = {out_val}")
        
        trigger_name = trigger_tag.strip()
        output_name = output_tag.strip()
        try:
            trigger_tag = tag_dict[trigger_name].replace('(', '').replace(',', '').replace(')', '')
        except KeyError:
            return f"Tag {trigger_name} not found in DICOM dictionary."
        try:
            output_tag = tag_dict[output_name].replace('(', '').replace(',', '').replace(')', '')
        except KeyError:
            return f"Tag {output_name} not found in DICOM dictionary."

        if trigger_tag == output_tag:
            script_lines.append(
                f"   <e en='T' t='{output_tag}' n='{output_name}'>@lookup(this, {keytype})</e>"
            )
        else:
            script_lines.append(
                f"   <e en='T' t='{output_tag}' n='{output_name}'>@lookup({trigger_tag}, {keytype})</e>"
            )

    return lookup_lines, script_lines


# =============================================================================
# HIPAA Safe Harbor De-identification Functions
# =============================================================================

def get_hipaa_safe_harbor_config():
    """
    Get HIPAA Safe Harbor configuration as a structured dictionary.

    This is the single source of truth for what tags are kept, removed, date-shifted,
    and randomized.

    Returns:
        dict: Configuration with keys:
            - tags_to_keep: List of (tag_hex, tag_name) tuples for tags that are preserved
            - tags_to_dateshift: List of (tag_hex, tag_name) tuples for date tags
            - tags_to_keep_time: List of (tag_hex, tag_name) tuples for time tags (kept as-is)
            - tags_to_randomize: List of (tag_hex, tag_name) tuples for UID tags
            - tags_to_remove: List of (tag_hex, tag_name) tuples for explicitly removed tags
    """
    config = {
        'tags_to_keep': [
            ('00080060', 'Modality'),
            ('00180015', 'BodyPartExamined'),
            ('00080008', 'ImageType'),
            ('00080070', 'Manufacturer'),
            ('00081090', 'ManufacturerModelName'),
        ],
        'tags_to_dateshift': [
            ('00080020', 'StudyDate'),
            ('00080021', 'SeriesDate'),
            ('00080022', 'AcquisitionDate'),
            ('00080023', 'ContentDate'),
            ('00100030', 'PatientBirthDate'),
            ('00181200', 'DateOfLastCalibration'),
            ('00209030', 'StudyCompletionDate'),
        ],
        'tags_to_keep_time': [
            ('00080030', 'StudyTime'),
            ('00080031', 'SeriesTime'),
            ('00080032', 'AcquisitionTime'),
            ('00080033', 'ContentTime'),
            ('00181201', 'TimeOfLastCalibration'),
            ('00209040', 'StudyCompletionTime'),
        ],
        'tags_to_randomize': [
            ('00080018', 'SOPInstanceUID'),
            ('0020000D', 'StudyInstanceUID'),
            ('0020000E', 'SeriesInstanceUID'),
            ('00200052', 'FrameOfReferenceUID'),
            ('00020003', 'MediaStorageSOPInstanceUID'),
            ('00209161', 'ConcatenationUID'),
            ('00209164', 'DimensionOrganizationUID'),
            ('00180024', 'DeviceUID'),
            ('00080014', 'InstanceCreatorUID'),
            ('00083010', 'IrradiationEventUID'),
            ('00281199', 'PaletteColorLookupTableUID'),
            ('30060024', 'ReferencedFrameOfReferenceUID'),
            ('004021A1', 'TemplateExtensionOrganizationUID'),
        ],
        'tags_to_remove': [
            ('00080080', 'InstitutionName'),
            ('00204000', 'ImageComments'),
            ('00324000', 'StudyComments'),
            ('00102180', 'Occupation'),
            ('00102000', 'MedicalAlerts'),
            ('001021F0', 'PatientReligiousPreference'),
            ('0008103E', 'SeriesDescription'),
            ('00181030', 'ProtocolName'),
        ]
    }

    return config


def generate_hipaa_safe_harbor_script(site_id, date_shift_days):
    """
    Generate a CTP anonymizer script that complies with HIPAA Safe Harbor (45 CFR §164.514(b)(2)).

    This function creates an anonymizer script that removes or anonymizes all 18 HIPAA identifiers:
    1. Names
    2. Geographic subdivisions smaller than state
    3. Dates (except year) - date-shifted by configured amount
    4. Telephone numbers
    5. Fax numbers
    6. Email addresses
    7. Social Security Numbers
    8. Medical Record Numbers - hashed
    9. Health Plan Beneficiary Numbers
    10. Account Numbers
    11. Certificate/License Numbers
    12. Vehicle Identifiers (N/A for DICOM)
    13. Device Identifiers and Serial Numbers - removed (but keep manufacturer/model per DICOM PS3.15)
    14. Web URLs
    15. IP Addresses
    16. Biometric Identifiers (N/A for typical DICOM)
    17. Full-face Photographs - handled via SOP Class filtering
    18. Any Other Unique Identifying Number - all UIDs rehashed, AccessionNumber hashed

    Args:
        site_id: Site identifier for ClinicalTrialSiteID tag
        date_shift_days: Number of days to shift dates (negative for backwards shift)

    Returns:
        str: XML anonymizer script for CTP
    """
    script = ['<script>']

    # Add parameters
    script.extend([
        f'   <p t="DATEINC">{date_shift_days}</p>',
        '   <p t="NOTICE1">HIPAA Safe Harbor Mode: All 18 HIPAA identifiers will be removed or anonymized.</p>',
        '   <p t="NOTICE2">WARNING: Review quarantined files for encapsulated content.</p>',
        '   <p t="PROFILENAME">HIPAA Safe Harbor De-identification</p>',
        '   <p t="PROJECTNAME">Project</p>',
        '   <p t="SITENAME">SiteName</p>',
        f'   <p t="SITEID">{site_id}</p>',
        '   <p t="TRIALNAME">Trial</p>',
        '   <p t="SUBJECT">Subject</p>',
        '   <p t="UIDROOT">1.2.840.113654.2.70.1</p>',
    ])

    # De-identification method tags
    script.extend([
        '   <e en="T" t="00120062" n="PatientIdentityRemoved">@always()YES</e>',
        '   <e en="T" t="00120063" n="DeIdentificationMethod">@always()HIPAA Safe Harbor per 45 CFR §164.514(b)(2)</e>',
        '   <e en="T" t="00120064" n="DeIdentificationMethodCodeSequence">113100/113101/113105/113107/113108/113109/113111</e>',
        '   <e en="T" t="00280303" n="LongitudinalTemporalInformationModified">@always()MODIFIED</e>',
        '   <e en="T" t="00200011" n="SeriesNumber">@always()@integer(SeriesInstanceUID,seriesnum,5)</e>',
    ])

    # HIPAA Identifier #1: Names
    script.append('   <e en="T" t="00100010" n="PatientName">@hashname(this,6,2)</e>')

    script.append('   <e en="T" t="00080090" n="ReferringPhysicianName">@empty()</e>')
    script.append('   <e en="T" t="00081050" n="PerformingPhysicianName">@empty()</e>')
    script.append('   <e en="T" t="00081070" n="OperatorsName">@empty()</e>')
    script.append('   <e en="T" t="00081048" n="PhysicianOfRecord">@empty()</e>')
    script.append('   <e en="T" t="00081060" n="NameOfPhysiciansReadingStudy">@empty()</e>')
    script.append('   <e en="T" t="00081072" n="OperatorIdentificationSequence">@remove()</e>')

    # HIPAA Identifier #2: Geographic subdivisions smaller than state
    script.append('   <e en="T" t="00101040" n="PatientAddress">@remove()</e>')
    script.append('   <e en="T" t="00080081" n="InstitutionAddress">@remove()</e>')
    script.append('   <e en="T" t="00380300" n="CurrentPatientLocation">@remove()</e>')

    # HIPAA Identifier #3: Dates (except year)
    config = get_hipaa_safe_harbor_config()

    # Handle date tags (date-shifted)
    for tag_hex, tag_name in config['tags_to_dateshift']:
        script.append(f'   <e en="T" t="{tag_hex}" n="{tag_name}">@incrementdate(this,@DATEINC)</e>')

    # Handle time tags (kept as-is)
    for tag_hex, tag_name in config['tags_to_keep_time']:
        script.append(f'   <e en="T" t="{tag_hex}" n="{tag_name}">@keep()</e>')

    # HIPAA Identifier #4,5,6: Telephone, Fax, Email
    script.append('   <e en="T" t="0040A353" n="TelephoneNumberTrial">@remove()</e>')

    # HIPAA Identifier #7: SSN - not standard DICOM, removed via private tag removal

    # HIPAA Identifier #8: Medical Record Numbers
    # PatientID - hash
    script.append('   <e en="T" t="00100020" n="PatientID">@hash(this,10)</e>')
    script.append('   <e en="T" t="00101000" n="OtherPatientIDs">@remove()</e>')

    # HIPAA Identifier #9: Health Plan Beneficiary Numbers
    script.append('   <e en="T" t="00101050" n="InsurancePlanIdentification">@remove()</e>')

    # HIPAA Identifier #10: Account Numbers - typically in private tags

    # HIPAA Identifier #11: Certificate/License Numbers - typically in private tags

    # HIPAA Identifier #13: Device Identifiers and Serial Numbers
    # Device serial number - remove
    # But KEEP Manufacturer and ManufacturerModelName (device type is allowed per DICOM PS3.15)
    script.append('   <e en="T" t="00181000" n="DeviceSerialNumber">@remove()</e>')
    script.append('   <e en="T" t="00080070" n="Manufacturer">@keep()</e>')
    script.append('   <e en="T" t="00081090" n="ManufacturerModelName">@keep()</e>')

    # HIPAA Identifier #18: Unique Identifying Numbers
    # AccessionNumber - hash
    script.append('   <e en="T" t="00080050" n="AccessionNumber">@hash(this,16)</e>')

    # All UIDs - rehash (using config from single source of truth)
    for tag_hex, tag_name in config['tags_to_randomize']:
        script.append(f'   <e en="T" t="{tag_hex}" n="{tag_name}">@hashuid(@UIDROOT,this)</e>')

    # Additional tags to keep (safe for HIPAA) - from config
    for tag_hex, tag_name in config['tags_to_keep']:
        script.append(f'   <e en="T" t="{tag_hex}" n="{tag_name}">@keep()</e>')

    # Tags to explicitly remove - from config
    for tag_hex, tag_name in config['tags_to_remove']:
        script.append(f'   <e en="T" t="{tag_hex}" n="{tag_name}">@remove()</e>')

    # Set ClinicalTrialSiteID
    script.append(f'   <e en="T" t="00120030" n="ClinicalTrialSiteID">@always(){site_id}</e>')

    # Global removal options - ALWAYS enabled for HIPAA Safe Harbor
    script.extend([
        '   <r en="T" t="curves">Remove curves</r>',
        '   <r en="T" t="overlays">Remove overlays</r>',
        '   <r en="T" t="privategroups">Remove private groups</r>',
        '   <r en="T" t="unspecifiedelements">Remove unchecked elements</r>',
        '</script>'
    ])

    return '\n'.join(script)
