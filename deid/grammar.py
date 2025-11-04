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
    "CodeSetExtensionCreatorUID": "@hashuid(@UIDROOT,this)",
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
    "LargePaletteColorLUTUid": "@hashuid(@UIDROOT,this)",
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


def generate_anonymizer_script(tags_to_keep, tags_to_dateshift, tags_to_randomize, date_shift_days, site_id, lookup_lines=None):
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
        '   <p t="UIDROOT">1.2.840.113654.2.70.1</p>'
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
        '   <r en="T" t="curves">Remove curves</r>',
        '   <r en="T" t="overlays">Remove overlays</r>',
        '   <r en="T" t="privategroups">Remove private groups</r>',
        '   <r en="T" t="unspecifiedelements">Remove unchecked elements</r>',
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
    