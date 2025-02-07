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
    Parse the DICOM dictionary XML file and build a mapping of tag names to their hex codes.
    Returns a dictionary where keys are tag names and values are tag hex codes.
    """
    dict_path = Path(__file__).parent / 'dictionary.xml'
    
    tree = ET.parse(dict_path)
    root = tree.getroot()
    
    tag_dict = {}
    
    for element in root.findall('.//element'):
        tag = element.get('tag')
        key = element.get('key')
        tag_dict[key] = tag
    
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
    # Build general filters string
    general_str = "\n* ".join(
        f'{f["tag"]}.{f["action"]}("{f["value"]}")' for f in general_filters
    )

    # Build modality filters string
    modality_strs = []
    for _, filters in modality_filters.items():
        modality_expr = "\n* ".join(
            f'{f["tag"]}.{f["action"]}("{f["value"]}")' for f in filters
        )
        modality_strs.append(f"({modality_expr})")
    
    modalities_str = "\n+ ".join(modality_strs)

    # Combine into full expression
    if general_str and modalities_str:
        return f"{general_str} \n* ({modalities_str})"
    elif general_str:
        return general_str
    else:
        return modalities_str


def generate_anonymizer_script(tags_to_keep, tags_to_dateshift, tags_to_randomize, date_shift_days, lookup_file=None):
    # Convert newline-separated strings to arrays, filtering out empty strings
    tags_to_keep = [tag.strip() for tag in tags_to_keep.split('\n') if tag.strip()]
    tags_to_dateshift = [tag.strip() for tag in tags_to_dateshift.split('\n') if tag.strip()]
    tags_to_randomize = [tag.strip() for tag in tags_to_randomize.split('\n') if tag.strip()]
    tags_to_lookup = []
    if lookup_file:
        df = pd.read_excel(lookup_file)
        for _, row in df.iterrows():
            tag_name = row[0]
            if tag_name not in tags_to_lookup:
                tags_to_lookup.append(tag_name)
    
    script = ['<script>']
    
    # Add header parameters
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
    
    # Process DICOM tags
    for tag_name in sorted(set(tags_to_keep + tags_to_dateshift + tags_to_randomize + tags_to_lookup)):
        name = tag_name.strip()
        tag = tag_dict[tag_name].replace('(', '').replace(',', '').replace(')', '')
        if tag_name in tags_to_lookup:
            lookup_element_name = LOOKUP_ELEMENT_NAMES.get(name, 'ptid')
            script.append(f'   <e en="T" t="{tag}" n="{name}">@lookup(this,{lookup_element_name})</e>')
        elif tag_name in tags_to_keep:
            script.append(f'   <e en="T" t="{tag}" n="{name}">@keep()</e>')
        elif tag_name in tags_to_dateshift:
            script.append(f'   <e en="T" t="{tag}" n="{name}">@incrementdate(this,@DATEINC)</e>')
        elif tag_name in tags_to_randomize:
            hash_method = HASH_METHODS.get(name, "@hash(this)")
            script.append(f'   <e en="T" t="{tag}" n="{name}">{hash_method}</e>')
    
    script.extend([
        '   <r en="T" t="curves">Remove curves</r>',
        '   <r en="T" t="overlays">Remove overlays</r>',
        '   <r en="T" t="privategroups">Remove private groups</r>',
        '   <r en="T" t="unspecifiedelements">Remove unchecked elements</r>',
        '</script>'
    ])
    
    return '\n'.join(script)

def generate_lookup_table(lookup_file=None):
    lookup_table = """"""
    if not lookup_file:
        return lookup_table
    
    df = pd.read_excel(lookup_file)
    
    # Iterate through each row
    for _, row in df.iterrows():
        tag_name = row[0]  # First column contains tag name
        original_value = row[1]  # Second column contains original value 
        new_value = row[2]  # Third column contains new value
        
        # Get the element name for the lookup table format
        element_name = LOOKUP_ELEMENT_NAMES.get(tag_name, 'ptid')
        
        # Add the mapping to the lookup table
        lookup_table += f"{element_name}/{original_value}={new_value}\n"
        
    return lookup_table
