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


def generate_anonymizer_script(tags_to_keep, tags_to_dateshift, tags_to_randomize, date_shift_days):
    # Start of script
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
    for tag_name in sorted(set(tags_to_keep + tags_to_dateshift + tags_to_randomize)):
        tag = tag_dict[tag_name]
        if tag_name in tags_to_keep:
            script.append(f'   <e en="T" t="{tag}" n="{tag_name}">@keep()</e>')
        elif tag_name in tags_to_dateshift:
            script.append(f'   <e en="T" t="{tag}" n="{tag_name}">@incrementdate(this,@DATEINC)</e>')
        elif tag_name in tags_to_randomize:
            script.append(f'   <e en="T" t="{tag}" n="{tag_name}">@hashuid(@UIDROOT,this)</e>')
    
    # Close script
    script.append('</script>')
    
    return '\n'.join(script)
    