def preprocess_input(general_filters, modality_filters):
    # Build general filters string
    general_str = " * ".join(
        f'{f["tag"]}.{f["action"]}("{f["value"]}")' for f in general_filters
    )

    # Build modality filters string
    modality_strs = []
    for modality, filters in modality_filters.items():
        modality_expr = " * ".join(
            f'{f["tag"]}.{f["action"]}("{f["value"]}")' for f in filters
        )
        modality_strs.append(f"({modality_expr})")
    
    modalities_str = " + ".join(modality_strs)

    # Combine into full expression
    if general_str and modalities_str:
        return f"{general_str} * ({modalities_str})"
    elif general_str:
        return general_str
    else:
        return modalities_str
