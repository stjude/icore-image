function collectFilters(generalFilterContainerId, modalityFilterEnabled = true) {
    const filters = {
        general_filters: [],
        modality_filters: {}
    };

    // Get general filters
    const generalFilterContainer = document.querySelector(generalFilterContainerId);
    if (generalFilterContainer) {
        const filterRows = generalFilterContainer.querySelectorAll('.filter-row:not(#filter-template)');
        filterRows.forEach(filterRow => {
            const tagSelect = filterRow.querySelector('select[name="tag"]');
            const actionSelect = filterRow.querySelector('select[name="action"]');
            const valueInput = filterRow.querySelector('input[name="value"]');
            
            if (tagSelect && actionSelect) {
                const filter = {
                    tag: tagSelect.value,
                    action: actionSelect.value,
                    value: valueInput ? valueInput.value : ''
                };
                if (filter.tag && filter.action) {
                    filters.general_filters.push(filter);
                }
            }
        });
    }

    // Get modality filters if enabled
    if (modalityFilterEnabled && document.getElementById('modality-filter-toggle')?.checked) {
        const checkedModalities = document.querySelectorAll('.modality-checkbox:checked');
        
        checkedModalities.forEach(modalityCheckbox => {
            const modalityId = modalityCheckbox.value;
            const filterContainer = document.getElementById(`filter-container-${modalityId}`);
            
            if (filterContainer) {
                const modalityFilters = [];
                const filterRows = filterContainer.querySelectorAll('.filter-row:not(#filter-template)');
                
                filterRows.forEach(filterRow => {
                    const filter = {
                        tag: filterRow.querySelector('select[name="tag"]')?.value,
                        action: filterRow.querySelector('select[name="action"]')?.value,
                        value: filterRow.querySelector('input[name="value"]')?.value
                    };
                    if (filter.tag && filter.action) {
                        modalityFilters.push(filter);
                    }
                });
                modalityFilters.push({tag: 'Modality', action: 'equals', value: modalityId});
                
                if (modalityFilters.length > 0) {
                    filters.modality_filters[modalityId] = modalityFilters;
                }
            }
        });
    }

    return filters;
} 