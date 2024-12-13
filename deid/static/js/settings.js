// Common settings functionality
function getCsrfToken() {
    return document.querySelector('meta[name="csrf_token"]').getAttribute('content');
}

function addFilterToModality(modalityId) {
    const filterContainer = document.getElementById(`filter-container-${modalityId}`);
    const template = document.querySelector('#filter-template');
    const newFilter = template.cloneNode(true);
    newFilter.style.display = 'block';
    newFilter.removeAttribute('id');
    filterContainer.appendChild(newFilter);
}

function loadFiltersFromSettings(filters) {
    document.querySelector('#filters-container').innerHTML = '';
    if (filters.general_filters) {
        filters.general_filters.forEach(filter => {
            const newFilter = addFilter();
            newFilter.querySelector('[name="tag"]').value = filter.tag;
            newFilter.querySelector('[name="action"]').value = filter.action;
            newFilter.querySelector('[name="value"]').value = filter.value;
        });
    }
    if (filters.modality_filters) {
        Object.entries(filters.modality_filters).forEach(([modality, modalityFilters]) => {
            const filterContainer = document.querySelector(`#filter-container-${modality}`);
            if (filterContainer) {
                filterContainer.innerHTML = '';
                modalityFilters.forEach(filter => {
                    const template = document.querySelector('#filter-template');
                    const newFilter = template.cloneNode(true);
                    newFilter.style.display = 'block';
                    newFilter.removeAttribute('id');
                    newFilter.classList.add('filter-row');
                    
                    newFilter.querySelector('[name="tag"]').value = filter.tag;
                    newFilter.querySelector('[name="action"]').value = filter.action;
                    newFilter.querySelector('[name="value"]').value = filter.value;
                    
                    filterContainer.appendChild(newFilter);
                });
            }
        });
    }
}

// ... existing functions ...

function collectFilterData() {
    // Collect general filters
    const generalFilters = Array.from(document.querySelectorAll('#filters-container .filter-row')).map(row => ({
        tag: row.querySelector('[name="tag"]').value,
        action: row.querySelector('[name="action"]').value,
        value: row.querySelector('[name="value"]').value
    }));

    // Collect modality filters
    const modalityFilters = {};
    modalities.forEach(modality => {
        const filterContainer = document.querySelector(`#filter-container-${modality}`);
        if (filterContainer) {
            const filters = Array.from(filterContainer.querySelectorAll('.filter-row')).map(row => ({
                tag: row.querySelector('[name="tag"]').value,
                action: row.querySelector('[name="action"]').value,
                value: row.querySelector('[name="value"]').value
            }));
            if (filters.length > 0) {
                modalityFilters[modality] = filters;
            }
        }
    });

    return {
        general_filters: generalFilters,
        modality_filters: modalityFilters
    };
}