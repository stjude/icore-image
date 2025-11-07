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
            newFilter.querySelector('[name="action"]').value = generateActionString(filter.action);
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
                    newFilter.querySelector('[name="action"]').value = generateActionString(filter.action);
                    newFilter.querySelector('[name="value"]').value = filter.value;
                    
                    filterContainer.appendChild(newFilter);
                });
            }
        });
    }
}

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

function generateActionString(action) {
    if (action == 'DoesNotContain') {
        return 'not_containsIgnoreCase'
    }
    if (action == 'Contains') {
        return 'containsIgnoreCase'
    }
    if (action == 'DoesNotStartWith') {
        return 'not_startsWithIgnoreCase'
    }
    if (action == 'StartsWith') {
        return 'startsWithIgnoreCase'
    }
    if (action == 'DoesNotEndWith') {
        return 'not_endsWithIgnoreCase'
    }
    if (action == 'EndsWith') {
        return 'endsWithIgnoreCase'
    }
    if (action == 'DoesNotEqual') {
        return 'not_equalsIgnoreCase'
    }
    if (action == 'Equals') {
        return 'equalsIgnoreCase'
    }
    return action;
}

async function handleProtocolChange() {
    const protocolId = document.getElementById('protocol_select').value;
    if (!protocolId) return;

    try {
        const response = await fetch(`/get_protocol_settings/${protocolId}/`);
        if (!response.ok) throw new Error('Failed to load protocol settings');
        
        const data = await response.json();
        if (data.protocol_settings) {
            loadProtocolSettings(data.protocol_settings);
        }
    } catch (error) {
        console.error('Error loading protocol settings:', error);
    }
}

function loadProtocolSettings(settings) {
    // Load text area values
    if (settings.tags_to_keep) {
        document.querySelector('[name="default_tags_to_keep"]').value = settings.tags_to_keep;
    }
    if (settings.tags_to_dateshift) {
        document.querySelector('[name="default_tags_to_dateshift"]').value = settings.tags_to_dateshift;
    }
    if (settings.tags_to_randomize) {
        document.querySelector('[name="default_tags_to_randomize"]').value = settings.tags_to_randomize;
    }
    
    // Load date shift days
    if (settings.date_shift_days) {
        document.querySelector('[name="default_date_shift_days"]').value = settings.date_shift_days;
    }

    // Handle restricted status
    const isRestricted = settings.is_restricted;
    const inputs = document.querySelectorAll('input, textarea, select');
    inputs.forEach(input => {
        input.disabled = isRestricted;
    });
    
    // Keep the protocol selector enabled even in restricted mode
    document.getElementById('protocol_select').disabled = false;

    // Load filters if they exist
    loadFiltersFromSettings(settings.filters);
}