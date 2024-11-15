// Add this function to load saved settings
async function loadSavedSettings() {
    try {
        const response = await fetch('/get_settings/');
        const settings = await response.json();
        
        // Set default image source
        const imageSourceRadio = document.querySelector(`input[name="default_image_source"][value="${settings.default_image_source}"]`);
        if (imageSourceRadio) imageSourceRadio.checked = true;
        
        // Set default tags
        const tagsToKeep = document.querySelector('[name="default_tags_to_keep"]');
        if (tagsToKeep) tagsToKeep.value = settings.default_tags_to_keep || '';
        
        const tagsToDateshift = document.querySelector('[name="default_tags_to_dateshift"]');
        if (tagsToDateshift) tagsToDateshift.value = settings.default_tags_to_dateshift || '';
        
        const tagsToRandomize = document.querySelector('[name="default_tags_to_randomize"]');
        if (tagsToRandomize) tagsToRandomize.value = settings.default_tags_to_randomize || '';
        
        // Set default date shift
        const dateShiftDays = document.querySelector('[name="default_date_shift_days"]');
        if (dateShiftDays) dateShiftDays.value = settings.default_date_shift_days || '';
        
        // Set ID generation method
        const idMethodRadio = document.querySelector(`input[name="id_generation_method"][value="${settings.id_generation_method}"]`);
        if (idMethodRadio) idMethodRadio.checked = true;
        
        // Load general filters
        if (settings.general_filters && settings.general_filters.length > 0) {
            const filterContainer = document.querySelector('#filters-container');
            settings.general_filters.forEach(filter => {
                const template = document.querySelector('#filter-template');
                const newFilter = template.cloneNode(true);
                newFilter.style.display = 'block';
                newFilter.removeAttribute('id');
                
                const tagSelect = newFilter.querySelector('select[name="tag"]');
                const actionSelect = newFilter.querySelector('select[name="action"]');
                const valueInput = newFilter.querySelector('input[name="value"]');
                
                if (tagSelect) tagSelect.value = filter.tag;
                if (actionSelect) actionSelect.value = filter.action;
                if (valueInput) valueInput.value = filter.value;
                
                filterContainer.insertBefore(newFilter, filterContainer.lastElementChild);
            });
        }
        
        // Load modality filters
        if (settings.modality_filters) {
            Object.entries(settings.modality_filters).forEach(([modality, filters]) => {
                const filterContainer = document.querySelector(`#filter-container-${modality}`);
                if (filterContainer) {
                    filters.forEach(filter => {
                        if (filter.tag !== 'Modality') {  // Skip the modality filter itself
                            const template = document.querySelector('#filter-template');
                            const newFilter = template.cloneNode(true);
                            newFilter.style.display = 'block';
                            newFilter.removeAttribute('id');
                            
                            const tagSelect = newFilter.querySelector('select[name="tag"]');
                            const actionSelect = newFilter.querySelector('select[name="action"]');
                            const valueInput = newFilter.querySelector('input[name="value"]');
                            
                            if (tagSelect) tagSelect.value = filter.tag;
                            if (actionSelect) actionSelect.value = filter.action;
                            if (valueInput) valueInput.value = filter.value;
                            
                            filterContainer.appendChild(newFilter);
                        }
                    });
                }
            });
        }
        
    } catch (error) {
        console.error('Error loading settings:', error);
    }
}