// Per-column Keep/Deid/Drop handling for uploaded spreadsheets.
// Shared by the Text Deidentification and IMAGINE Workflow screens.

const COLUMN_ACTION_OPTIONS = ['keep', 'deid', 'drop'];

// Pages may define a global `revalidateColumnActionsForm()` to re-run their
// run-button validation whenever the column selections change.
function notifyColumnActionsChanged() {
    markUnassignedColumns();
    if (typeof revalidateColumnActionsForm === 'function') {
        revalidateColumnActionsForm();
    }
    expandColumnOptionsIfNeeded();
}

// Flag every column row that still has no action chosen with a red left line and a
// gentle red background, clearing them once a choice is made. `!` important so the left
// line overrides the row's default `border-b border-gray-200` edges.
function markUnassignedColumns() {
    document.querySelectorAll('#column-actions-container .column-action-row').forEach(row => {
        const unassigned = !getRowAction(row);
        row.classList.toggle('!border-l-4', unassigned);
        row.classList.toggle('!border-l-red-600', unassigned);
        row.classList.toggle('bg-red-50', unassigned);
    });
}

// Once a spreadsheet's columns are loaded but not all assigned, reveal the
// Deidentification Options panel so the unassigned columns are visible. Only ever
// expands (never collapses), so it won't fight a user who closes it deliberately.
function expandColumnOptionsIfNeeded() {
    const rows = document.querySelectorAll('#column-actions-container .column-action-row');
    if (rows.length === 0 || allColumnsAssigned()) return;
    const options = document.getElementById('deid_options');
    if (options && options.classList.contains('hidden')) {
        options.classList.remove('hidden');
        const button = document.getElementById('deidOptionsButton');
        if (button) button.textContent = '▲';
    }
}

function setColumnActionsStatus(message) {
    const status = document.getElementById('column-actions-status');
    if (!status) return;
    if (message) {
        status.textContent = message;
        status.classList.remove('hidden');
    } else {
        status.classList.add('hidden');
    }
}

function setDropUnassignedVisible(visible) {
    const button = document.getElementById('drop-unassigned-button');
    if (!button) return;
    button.classList.toggle('hidden', !visible);
}

// Currently selected action for a row ('' if none chosen).
function getRowAction(row) {
    const checked = row.querySelector('input.column-action-radio:checked');
    return checked ? checked.value : '';
}

// Select the radio matching `value` in a row (no-op for an unknown value).
function setRowAction(row, value) {
    row.querySelectorAll('input.column-action-radio').forEach(radio => {
        radio.checked = radio.value === value;
    });
}

// Default every column that still has no action to "drop".
function dropAllUnassigned() {
    document.querySelectorAll('#column-actions-container .column-action-row').forEach(row => {
        if (!getRowAction(row)) {
            setRowAction(row, 'drop');
        }
    });
    notifyColumnActionsChanged();
}

// Tracks the most recent loadColumnActions() call. The input field's drag-drop
// dispatches both 'input' and 'change', so this can fire twice concurrently;
// only the latest run is allowed to render, preventing duplicated rows.
let _columnActionsLoadSeq = 0;

// Read the uploaded spreadsheet's columns and render a selector per column,
// pre-selecting any previously remembered action.
async function loadColumnActions(inputPath) {
    const container = document.getElementById('column-actions-container');
    if (!container) return;

    const seq = ++_columnActionsLoadSeq;
    container.innerHTML = '';
    setDropUnassignedVisible(false);

    const path = (inputPath || '').trim();
    if (!path.toLowerCase().endsWith('.xlsx')) {
        setColumnActionsStatus('Upload an Excel (.xlsx) spreadsheet to configure its columns.');
        notifyColumnActionsChanged();
        return;
    }

    setColumnActionsStatus('Reading columns…');

    let columns = [];
    try {
        const response = await fetch('/get_spreadsheet_columns/', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCsrfToken() },
            body: JSON.stringify({ input_file: path })
        });
        const result = await response.json();
        if (seq !== _columnActionsLoadSeq) return; // superseded by a newer call
        if (result.status !== 'success') {
            setColumnActionsStatus(result.message || 'Could not read columns from the file.');
            notifyColumnActionsChanged();
            return;
        }
        columns = result.columns || [];
    } catch (error) {
        if (seq !== _columnActionsLoadSeq) return;
        console.error('Error reading spreadsheet columns:', error);
        setColumnActionsStatus('Could not read columns from the file.');
        notifyColumnActionsChanged();
        return;
    }

    // Load remembered actions (fall back to bundled defaults for first use).
    let savedActions = {};
    try {
        const settingsResp = await fetch('/load_settings/');
        const settings = await settingsResp.json();
        savedActions = settings.column_actions || settings.default_column_actions || {};
    } catch (error) {
        console.error('Error loading settings:', error);
    }
    if (seq !== _columnActionsLoadSeq) return; // superseded while awaiting settings

    const template = document.getElementById('column-action-template');
    columns.forEach((column, index) => {
        const row = template.cloneNode(true);
        row.removeAttribute('id');
        row.style.display = 'flex';

        const nameEl = row.querySelector('.column-action-name');
        nameEl.textContent = column;
        nameEl.setAttribute('title', column);

        // Give this row's radios a unique group name so they're mutually
        // exclusive within the row but independent of other columns.
        const groupName = `column_action_${index}`;
        row.querySelectorAll('input.column-action-radio').forEach(radio => {
            radio.name = groupName;
            radio.addEventListener('change', notifyColumnActionsChanged);
        });

        const saved = savedActions[column];
        if (COLUMN_ACTION_OPTIONS.includes(saved)) {
            setRowAction(row, saved);
        }

        container.appendChild(row);
    });

    if (columns.length === 0) {
        setColumnActionsStatus('No columns found in the spreadsheet.');
    } else {
        setColumnActionsStatus('');
    }
    setDropUnassignedVisible(columns.length > 0);

    notifyColumnActionsChanged();
}

// Collect the current selections as { columnName: action } (assigned rows only).
function collectColumnActions() {
    const actions = {};
    document.querySelectorAll('#column-actions-container .column-action-row').forEach(row => {
        const name = row.querySelector('.column-action-name').textContent;
        const value = getRowAction(row);
        if (name && value) {
            actions[name] = value;
        }
    });
    return actions;
}

// True only when columns have been loaded and every one has an action chosen.
function allColumnsAssigned() {
    const rows = document.querySelectorAll('#column-actions-container .column-action-row');
    if (rows.length === 0) return false;
    return Array.from(rows).every(row => getRowAction(row));
}
