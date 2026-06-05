// Per-column Keep/Deid/Drop handling for uploaded spreadsheets.
// Shared by the Text Deidentification and Single-Click iCore screens.

const COLUMN_ACTION_OPTIONS = ['keep', 'deid', 'drop'];

// Pages may define a global `revalidateColumnActionsForm()` to re-run their
// run-button validation whenever the column selections change.
function notifyColumnActionsChanged() {
    if (typeof revalidateColumnActionsForm === 'function') {
        revalidateColumnActionsForm();
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

// Read the uploaded spreadsheet's columns and render a selector per column,
// pre-selecting any previously remembered action.
async function loadColumnActions(inputPath) {
    const container = document.getElementById('column-actions-container');
    if (!container) return;

    container.innerHTML = '';

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
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ input_file: path })
        });
        const result = await response.json();
        if (result.status !== 'success') {
            setColumnActionsStatus(result.message || 'Could not read columns from the file.');
            notifyColumnActionsChanged();
            return;
        }
        columns = result.columns || [];
    } catch (error) {
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

    const template = document.getElementById('column-action-template');
    columns.forEach(column => {
        const row = template.cloneNode(true);
        row.removeAttribute('id');
        row.style.display = 'flex';

        const nameEl = row.querySelector('.column-action-name');
        nameEl.textContent = column;
        nameEl.setAttribute('title', column);

        const select = row.querySelector('select[name="column_action"]');
        const saved = savedActions[column];
        select.value = COLUMN_ACTION_OPTIONS.includes(saved) ? saved : '';
        select.addEventListener('change', notifyColumnActionsChanged);

        container.appendChild(row);
    });

    if (columns.length === 0) {
        setColumnActionsStatus('No columns found in the spreadsheet.');
    } else {
        setColumnActionsStatus('');
    }

    notifyColumnActionsChanged();
}

// Collect the current selections as { columnName: action } (assigned rows only).
function collectColumnActions() {
    const actions = {};
    document.querySelectorAll('#column-actions-container .column-action-row').forEach(row => {
        const name = row.querySelector('.column-action-name').textContent;
        const value = row.querySelector('select[name="column_action"]').value;
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
    return Array.from(rows).every(
        row => row.querySelector('select[name="column_action"]').value
    );
}
