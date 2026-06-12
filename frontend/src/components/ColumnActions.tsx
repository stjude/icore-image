import { useEffect, useState } from 'react';

import { loadSettings } from '../api/endpoints';
import { getSpreadsheetColumns } from '../api/endpoints';
import type { ColumnAction } from '../api/types';

export interface ColumnActionsState {
  /** Selected actions for assigned columns only ({ columnName: action }). */
  actions: Record<string, ColumnAction>;
  /** True only when columns are loaded and every one has an action. */
  allAssigned: boolean;
}

interface Props {
  inputPath: string;
  onStateChange: (state: ColumnActionsState) => void;
}

const COLUMN_ACTION_OPTIONS: ColumnAction[] = ['keep', 'deid', 'drop'];

/** Per-column Keep/Deid/Drop handling for uploaded spreadsheets — port of
 * static/js/column_actions.js + components/column_actions.html. */
export function ColumnActions({ inputPath, onStateChange }: Props) {
  const [columns, setColumns] = useState<string[]>([]);
  const [selected, setSelected] = useState<Record<string, ColumnAction>>({});
  const [status, setStatus] = useState(
    'Upload an Excel (.xlsx) spreadsheet to configure its columns.',
  );

  // Report the collected actions + completeness whenever they change.
  useEffect(() => {
    const actions: Record<string, ColumnAction> = {};
    for (const column of columns) {
      const action = selected[column];
      if (action) actions[column] = action;
    }
    onStateChange({
      actions,
      allAssigned: columns.length > 0 && columns.every((c) => selected[c]),
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onStateChange identity is not load-bearing
  }, [columns, selected]);

  // (Re)load columns whenever the input path changes. The effect-cleanup flag
  // replaces the legacy _columnActionsLoadSeq race guard.
  useEffect(() => {
    let cancelled = false;
    setColumns([]);
    setSelected({});

    const path = inputPath.trim();
    if (!path.toLowerCase().endsWith('.xlsx')) {
      setStatus('Upload an Excel (.xlsx) spreadsheet to configure its columns.');
      return;
    }

    setStatus('Reading columns…');
    void (async () => {
      let loaded: string[];
      try {
        const result = await getSpreadsheetColumns(path);
        if (cancelled) return;
        if (result.status !== 'success') {
          setStatus(result.message ?? 'Could not read columns from the file.');
          return;
        }
        loaded = result.columns ?? [];
      } catch (error) {
        if (cancelled) return;
        console.error('Error reading spreadsheet columns:', error);
        setStatus('Could not read columns from the file.');
        return;
      }

      // Pre-select remembered actions (bundled defaults on first use).
      let saved: Record<string, ColumnAction> = {};
      try {
        const settings = await loadSettings();
        saved = settings.column_actions ?? settings.default_column_actions ?? {};
      } catch (error) {
        console.error('Error loading settings:', error);
      }
      if (cancelled) return;

      const preselected: Record<string, ColumnAction> = {};
      for (const column of loaded) {
        const action = saved[column];
        if (action && COLUMN_ACTION_OPTIONS.includes(action)) {
          preselected[column] = action;
        }
      }
      setColumns(loaded);
      setSelected(preselected);
      setStatus(loaded.length === 0 ? 'No columns found in the spreadsheet.' : '');
    })();

    return () => {
      cancelled = true;
    };
  }, [inputPath]);

  const dropAllUnassigned = () => {
    setSelected((current) => {
      const next = { ...current };
      for (const column of columns) {
        next[column] ??= 'drop';
      }
      return next;
    });
  };

  return (
    <>
      <div id="column-actions-section">
        <div className="text-sm text-gray-500">Column Handling</div>
        <div className="text-xs text-gray-400 mb-2">
          Choose an action for each column in the uploaded spreadsheet. Every column must
          be assigned before deidentification can run.
        </div>
        {columns.length > 0 && (
          <button
            type="button"
            id="drop-unassigned-button"
            className="mb-2 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100"
            onClick={dropAllUnassigned}
          >
            Drop all unassigned
          </button>
        )}
        {status && (
          <div id="column-actions-status" className="text-sm text-gray-500 italic">
            {status}
          </div>
        )}
        <div id="column-actions-container" className="w-full">
          {columns.map((column, index) => (
            <div
              key={column}
              className="column-action-row flex items-center gap-3 border-b border-gray-200"
            >
              <div className="column-action-name flex-1 truncate text-sm p-3" title={column}>
                {column}
              </div>
              <div className="flex items-center gap-4 p-3">
                {COLUMN_ACTION_OPTIONS.map((option) => (
                  <label
                    key={option}
                    className="flex items-center gap-1 text-sm cursor-pointer"
                  >
                    <input
                      type="radio"
                      className="column-action-radio"
                      name={`column_action_${index}`}
                      value={option}
                      checked={selected[column] === option}
                      onChange={() => {
                        setSelected((current) => ({ ...current, [column]: option }));
                      }}
                    />{' '}
                    {option === 'keep' ? 'Keep' : option === 'deid' ? 'Deid' : 'Drop'}
                  </label>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}
