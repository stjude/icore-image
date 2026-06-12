import { useEffect, useRef, useState } from 'react';
import { useOutletContext } from 'react-router';

import { loadSettings, saveSettings } from '../../api/endpoints';
import type { ColumnAction } from '../../api/types';
import { getCsrfToken } from '../../lib/csrf';
import type { SettingsOutletContext } from './SettingsLayout';

// Note: the legacy template also injected a `protocols` JSON blob
// ({{ protocols|json_script:"protocols-data" }}) and parsed it into a global,
// but the page scripts never used it, so it is intentionally not ported.

const COLUMN_ACTIONS: ColumnAction[] = ['keep', 'deid', 'drop'];

interface ColumnRow {
  id: number;
  name: string;
}

type ColumnGroups = Record<ColumnAction, ColumnRow[]>;

function capitalize(action: string): string {
  return action.charAt(0).toUpperCase() + action.slice(1);
}

export function TextDeidSettings() {
  const { registerSaveHandler, showSaveMessage } = useOutletContext<SettingsOutletContext>();

  const [groups, setGroups] = useState<ColumnGroups>({ keep: [], deid: [], drop: [] });
  const [textToKeep, setTextToKeep] = useState('');
  const [textToRemove, setTextToRemove] = useState('');

  const nextIdRef = useRef(0);
  const inputRefs = useRef(new Map<number, HTMLInputElement>());
  const pendingFocusId = useRef<number | null>(null);

  // Legacy loadSavedSettings() on DOMContentLoaded: build a row per
  // remembered column action and pre-fill the default keep/remove phrases.
  useEffect(() => {
    void (async () => {
      try {
        const settings = await loadSettings();

        const columnActions = settings.column_actions || settings.default_column_actions || {};
        const loaded: ColumnGroups = { keep: [], deid: [], drop: [] };
        Object.entries(columnActions).forEach(([name, action]) => {
          if (!COLUMN_ACTIONS.includes(action)) return;
          loaded[action].push({ id: nextIdRef.current++, name });
        });
        setGroups(loaded);

        const defaultTextToKeep = settings.default_text_to_keep;
        if (typeof defaultTextToKeep === 'string' && defaultTextToKeep) {
          setTextToKeep(defaultTextToKeep);
        }
        const defaultTextToRemove = settings.default_text_to_remove;
        if (typeof defaultTextToRemove === 'string' && defaultTextToRemove) {
          setTextToRemove(defaultTextToRemove);
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  // Legacy addColumnRow() focused the new row's input after appending it.
  useEffect(() => {
    if (pendingFocusId.current !== null) {
      inputRefs.current.get(pendingFocusId.current)?.focus();
      pendingFocusId.current = null;
    }
  }, [groups]);

  const addColumnRow = (action: ColumnAction) => {
    const id = nextIdRef.current++;
    pendingFocusId.current = id;
    setGroups((prev) => ({ ...prev, [action]: [...prev[action], { id, name: '' }] }));
  };

  const updateRowName = (action: ColumnAction, id: number, name: string) => {
    setGroups((prev) => ({
      ...prev,
      [action]: prev[action].map((row) => (row.id === id ? { ...row, name } : row)),
    }));
  };

  // Legacy behavior: changing the action moves the row to the end of the
  // matching group (appendChild into `group-<action>`).
  const moveRow = (id: number, from: ColumnAction, to: ColumnAction) => {
    setGroups((prev) => {
      const row = prev[from].find((r) => r.id === id);
      if (!row || from === to) return prev;
      return {
        ...prev,
        [from]: prev[from].filter((r) => r.id !== id),
        [to]: [...prev[to], row],
      };
    });
  };

  const removeRow = (action: ColumnAction, id: number) => {
    setGroups((prev) => ({ ...prev, [action]: prev[action].filter((row) => row.id !== id) }));
  };

  // Legacy collectColumnActionsFromGroups(): iterate rows in document order
  // (keep, then deid, then drop), trim names, skip blanks; duplicate names
  // resolve to the last row encountered.
  const collectColumnActionsFromGroups = (): Record<string, ColumnAction> => {
    const columnActions: Record<string, ColumnAction> = {};
    COLUMN_ACTIONS.forEach((action) => {
      groups[action].forEach((row) => {
        const name = row.name.trim();
        if (name) {
          columnActions[name] = action;
        }
      });
    });
    return columnActions;
  };

  const handleSave = async () => {
    const data = {
      column_actions: collectColumnActionsFromGroups(),
      default_text_to_keep: textToKeep,
      default_text_to_remove: textToRemove,
    };

    try {
      await saveSettings(data);
      showSaveMessage('Settings Saved');
    } catch (error) {
      console.error('Error saving settings:', error);
      showSaveMessage('Error Saving Settings', true);
    }
  };

  // The layout's Save Settings button calls whatever is registered; keep the
  // registered wrapper stable while always invoking the latest handler.
  const saveRef = useRef(handleSave);
  saveRef.current = handleSave;
  useEffect(() => {
    registerSaveHandler(() => void saveRef.current());
    return () => {
      registerSaveHandler(null);
    };
  }, [registerSaveHandler]);

  // Ported verbatim from the legacy resetTextDeidBtn click handler, including
  // its non-OK alert (truncated body text) and full-page reload on success.
  const handleReset = async () => {
    const confirmed = window.confirm(
      'Are you sure you want to reset all Text Deidentification settings to iCore defaults? This action cannot be undone.',
    );

    if (!confirmed) {
      return;
    }

    try {
      const response = await fetch('/reset_deid_settings/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCsrfToken(),
        },
        body: JSON.stringify({
          settings_type: 'text_deid',
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        window.alert('Server error (status ' + String(response.status) + '): ' + text.substring(0, 200));
        return;
      }

      const result = (await response.json()) as { status?: string; message?: string };

      if (result.status === 'success') {
        window.scrollTo(0, 0);
        window.location.reload();
      } else {
        window.alert('Error resetting settings: ' + (result.message ?? ''));
      }
    } catch (error) {
      console.error('Error resetting settings:', error);
      const message = error instanceof Error ? error.message : String(error);
      window.alert('Error resetting settings: ' + message + '. Please try again.');
    }
  };

  return (
    <>
      <div className="mt-6">
        <div className="text-md mb-1">Remembered Column Actions</div>
        <div className="text-sm text-gray-500 mb-4">
          When a spreadsheet column matches one of these names, the chosen action is applied
          automatically. Selections you make while running a job are saved here, and you can edit
          them at any time.
        </div>

        {COLUMN_ACTIONS.map((action) => (
          <div key={action} className="mb-6">
            <div className="font-medium mb-2">{capitalize(action)}</div>
            <div id={`group-${action}`} className="space-y-2">
              {groups[action].map((row) => (
                <div key={row.id} className="column-setting-row flex items-center gap-2">
                  <input
                    type="text"
                    className="flex-1 border-2 border-gray-400 p-2"
                    placeholder="Column name"
                    value={row.name}
                    ref={(element) => {
                      if (element) {
                        inputRefs.current.set(row.id, element);
                      } else {
                        inputRefs.current.delete(row.id);
                      }
                    }}
                    onChange={(event) => {
                      updateRowName(action, row.id, event.target.value);
                    }}
                  />
                  <select
                    className="border-2 border-gray-400 p-2 bg-white"
                    value={action}
                    onChange={(event) => {
                      moveRow(row.id, action, event.target.value as ColumnAction);
                    }}
                  >
                    {COLUMN_ACTIONS.map((option) => (
                      <option key={option} value={option}>
                        {capitalize(option)}
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    className="px-2 text-gray-500 hover:text-black"
                    onClick={() => {
                      removeRow(action, row.id);
                    }}
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
            <button
              type="button"
              className="mt-2 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100"
              onClick={() => {
                addColumnRow(action);
              }}
            >
              + Add column
            </button>
          </div>
        ))}
      </div>

      <div className="mt-8">
        <div className="text-md mb-4">Default Deidentification Lists</div>
        <div className="flex space-x-4">
          <div className="flex-1">
            <div className="text-sm text-gray-500">Phrases to keep</div>
            <textarea
              className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
              name="default_text_to_keep"
              value={textToKeep}
              onChange={(event) => {
                setTextToKeep(event.target.value);
              }}
            ></textarea>
          </div>
          <div className="flex-1">
            <div className="text-sm text-gray-500">Phrases to remove</div>
            <textarea
              className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
              name="default_text_to_remove"
              value={textToRemove}
              onChange={(event) => {
                setTextToRemove(event.target.value);
              }}
            ></textarea>
          </div>
        </div>
      </div>

      {/* Reset Settings Section */}
      <div className="mt-12 pt-4 border-t border-gray-300">
        <div className="text-md mb-2">Reset Deidentification Settings</div>
        <a
          href="#"
          id="resetTextDeidBtn"
          className="text-sm text-blue-600 hover:underline"
          onClick={(event) => {
            event.preventDefault();
            void handleReset();
          }}
        >
          Reset to iCore defaults.
        </a>
      </div>
    </>
  );
}
