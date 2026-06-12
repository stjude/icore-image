import { useState } from 'react';

export type ColumnType = 'accession' | 'mrn' | 'date';

const COLUMN_MODAL_TEXT: Record<ColumnType, { title: string; label: string }> = {
  accession: { title: 'Edit Accession Column Name', label: 'Accession Column Name:' },
  mrn: { title: 'Edit MRN Column Name', label: 'MRN Column Name:' },
  date: { title: 'Edit Date Column Name', label: 'Date Column Name:' },
};

interface ColumnNameModalProps {
  columnType: ColumnType;
  initialValue: string;
  onSave: (value: string) => void;
  onClose: () => void;
}

/** The "Edit Column Name" modal (legacy #column-modal). Saving with an empty
 * (all-whitespace) value closes the modal without changing the column. */
export function ColumnNameModal({
  columnType,
  initialValue,
  onSave,
  onClose,
}: ColumnNameModalProps) {
  const [value, setValue] = useState(initialValue);
  const { title, label } = COLUMN_MODAL_TEXT[columnType];

  const save = () => {
    const trimmed = value.trim();
    if (trimmed) {
      onSave(trimmed);
    }
    onClose();
  };

  return (
    <div
      id="column-modal"
      className="fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center"
      style={{ zIndex: 1000 }}
    >
      <div className="bg-white p-6 rounded-lg shadow-lg w-96">
        <h3 className="text-lg font-medium mb-4" id="modal-title">
          {title}
        </h3>
        <div className="mb-4">
          <label className="text-sm text-gray-600 mb-2 block" id="modal-label">
            {label}
          </label>
          <input
            type="text"
            id="modal-input"
            className="w-full border-2 border-gray-400 p-2"
            autoFocus
            value={value}
            onChange={(event) => {
              setValue(event.target.value);
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                save();
              } else if (event.key === 'Escape') {
                onClose();
              }
            }}
          />
        </div>
        <div className="flex justify-end gap-2">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300"
          >
            Cancel
          </button>
          <button
            onClick={save}
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Save
          </button>
        </div>
      </div>
    </div>
  );
}

interface DateWindowModalProps {
  initialValue: string;
  onSave: (value: number) => void;
  onClose: () => void;
}

/** The "Edit Date Window" modal (legacy #date-window-modal). Saving parses
 * the input as an integer, falling back to 0. */
export function DateWindowModal({ initialValue, onSave, onClose }: DateWindowModalProps) {
  const [value, setValue] = useState(initialValue);

  const save = () => {
    onSave(parseInt(value) || 0);
    onClose();
  };

  return (
    <div
      id="date-window-modal"
      className="fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center"
      style={{ zIndex: 1000 }}
    >
      <div className="bg-white p-6 rounded-lg shadow-lg w-96">
        <h3 className="text-lg font-medium mb-4">Edit Date Window</h3>
        <div className="mb-4">
          <label className="text-sm text-gray-600 mb-2 block">Date Window (Days):</label>
          <input
            type="number"
            id="date-window-input"
            className="w-full border-2 border-gray-400 p-2"
            min={0}
            autoFocus
            value={value}
            onChange={(event) => {
              setValue(event.target.value);
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                save();
              } else if (event.key === 'Escape') {
                onClose();
              }
            }}
          />
          <div className="text-xs text-gray-500 mt-1">
            Number of days before and after the study date to query
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300"
          >
            Cancel
          </button>
          <button
            onClick={save}
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Save
          </button>
        </div>
      </div>
    </div>
  );
}
