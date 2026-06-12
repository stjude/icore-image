import { useRef, useState } from 'react';

import { loadSettings } from '../api/endpoints';
import { useConstants } from '../hooks/useConstants';

export interface Filter {
  tag: string;
  action: string;
  value: string;
}

export type ModalityFilterMap = Record<string, Filter[]>;

export const FILTER_ACTIONS: [string, string][] = [
  ['equalsIgnoreCase', 'Equals'],
  ['not_equalsIgnoreCase', 'Does Not Equal'],
  ['containsIgnoreCase', 'Contains'],
  ['not_containsIgnoreCase', 'Does Not Contain'],
  ['startsWithIgnoreCase', 'Starts With'],
  ['not_startsWithIgnoreCase', 'Does Not Start With'],
  ['endsWithIgnoreCase', 'Ends With'],
  ['not_endsWithIgnoreCase', 'Does Not End With'],
  ['isLessThan', 'Less Than'],
  ['isGreaterThan', 'Greater Than'],
];

export function newFilter(dicomFields: [string, string][]): Filter {
  return {
    tag: dicomFields[0]?.[0] ?? '',
    action: FILTER_ACTIONS[0]?.[0] ?? '',
    value: '',
  };
}

interface FilterRowProps {
  filter: Filter;
  onChange: (filter: Filter) => void;
  onRemove: () => void;
}

/** One tag/operation/value row, mirroring components/add_filters.html. */
export function FilterRow({ filter, onChange, onRemove }: FilterRowProps) {
  const constants = useConstants();
  return (
    <div className="filter-row w-full flex items-center gap-2 mt-2">
      <select
        name="tag"
        className="h-full border-2 border-gray-400 p-2 bg-white"
        required
        value={filter.tag}
        onChange={(event) => {
          onChange({ ...filter, tag: event.target.value });
        }}
      >
        {(constants?.dicom_fields ?? []).map(([fieldId, fieldName]) => (
          <option key={fieldId} value={fieldId}>
            {fieldName}
          </option>
        ))}
      </select>
      <select
        name="action"
        className="h-full border-2 border-gray-400 p-2 bg-white"
        required
        value={filter.action}
        onChange={(event) => {
          onChange({ ...filter, action: event.target.value });
        }}
      >
        {FILTER_ACTIONS.map(([actionId, actionName]) => (
          <option key={actionId} value={actionId}>
            {actionName}
          </option>
        ))}
      </select>
      <input
        className="h-10 border-2 border-gray-400 p-1"
        type="text"
        name="value"
        placeholder="Enter value"
        required
        value={filter.value}
        onChange={(event) => {
          onChange({ ...filter, value: event.target.value });
        }}
      />
      <button type="button" onClick={onRemove}>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          fill="none"
          viewBox="0 0 24 24"
          strokeWidth="1.5"
          stroke="currentColor"
          className="size-6"
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18 18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
}

interface FilterListProps {
  filters: Filter[];
  onChange: (filters: Filter[]) => void;
}

/** Filter rows + "+ Add Filter" button (components/add_filters.html). */
export function FilterList({ filters, onChange }: FilterListProps) {
  const constants = useConstants();
  return (
    <>
      <div className="w-full flex-row gap-2">
        {filters.map((filter, index) => (
          <FilterRow
            key={index}
            filter={filter}
            onChange={(updated) => {
              onChange(filters.map((f, i) => (i === index ? updated : f)));
            }}
            onRemove={() => {
              onChange(filters.filter((_, i) => i !== index));
            }}
          />
        ))}
      </div>
      <button
        type="button"
        className="mt-2 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100"
        onClick={() => {
          onChange([...filters, newFilter(constants?.dicom_fields ?? [])]);
        }}
      >
        + Add Filter
      </button>
    </>
  );
}

interface ModalityFiltersProps {
  modalities: string[];
  value: ModalityFilterMap;
  onChange: (value: ModalityFilterMap) => void;
}

/** Per-modality filter lists (the filters-{modality} containers). */
export function ModalityFilters({ modalities, value, onChange }: ModalityFiltersProps) {
  return (
    <div className="grid grid-cols-1 gap-4">
      {modalities.map((modality) => (
        <div key={modality}>
          <div className="text-sm text-gray-600">{modality} Filters</div>
          <div className="ml-4 mt-2">
            <div className="space-y-2">
              <FilterList
                filters={value[modality] ?? []}
                onChange={(filters) => {
                  onChange({ ...value, [modality]: filters });
                }}
              />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

export interface SavedFilters {
  general_filters?: Filter[];
  modality_filters?: Record<string, Filter[]>;
}

/** Consolidated filter state for the run pages: general filters plus the
 * modality toggle/checkbox/per-modality lists, with the legacy behaviors —
 * a modality's filter UI is created on first check (loading that modality's
 * saved filters from `settings[savedFiltersKey]` via a fresh fetch) and only
 * hidden on uncheck.
 */
export function useFilters(savedFiltersKey: string) {
  const [generalFilters, setGeneralFilters] = useState<Filter[]>([]);
  const [modalityEnabled, setModalityEnabled] = useState(false);
  const [checked, setChecked] = useState<Record<string, boolean>>({});
  const [initialized, setInitialized] = useState<Record<string, boolean>>({});
  const initializedRef = useRef(new Set<string>());
  const [modalityFilters, setModalityFilters] = useState<Record<string, Filter[]>>({});

  const handleModalitySelection = (modality: string, isChecked: boolean) => {
    setChecked((prev) => ({ ...prev, [modality]: isChecked }));
    if (isChecked && !initializedRef.current.has(modality)) {
      initializedRef.current.add(modality);
      setInitialized((prev) => ({ ...prev, [modality]: true }));
      void loadSettings()
        .then((settings) => {
          const saved =
            (settings[savedFiltersKey] as SavedFilters | undefined)?.modality_filters?.[
              modality
            ] ?? [];
          if (saved.length > 0) {
            setModalityFilters((prev) => ({
              ...prev,
              [modality]: [...(prev[modality] ?? []), ...saved],
            }));
          }
        })
        .catch((error: unknown) => {
          console.error('Error loading settings:', error);
        });
    }
  };

  /** Legacy loadFiltersFromSettings(): replaces the general filter rows, and
   * replaces filters only for modalities whose containers already exist.
   * Saved filters for not-yet-initialized modalities are silently dropped. */
  const applyFromSettings = (filters: SavedFilters) => {
    setGeneralFilters(filters.general_filters ?? []);
    const saved = filters.modality_filters;
    if (saved) {
      setModalityFilters((prev) => {
        const next = { ...prev };
        Object.entries(saved).forEach(([modality, list]) => {
          if (initializedRef.current.has(modality)) {
            next[modality] = list;
          }
        });
        return next;
      });
    }
  };

  const payload = (modalities: string[]) =>
    buildFiltersPayload(
      generalFilters,
      modalityEnabled,
      modalities.filter((modality) => checked[modality]),
      modalityFilters,
    );

  return {
    generalFilters,
    setGeneralFilters,
    modalityEnabled,
    setModalityEnabled,
    checked,
    initialized,
    modalityFilters,
    setModalityFilters,
    handleModalitySelection,
    applyFromSettings,
    payload,
  };
}

interface ModalityFilterSectionProps {
  modalities: string[];
  filters: ReturnType<typeof useFilters>;
  disabled?: boolean;
}

/** "Filter by Modality" toggle + per-modality checkbox/filter lists, shared
 * verbatim by the query/deid/export/single-click pages. */
export function ModalityFilterSection({
  modalities,
  filters,
  disabled = false,
}: ModalityFilterSectionProps) {
  return (
    <div className="mt-4">
      <div className="flex items-center">
        <input
          type="checkbox"
          id="modality-filter-toggle"
          className="mr-2"
          checked={filters.modalityEnabled}
          disabled={disabled}
          onChange={(event) => {
            // Legacy toggleModalityFilters() also cleared the (always empty)
            // #modality-filters-container; per-modality filter state is
            // intentionally preserved across toggles.
            filters.setModalityEnabled(event.target.checked);
          }}
        />
        <label htmlFor="modality-filter-toggle">Filter by Modality</label>
      </div>

      {/* Modality options */}
      <div
        id="modality-options"
        className={filters.modalityEnabled ? 'ml-4 mt-2' : 'ml-4 mt-2 hidden'}
      >
        <div className="grid grid-cols-1 gap-2">
          {modalities.map((modality) => (
            <div key={modality}>
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id={`modality-${modality}`}
                  value={modality}
                  className="modality-checkbox mr-2"
                  checked={filters.checked[modality] ?? false}
                  disabled={disabled}
                  onChange={(event) => {
                    filters.handleModalitySelection(modality, event.target.checked);
                  }}
                />
                <label htmlFor={`modality-${modality}`}>{modality}</label>
              </div>
              <div
                id={`filters-${modality}`}
                className={filters.checked[modality] ? 'ml-4 mt-2' : 'ml-4 mt-2 hidden'}
              >
                {filters.initialized[modality] && (
                  <fieldset disabled={disabled} className="m-0 p-0 border-0 min-w-0">
                    <FilterList
                      filters={filters.modalityFilters[modality] ?? []}
                      onChange={(updated) => {
                        filters.setModalityFilters((prev) => ({
                          ...prev,
                          [modality]: updated,
                        }));
                      }}
                    />
                  </fieldset>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Container for modality-specific filters (unused; kept from the
          legacy template, where nothing was ever appended to it) */}
      <div id="modality-filters-container" className="ml-4 mt-2"></div>
    </div>
  );
}

export interface FiltersPayload {
  general_filters: Filter[];
  modality_filters: Record<string, Filter[]>;
}

/** Equivalent of the legacy collectFilters(): assemble the request payload.
 * Each selected modality's filters gain a trailing Modality-equals entry. */
export function buildFiltersPayload(
  generalFilters: Filter[],
  modalityFiltersEnabled: boolean,
  checkedModalities: string[],
  modalityFilters: ModalityFilterMap,
): FiltersPayload {
  const payload: FiltersPayload = {
    general_filters: generalFilters.filter((f) => f.tag && f.action),
    modality_filters: {},
  };
  if (modalityFiltersEnabled) {
    for (const modality of checkedModalities) {
      const filters = (modalityFilters[modality] ?? []).filter((f) => f.tag && f.action);
      payload.modality_filters[modality] = [
        ...filters,
        { tag: 'Modality', action: 'equals', value: modality },
      ];
    }
  }
  return payload;
}
