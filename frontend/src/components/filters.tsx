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
