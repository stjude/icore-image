import { useEffect, useRef, useState } from 'react';

import { ApiError, postJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import type { RunResponse, Settings } from '../api/types';
import { buildFiltersPayload, FilterList, type Filter, type ModalityFilterMap } from '../components/filters';
import { PathInput } from '../components/PathInput';
import { useConstants } from '../hooks/useConstants';

type QueryType = 'accession' | 'mrn_date';
type ColumnType = 'accession' | 'mrn' | 'date';

/** Shape of the saved filters blobs in settings.json (query_filters /
 * filters keys), matching what the legacy inline JS read from them. */
interface SavedFilters {
  general_filters?: Filter[];
  modality_filters?: Record<string, Filter[]>;
}

const COLUMN_MODAL_TITLES: Record<ColumnType, string> = {
  accession: 'Edit Accession Column Name',
  mrn: 'Edit MRN Column Name',
  date: 'Edit Date Column Name',
};

const COLUMN_MODAL_LABELS: Record<ColumnType, string> = {
  accession: 'Accession Column Name:',
  mrn: 'MRN Column Name:',
  date: 'Date Column Name:',
};

export function ImageQuery() {
  const constants = useConstants();
  // Server context `modalities` (baked into the legacy template).
  const modalities = constants?.modalities ?? [];

  const [studyName, setStudyName] = useState('');
  const [inputFile, setInputFile] = useState('');
  const [outputFolder, setOutputFolder] = useState('');
  const [queryType, setQueryType] = useState<QueryType>('accession');
  const [useFallback, setUseFallback] = useState(false);
  // Legacy showed these defaults in the display spans before settings loaded;
  // the settings pre-fill always overwrites them (with the same fallbacks).
  const [accessionColumn, setAccessionColumn] = useState('AccessionNumber');
  const [mrnColumn, setMrnColumn] = useState('PatientID');
  const [dateColumn, setDateColumn] = useState('StudyDate');
  // Legacy stored settings.default_date_window_days verbatim (number or
  // string) and only parseInt-ed it when building the run payload.
  const [dateWindow, setDateWindow] = useState<number | string>(0);
  const [optionsOpen, setOptionsOpen] = useState(false);

  const [generalFilters, setGeneralFilters] = useState<Filter[]>([]);
  const [modalityFiltersEnabled, setModalityFiltersEnabled] = useState(false);
  const [checkedModalities, setCheckedModalities] = useState<string[]>([]);
  // Modalities whose filter section has been created. Legacy built the
  // section on first check and only hid it on uncheck, keeping its rows.
  const [initializedModalities, setInitializedModalities] = useState<string[]>([]);
  const [modalityFilters, setModalityFilters] = useState<ModalityFilterMap>({});

  // Legacy only attached the validation listeners after /load_settings/
  // succeeded, so the run button stayed disabled (without the dimmed
  // classes) until then — and forever if the settings fetch failed.
  const [validationActive, setValidationActive] = useState(false);

  const [columnModal, setColumnModal] = useState<ColumnType | null>(null);
  const [columnModalInput, setColumnModalInput] = useState('');
  const [dateWindowModalOpen, setDateWindowModalOpen] = useState(false);
  const [dateWindowInput, setDateWindowInput] = useState('0');

  // The run payload reuses pacs_configs/application_aet from the settings
  // loaded on mount, exactly like the legacy page-level `settings` global.
  const settingsRef = useRef<Settings>({});

  useEffect(() => {
    void (async () => {
      try {
        const settings = await loadSettings();
        settingsRef.current = settings;

        // Legacy quirk preserved: anything other than exactly "Accession"
        // (including a missing key) selects the MRN + Date radio.
        if (settings.default_query_method === 'Accession') {
          setQueryType('accession');
        } else {
          setQueryType('mrn_date');
        }
        setAccessionColumn(settings.default_accession_header || 'AccessionNumber');
        setMrnColumn(settings.default_mrn_header || 'PatientID');
        setDateColumn(settings.default_date_header || 'StudyDate');
        setDateWindow(
          settings.default_date_window_days !== undefined ? settings.default_date_window_days : 0,
        );
        if (settings.default_output_folder) {
          setOutputFolder(settings.default_output_folder);
        }
        const queryFilters = settings.query_filters as SavedFilters | undefined;
        if (queryFilters) {
          // Legacy loadFiltersFromSettings(): rebuild the general filter rows.
          // Its modality_filters branch targeted #filter-container-<modality>
          // nodes that never exist at load time, so it was a no-op; preserved.
          if (queryFilters.general_filters) {
            setGeneralFilters(
              queryFilters.general_filters.map((filter) => ({
                tag: filter.tag,
                action: filter.action,
                value: filter.value,
              })),
            );
          }
        }

        setValidationActive(true);
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  const formValid = Boolean(studyName.trim() && inputFile.trim());

  // toggleQueryTypeInputs()/toggleFallbackQueryInputs() visibility rules.
  const accessionVisible = queryType === 'accession';
  const fallbackOptionVisible = queryType === 'accession';
  const mrnDateVisible = queryType === 'mrn_date' || useFallback;

  const setColumnValue = (columnType: ColumnType, value: string) => {
    if (columnType === 'accession') {
      setAccessionColumn(value);
    } else if (columnType === 'mrn') {
      setMrnColumn(value);
    } else {
      setDateColumn(value);
    }
  };

  const openColumnModal = (columnType: ColumnType) => {
    const currentValue =
      columnType === 'accession' ? accessionColumn : columnType === 'mrn' ? mrnColumn : dateColumn;
    setColumnModalInput(currentValue);
    setColumnModal(columnType);
  };

  const saveColumnValue = () => {
    const value = columnModalInput.trim();
    if (value && columnModal !== null) {
      setColumnValue(columnModal, value);
    }
    setColumnModal(null);
  };

  const openDateWindowModal = () => {
    setDateWindowInput(String(dateWindow) || '0');
    setDateWindowModalOpen(true);
  };

  const saveDateWindowValue = () => {
    setDateWindow(parseInt(dateWindowInput, 10) || 0);
    setDateWindowModalOpen(false);
  };

  const handleModalitySelection = (modality: string, checked: boolean) => {
    if (checked) {
      setCheckedModalities((prev) => [...prev, modality]);
      if (!initializedModalities.includes(modality)) {
        setInitializedModalities((prev) => [...prev, modality]);
        // Legacy fetched /load_settings/ again on first selection of each
        // modality and appended any saved filters for it. Note it read the
        // `filters` key (not `query_filters`) — quirk preserved.
        void (async () => {
          try {
            const settings = await loadSettings();
            const savedFilters =
              (settings.filters as SavedFilters | undefined)?.modality_filters?.[modality] ?? [];
            if (savedFilters.length > 0) {
              setModalityFilters((prev) => ({
                ...prev,
                [modality]: [
                  ...(prev[modality] ?? []),
                  ...savedFilters.map((filter) => ({
                    tag: filter.tag,
                    action: filter.action,
                    value: filter.value,
                  })),
                ],
              }));
            }
          } catch (error) {
            console.error('Error loading settings:', error);
          }
        })();
      }
    } else {
      // Legacy only hid the section; its filters are kept for re-checking.
      setCheckedModalities((prev) => prev.filter((m) => m !== modality));
    }
  };

  const runQuery = async () => {
    const settings = settingsRef.current;
    const filters = buildFiltersPayload(
      generalFilters,
      modalityFiltersEnabled,
      checkedModalities,
      modalityFilters,
    );
    const isAccessionQuery = queryType === 'accession';
    const data: Record<string, unknown> = {
      study_name: studyName,
      output_folder: outputFolder,
      input_file: inputFile,
      pacs_configs: settings.pacs_configs,
      application_aet: settings.application_aet,
      ...filters,
    };

    if (isAccessionQuery) {
      data.acc_col = accessionColumn;
      data.mrn_col = mrnColumn;
      data.date_col = '';
      data.date_window = 0;
      if (useFallback) {
        data.use_fallback_query = true;
        data.date_col = dateColumn;
        data.date_window = parseInt(String(dateWindow), 10) || 0;
      }
    } else {
      data.acc_col = '';
      data.mrn_col = mrnColumn;
      data.date_col = dateColumn;
      data.date_window = parseInt(String(dateWindow), 10) || 0;
    }
    console.log(data);
    try {
      const result = await postJson<RunResponse>('/run_query/', data);
      if (result.status === 'success') {
        // Redirect to progress page with project_id
        window.location.href = `/task_progress?project_id=${result.project_id}`;
      } else {
        console.error('Error starting query:', result);
        alert(result.message ?? 'Error starting query');
      }
    } catch (error) {
      if (error instanceof ApiError) {
        // The legacy page parsed error responses as JSON and alerted the
        // server-provided message; postJson surfaces that as an ApiError.
        console.error('Error starting query:', error);
        alert(error.message || 'Error starting query');
      } else {
        console.error('Error:', error);
        alert(`Error starting query: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1">Image Query</h1>
      </div>
      <div className="flex-1 bg-white p-4 ml-4 mt-4">
        <div className="text-sm text-black-5">Project Name</div>
        <input
          type="text"
          className="mt-2 w-full h-full border-2 border-gray-400 p-2"
          name="study_name"
          value={studyName}
          onChange={(event) => {
            setStudyName(event.target.value);
          }}
          // Legacy: onkeypress="return event.charCode !== 32" (block typed spaces).
          onKeyDown={(event) => {
            if (event.key === ' ') event.preventDefault();
          }}
        />
        <div className="text-sm text-gray-500 mt-4">Input File</div>
        <div className="flex items-center">
          <PathInput
            className="mt-2 w-full h-full border-2 border-gray-400 p-2"
            name="input_file"
            placeholder="Drag and drop query excel file"
            value={inputFile}
            onChange={setInputFile}
          />
        </div>
        <div className="flex items-center mt-4">
          <div className="text-sm text-gray-600">Query using:</div>
          <input
            className="ml-4"
            type="radio"
            id="query_accession"
            name="query_type"
            value="accession"
            checked={queryType === 'accession'}
            onChange={() => {
              setQueryType('accession');
            }}
          />
          <span className="text-sm ml-1">Accession</span>
          <input
            className="ml-4"
            type="radio"
            id="query_mrn_date"
            name="query_type"
            value="mrn_date"
            checked={queryType === 'mrn_date'}
            onChange={() => {
              setQueryType('mrn_date');
              // Legacy toggleQueryTypeInputs() unchecks the fallback box
              // whenever the MRN + Date method is selected.
              setUseFallback(false);
            }}
          />
          <span className="text-sm ml-1">MRN + Date</span>
        </div>
        <div id="fallback_query_option" className={fallbackOptionVisible ? 'mt-2' : 'mt-2 hidden'}>
          <label className="flex items-center cursor-pointer">
            <input
              type="checkbox"
              id="use_fallback_query"
              className="mr-2"
              checked={useFallback}
              onChange={(event) => {
                setUseFallback(event.target.checked);
              }}
            />
            <span className="text-sm text-gray-600">Enable MRN + Date fallback</span>
          </label>
          <div className="text-xs text-gray-400 ml-6">
            When accession query returns no results, retry using MRN + Study Date
          </div>
        </div>

        <div id="accession_display" className={accessionVisible ? 'mt-2' : 'mt-2 hidden'}>
          <div className="text-sm text-gray-500">
            Accession column: <span id="accession_column_display">{accessionColumn}</span>{' '}
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                openColumnModal('accession');
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>
          </div>
        </div>

        <div id="mrn_date_display" className={mrnDateVisible ? 'mt-2' : 'hidden mt-2'}>
          <div className="text-sm text-gray-500">
            MRN column: <span id="mrn_column_display">{mrnColumn}</span>{' '}
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                openColumnModal('mrn');
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>
            <span className="ml-6">
              Date column: <span id="date_column_display">{dateColumn}</span>
            </span>
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                openColumnModal('date');
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>
            <span className="ml-6">
              Date window (days): <span id="date_window_display">{dateWindow}</span>
            </span>
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                openDateWindowModal();
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>
          </div>
        </div>

        <div className="text-sm text-gray-500 mt-4">Output Folder</div>
        <PathInput
          className="mt-2 mb-2 w-full h-full border-2 border-gray-400 p-2"
          name="output_folder"
          value={outputFolder}
          onChange={setOutputFolder}
        />
        <div className="flex items-center mt-4">
          <button
            className="text-black"
            id="queryOptionsButton"
            onClick={() => {
              setOptionsOpen((open) => !open);
            }}
          >
            {optionsOpen ? '▲' : '▼'}
          </button>
          <div className="text-sm text-black ml-4">Query Options</div>
        </div>
        <div id="query_options" className={optionsOpen ? 'mt-4 ml-4' : 'mt-4 hidden ml-4'}>
          <div className="mb-4">
            <div className="text-sm text-gray-600 mb-2 mt-4">General Filters</div>
            <div className="text-sm text-gray-400 mb-2">Only include images where</div>
            <FilterList filters={generalFilters} onChange={setGeneralFilters} />
          </div>
          {/* Modality Filters Section */}
          <div className="mt-4">
            <div className="flex items-center">
              <input
                type="checkbox"
                id="modality-filter-toggle"
                className="mr-2"
                checked={modalityFiltersEnabled}
                onChange={(event) => {
                  setModalityFiltersEnabled(event.target.checked);
                }}
              />
              <label htmlFor="modality-filter-toggle">Filter by Modality</label>
            </div>

            {/* Modality options */}
            <div
              id="modality-options"
              className={modalityFiltersEnabled ? 'ml-4 mt-2' : 'ml-4 mt-2 hidden'}
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
                        checked={checkedModalities.includes(modality)}
                        onChange={(event) => {
                          handleModalitySelection(modality, event.target.checked);
                        }}
                      />
                      <label htmlFor={`modality-${modality}`}>{modality}</label>
                    </div>
                    <div
                      id={`filters-${modality}`}
                      className={
                        checkedModalities.includes(modality) ? 'ml-4 mt-2' : 'ml-4 mt-2 hidden'
                      }
                    >
                      {initializedModalities.includes(modality) && (
                        <FilterList
                          filters={modalityFilters[modality] ?? []}
                          onChange={(filters) => {
                            setModalityFilters((prev) => ({ ...prev, [modality]: filters }));
                          }}
                        />
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
            {/* Container for modality-specific filters (legacy vestige; nothing
                was ever appended to it on this page) */}
            <div id="modality-filters-container" className="ml-4 mt-2"></div>
          </div>
        </div>
      </div>
      <div className="m-4">
        <button
          id="pull_images_button"
          className={`mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100${
            validationActive && !formValid ? ' opacity-50 cursor-not-allowed' : ''
          }`}
          onClick={() => void runQuery()}
          disabled={!validationActive || !formValid}
        >
          Pull Images
        </button>
      </div>

      {/* Column Name Modal */}
      {columnModal !== null && (
        <div
          id="column-modal"
          className="fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center"
          style={{ zIndex: 1000 }}
        >
          <div className="bg-white p-6 rounded-lg shadow-lg w-96">
            <h3 className="text-lg font-medium mb-4" id="modal-title">
              {COLUMN_MODAL_TITLES[columnModal]}
            </h3>
            <div className="mb-4">
              <label className="text-sm text-gray-600 mb-2 block" id="modal-label">
                {COLUMN_MODAL_LABELS[columnModal]}
              </label>
              <input
                type="text"
                id="modal-input"
                className="w-full border-2 border-gray-400 p-2"
                autoFocus
                value={columnModalInput}
                onChange={(event) => {
                  setColumnModalInput(event.target.value);
                }}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    saveColumnValue();
                  } else if (event.key === 'Escape') {
                    setColumnModal(null);
                  }
                }}
              />
            </div>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => {
                  setColumnModal(null);
                }}
                className="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300"
              >
                Cancel
              </button>
              <button
                onClick={saveColumnValue}
                className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Date Window Modal */}
      {dateWindowModalOpen && (
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
                value={dateWindowInput}
                onChange={(event) => {
                  setDateWindowInput(event.target.value);
                }}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    saveDateWindowValue();
                  } else if (event.key === 'Escape') {
                    setDateWindowModalOpen(false);
                  }
                }}
              />
              <div className="text-xs text-gray-500 mt-1">
                Number of days before and after the study date to query
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => {
                  setDateWindowModalOpen(false);
                }}
                className="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300"
              >
                Cancel
              </button>
              <button
                onClick={saveDateWindowValue}
                className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
