import { useCallback, useEffect, useRef, useState } from 'react';

import { ApiError, getJson, postJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import type { RunResponse, Settings } from '../api/types';
import { buildFiltersPayload, FilterList, type Filter } from '../components/filters';
import { PathInput } from '../components/PathInput';
import { ScheduleInput } from '../components/ScheduleInput';
import { useConstants } from '../hooks/useConstants';

type QueryType = 'accession' | 'mrn_date';
type ColumnType = 'accession' | 'mrn' | 'date';

interface DeidFiltersSettings {
  general_filters?: Filter[];
  modality_filters?: Record<string, Filter[]>;
}

/** Extra free-form settings keys this page reads (legacy reads them straight
 * off the /load_settings/ JSON). */
interface ImageDeidExportSettings extends Settings {
  default_tags_to_keep?: string;
  default_tags_to_dateshift?: string;
  default_tags_to_randomize?: string;
  mapping_file_path?: string;
  use_mapping_file?: boolean;
  default_deid_pixels?: boolean;
  default_remove_unspecified?: boolean;
  default_remove_overlays?: boolean;
  default_remove_curves?: boolean;
  default_remove_private?: boolean;
  default_apply_default_ctp_filter_script?: boolean;
  deid_engine?: string;
  deid_filters?: DeidFiltersSettings;
  date_shift_range?: number;
  site_id?: string;
}

interface AdminSettings {
  imagine_sas_url?: string;
  [key: string]: unknown;
}

interface ValidateSasUrlResponse {
  valid: boolean;
  error?: string;
}

async function validateSasUrlOnLoad(sasUrl: string): Promise<ValidateSasUrlResponse> {
  try {
    return await postJson<ValidateSasUrlResponse>('/validate_sas_url/', { sas_url: sasUrl });
  } catch (error) {
    console.error('Error validating SAS URL:', error);
    return { valid: false, error: 'Error validating SAS URL' };
  }
}

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
function ColumnNameModal({ columnType, initialValue, onSave, onClose }: ColumnNameModalProps) {
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
function DateWindowModal({ initialValue, onSave, onClose }: DateWindowModalProps) {
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

export function ImageDeidExport() {
  const constants = useConstants();
  // This page's server context used the export modality list (CT first),
  // not the query-page ordering in constants.modalities.
  const modalities = constants?.export_modalities ?? [];

  // The legacy page kept the whole /load_settings/ and /load_admin_settings/
  // responses in page-global `settings` / `adminSettings` objects and read
  // date_shift_range / site_id / pacs_configs / application_aet /
  // mapping_file_path / imagine_sas_url from them at submit time.
  const [settings, setSettings] = useState<ImageDeidExportSettings>({});
  const [adminSettings, setAdminSettings] = useState<AdminSettings>({});

  const [showSasUrlRequiredModal, setShowSasUrlRequiredModal] = useState(false);
  // Mirrors the legacy page: the button starts disabled (without the dimmed
  // classes) and form validation only activates after the SAS URL check
  // passes; if the check fails the button stays permanently disabled.
  const [validationActive, setValidationActive] = useState(false);

  const [studyName, setStudyName] = useState('');
  const [inputFile, setInputFile] = useState('');
  const [outputFolder, setOutputFolder] = useState('');

  const [queryType, setQueryType] = useState<QueryType>('accession');
  const [useFallbackQuery, setUseFallbackQuery] = useState(false);
  const [accessionColumn, setAccessionColumn] = useState('AccessionNumber');
  const [mrnColumn, setMrnColumn] = useState('PatientID');
  const [dateColumn, setDateColumn] = useState('StudyDate');
  const [dateWindow, setDateWindow] = useState('0');

  const [deidOptionsOpen, setDeidOptionsOpen] = useState(false);

  const [generalFilters, setGeneralFilters] = useState<Filter[]>([]);
  const [modalityFilterEnabled, setModalityFilterEnabled] = useState(false);
  const [checkedModalities, setCheckedModalities] = useState<Record<string, boolean>>({});
  // Legacy created each modality's filter UI on first check and never tore it
  // down; unchecking only hides it. Initialized sections keep their filters.
  const [initializedModalities, setInitializedModalities] = useState<Record<string, boolean>>(
    {},
  );
  const initializedModalitiesRef = useRef(new Set<string>());
  const [modalityFilters, setModalityFilters] = useState<Record<string, Filter[]>>({});

  const [tagsToKeep, setTagsToKeep] = useState('');
  const [tagsToDateshift, setTagsToDateshift] = useState('');
  const [tagsToRandomize, setTagsToRandomize] = useState('');
  const [removeUnspecified, setRemoveUnspecified] = useState(false);
  const [removeOverlays, setRemoveOverlays] = useState(false);
  const [removeCurves, setRemoveCurves] = useState(false);
  const [removePrivate, setRemovePrivate] = useState(false);
  const [applyDefaultCtpFilterScript, setApplyDefaultCtpFilterScript] = useState(true);
  const [deidPixels, setDeidPixels] = useState(false);
  const [deidEngineRust, setDeidEngineRust] = useState(false);
  const [useMappingFile, setUseMappingFile] = useState(false);
  const [mappingFilePath, setMappingFilePath] = useState('');
  const [scPdfDestination, setScPdfDestination] = useState<'quarantine' | 'custom'>(
    'quarantine',
  );
  const [scPdfOutputDir, setScPdfOutputDir] = useState('');

  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduledTime, setScheduledTime] = useState('');

  const [columnModal, setColumnModal] = useState<ColumnType | null>(null);
  const [dateWindowModalOpen, setDateWindowModalOpen] = useState(false);

  // Legacy loadFiltersFromSettings(): replaces the general filter rows, and
  // replaces filters only for modalities whose containers already exist
  // (i.e. modalities the user has checked at least once). Saved filters for
  // not-yet-initialized modalities are silently dropped, exactly as before.
  const applyFiltersFromSettings = useCallback((filters: DeidFiltersSettings) => {
    setGeneralFilters(filters.general_filters ?? []);
    const modalityFiltersFromSettings = filters.modality_filters;
    if (modalityFiltersFromSettings) {
      setModalityFilters((prev) => {
        const next = { ...prev };
        Object.entries(modalityFiltersFromSettings).forEach(([modality, list]) => {
          if (initializedModalitiesRef.current.has(modality)) {
            next[modality] = list;
          }
        });
        return next;
      });
    }
  }, []);

  // Pre-fill from saved settings, as the legacy DOMContentLoaded handler did.
  // Legacy returned early (before any pre-fill, leaving the form blank and
  // the button disabled) when the SAS URL was missing or failed validation.
  useEffect(() => {
    void (async () => {
      try {
        const loaded = (await loadSettings()) as ImageDeidExportSettings;
        setSettings(loaded);

        const loadedAdmin = await getJson<AdminSettings>('/load_admin_settings/');
        setAdminSettings(loadedAdmin);

        const sasUrl = loadedAdmin.imagine_sas_url;
        if (!sasUrl || sasUrl.trim() === '') {
          setShowSasUrlRequiredModal(true);
          return;
        } else {
          const result = await validateSasUrlOnLoad(sasUrl);
          if (!result.valid) {
            setShowSasUrlRequiredModal(true);
            return;
          }
        }

        // Legacy quirk preserved: anything other than the literal string
        // "Accession" (including a missing setting) selects MRN + Date.
        setQueryType(loaded.default_query_method === 'Accession' ? 'accession' : 'mrn_date');
        setAccessionColumn(loaded.default_accession_header || 'AccessionNumber');
        setMrnColumn(loaded.default_mrn_header || 'PatientID');
        setDateColumn(loaded.default_date_header || 'StudyDate');
        setDateWindow(
          loaded.default_date_window_days !== undefined
            ? String(loaded.default_date_window_days)
            : '0',
        );
        if (loaded.default_output_folder) {
          setOutputFolder(loaded.default_output_folder);
        }

        if (loaded.default_tags_to_keep) {
          setTagsToKeep(loaded.default_tags_to_keep);
        }
        if (loaded.default_tags_to_dateshift) {
          setTagsToDateshift(loaded.default_tags_to_dateshift);
        }
        if (loaded.default_tags_to_randomize) {
          setTagsToRandomize(loaded.default_tags_to_randomize);
        }
        if (loaded.mapping_file_path) {
          setMappingFilePath(loaded.mapping_file_path);
        }
        if (loaded.use_mapping_file) {
          setUseMappingFile(Boolean(loaded.use_mapping_file));
        }
        if (loaded.default_deid_pixels) {
          setDeidPixels(Boolean(loaded.default_deid_pixels));
        }
        if (loaded.default_remove_unspecified !== undefined) {
          setRemoveUnspecified(Boolean(loaded.default_remove_unspecified));
        }
        if (loaded.default_remove_overlays !== undefined) {
          setRemoveOverlays(Boolean(loaded.default_remove_overlays));
        }
        if (loaded.default_remove_curves !== undefined) {
          setRemoveCurves(Boolean(loaded.default_remove_curves));
        }
        if (loaded.default_remove_private !== undefined) {
          setRemovePrivate(Boolean(loaded.default_remove_private));
        }
        if (loaded.default_apply_default_ctp_filter_script !== undefined) {
          setApplyDefaultCtpFilterScript(
            Boolean(loaded.default_apply_default_ctp_filter_script),
          );
        }
        if (loaded.deid_engine !== undefined) {
          setDeidEngineRust(loaded.deid_engine === 'rust');
        }
        if (loaded.deid_filters) {
          applyFiltersFromSettings(loaded.deid_filters);
        }

        setValidationActive(true);
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, [applyFiltersFromSettings]);

  // Legacy handleModalitySelection(): the first time a modality is checked,
  // its filter UI is created and that modality's saved filters are loaded via
  // a fresh /load_settings/ fetch. Re-checking later just unhides the section.
  const handleModalitySelection = (modality: string, checked: boolean) => {
    setCheckedModalities((prev) => ({ ...prev, [modality]: checked }));
    if (checked && !initializedModalitiesRef.current.has(modality)) {
      initializedModalitiesRef.current.add(modality);
      setInitializedModalities((prev) => ({ ...prev, [modality]: true }));
      void loadSettings()
        .then((loaded) => {
          const saved =
            (loaded as ImageDeidExportSettings).deid_filters?.modality_filters?.[modality] ??
            [];
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

  // Legacy validateForm(): project name plus input file.
  const formValid = Boolean(studyName.trim() && inputFile.trim());

  const mrnDateVisible = queryType === 'mrn_date' || useFallbackQuery;

  // Legacy updateButtonText().
  const buttonText = scheduleEnabled
    ? 'Schedule Pull Images, Deidentify and Export'
    : 'Pull Images, Deidentify and Export';

  const runImageDeidExport = async () => {
    const filters = buildFiltersPayload(
      generalFilters,
      modalityFilterEnabled,
      modalities.filter((modality) => checkedModalities[modality]),
      modalityFilters,
    );
    const isAccessionQuery = queryType === 'accession';
    const isScheduled = scheduleEnabled;

    const scPdfCustom = scPdfDestination === 'custom';
    const scPdfPath = scPdfOutputDir;

    const data: Record<string, unknown> = {
      study_name: studyName,
      input_file: inputFile,
      output_folder: outputFolder,
      tags_to_keep: tagsToKeep,
      tags_to_dateshift: tagsToDateshift,
      tags_to_randomize: tagsToRandomize,
      mapping_file_path: mappingFilePath || settings.mapping_file_path || '',
      use_mapping_file: useMappingFile,
      deid_pixels: deidPixels,
      remove_unspecified: removeUnspecified,
      remove_overlays: removeOverlays,
      remove_curves: removeCurves,
      remove_private: removePrivate,
      apply_default_ctp_filter_script: applyDefaultCtpFilterScript,
      deid_engine: deidEngineRust ? 'rust' : 'ctp',
      date_shift_days: settings.date_shift_range || 0,
      site_id: settings.site_id || '',
      pacs_configs: settings.pacs_configs || [],
      application_aet: settings.application_aet || '',
      sas_url: adminSettings.imagine_sas_url || '',
      sc_pdf_output_dir: scPdfCustom && scPdfPath ? scPdfPath : '',
      ...filters,
    };

    if (!inputFile.endsWith('.xlsx')) {
      alert('Input file must be an Excel file.');
      return;
    }

    if (isScheduled) {
      if (!scheduledTime) {
        alert('Please select a scheduled time');
        return;
      }
      data.scheduled_time = scheduledTime;
    }

    if (isAccessionQuery) {
      data.acc_col = accessionColumn;
      data.mrn_col = mrnColumn;
      data.date_col = '';
      data.date_window = 0;
      if (useFallbackQuery) {
        data.use_fallback_query = true;
        data.date_col = dateColumn;
        data.date_window = parseInt(dateWindow) || 0;
      }
    } else {
      data.acc_col = '';
      data.mrn_col = mrnColumn;
      data.date_col = dateColumn;
      data.date_window = parseInt(dateWindow) || 0;
    }

    try {
      const result = await postJson<RunResponse>('/run_imagedeidexport/', data);
      if (result.status === 'success') {
        if (isScheduled) {
          window.location.href = '/task_list';
        } else {
          window.location.href = `/task_progress?project_id=${result.project_id}`;
        }
      } else {
        alert(result.message || 'Error starting deidentification and export');
        console.error('Error:', result);
      }
    } catch (error) {
      if (error instanceof ApiError) {
        // Legacy parsed error responses as JSON and alerted the
        // server-provided message; postJson surfaces that as an ApiError.
        alert(error.message || 'Error starting deidentification and export');
        console.error('Error:', error);
      } else {
        alert(
          `Error starting deidentification and export: ${error instanceof Error ? error.message : String(error)}`,
        );
        console.error('Error:', error);
      }
    }
  };

  return (
    <>
      {/* modals/sas_url_required.html */}
      {showSasUrlRequiredModal && (
        <div
          id="sasUrlRequiredModal"
          className="fixed inset-0 bg-gray-600 bg-opacity-75 overflow-y-auto h-full w-full z-50"
        >
          <div className="relative top-20 mx-auto p-6 border w-[600px] shadow-lg rounded-md bg-white">
            <div className="mt-3">
              <div className="flex items-center justify-center mb-4">
                <div className="flex items-center justify-center h-12 w-12 rounded-full bg-red-100">
                  <svg
                    className="h-6 w-6 text-red-600"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth="2"
                      d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                    />
                  </svg>
                </div>
              </div>
              <h3 className="text-lg leading-6 font-medium text-gray-900 text-center mb-2">
                SAS URL Required
              </h3>
              <div className="mt-2 px-7 py-3">
                <p className="text-sm text-gray-700 text-center mb-4">
                  SAS URL must be configured in Admin Settings before using this module.
                </p>
                <p className="text-sm text-gray-600 text-center">
                  Please contact your administrator or navigate to Admin Settings to configure
                  the Azure Blob Storage SAS URL for data transfer to IMAGINE.
                </p>
              </div>
              <div className="items-center px-4 py-3 flex justify-center gap-2">
                <button
                  onClick={() => {
                    setShowSasUrlRequiredModal(false);
                  }}
                  className="px-6 py-2 bg-gray-300 text-gray-700 text-base font-medium rounded-md shadow-sm hover:bg-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-300"
                >
                  Cancel
                </button>
                <button
                  onClick={() => {
                    window.location.href = '/settings/admin/';
                  }}
                  className="px-6 py-2 bg-blue-500 text-white text-base font-medium rounded-md shadow-sm hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
                >
                  Go to Admin Settings
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      <div className="px-4">
        <h1 className="text-xl flex-1">Image Deidentification and Export</h1>
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

        <div className="text-sm text-gray-500 mt-4">Output Folder</div>
        <PathInput
          className="mt-2 mb-2 w-full h-full border-2 border-gray-400 p-2"
          name="output_folder"
          placeholder="Folder to store queried images in (images are preserved locally after export)"
          value={outputFolder}
          onChange={setOutputFolder}
        />

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
              // Legacy toggleQueryTypeInputs() unchecks the fallback
              // checkbox when switching to MRN + Date.
              setQueryType('mrn_date');
              setUseFallbackQuery(false);
            }}
          />
          <span className="text-sm ml-1">MRN + Date</span>
        </div>
        <div
          id="fallback_query_option"
          className={queryType === 'accession' ? 'mt-2' : 'mt-2 hidden'}
        >
          <label className="flex items-center cursor-pointer">
            <input
              type="checkbox"
              id="use_fallback_query"
              className="mr-2"
              checked={useFallbackQuery}
              onChange={(event) => {
                setUseFallbackQuery(event.target.checked);
              }}
            />
            <span className="text-sm text-gray-600">Enable MRN + Date fallback</span>
          </label>
          <div className="text-xs text-gray-400 ml-6">
            When accession query returns no results, retry using MRN + Study Date
          </div>
        </div>

        <div
          id="accession_display"
          className={queryType === 'accession' ? 'mt-2' : 'mt-2 hidden'}
        >
          <div className="text-sm text-gray-500">
            Accession column: <span id="accession_column_display">{accessionColumn}</span>{' '}
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                setColumnModal('accession');
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
                setColumnModal('mrn');
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>{' '}
            <span className="ml-6">
              Date column: <span id="date_column_display">{dateColumn}</span>
            </span>{' '}
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                setColumnModal('date');
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>{' '}
            <span className="ml-6">
              Date window (days): <span id="date_window_display">{dateWindow}</span>
            </span>{' '}
            <a
              href="#"
              onClick={(event) => {
                event.preventDefault();
                setDateWindowModalOpen(true);
              }}
              className="text-blue-600 hover:text-blue-800 ml-2"
            >
              (Change)
            </a>
          </div>
        </div>

        <div className="flex items-center mt-4">
          <button
            className="text-black"
            id="deidOptionsButton"
            onClick={() => {
              setDeidOptionsOpen((open) => !open);
            }}
          >
            {deidOptionsOpen ? '▲' : '▼'}
          </button>
          <div className="text-sm text-black ml-4">Deidentification Options</div>
        </div>
        <div id="deid_options" className={deidOptionsOpen ? 'mt-4 ml-4' : 'mt-4 hidden ml-4'}>
          <div className="mb-4">
            <div className="text-sm text-gray-600 mb-2">General Filters</div>
            <div className="text-sm text-gray-400 mb-2">Only include images where</div>
            <FilterList filters={generalFilters} onChange={setGeneralFilters} />
          </div>

          <div className="mt-4">
            <div className="flex items-center">
              <input
                type="checkbox"
                id="modality-filter-toggle"
                className="mr-2"
                checked={modalityFilterEnabled}
                onChange={(event) => {
                  // Legacy toggleModalityFilters() also cleared the (always
                  // empty) #modality-filters-container; per-modality filter
                  // state is intentionally preserved across toggles.
                  setModalityFilterEnabled(event.target.checked);
                }}
              />
              <label htmlFor="modality-filter-toggle">Filter by Modality</label>
            </div>

            <div
              id="modality-options"
              className={modalityFilterEnabled ? 'ml-4 mt-2' : 'ml-4 mt-2 hidden'}
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
                        checked={checkedModalities[modality] ?? false}
                        onChange={(event) => {
                          handleModalitySelection(modality, event.target.checked);
                        }}
                      />
                      <label htmlFor={`modality-${modality}`}>{modality}</label>
                    </div>
                    <div
                      id={`filters-${modality}`}
                      className={checkedModalities[modality] ? 'ml-4 mt-2' : 'ml-4 mt-2 hidden'}
                    >
                      {initializedModalities[modality] && (
                        <FilterList
                          filters={modalityFilters[modality] ?? []}
                          onChange={(filters) => {
                            setModalityFilters((prev) => ({
                              ...prev,
                              [modality]: filters,
                            }));
                          }}
                        />
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

          <div className="mt-6">
            <div className="text-md mb-2">Advanced Deidentification Options</div>
            <div className="flex space-x-4">
              <div className="flex-1">
                <div className="text-sm text-gray-500">To keep</div>
                <textarea
                  className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
                  name="tags_to_keep"
                  value={tagsToKeep}
                  onChange={(event) => {
                    setTagsToKeep(event.target.value);
                  }}
                ></textarea>
              </div>
              <div className="flex-1">
                <div className="text-sm text-gray-500">To date shift</div>
                <textarea
                  className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
                  name="tags_to_dateshift"
                  value={tagsToDateshift}
                  onChange={(event) => {
                    setTagsToDateshift(event.target.value);
                  }}
                ></textarea>
              </div>
              <div className="flex-1">
                <div className="text-sm text-gray-500">To randomize</div>
                <textarea
                  className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
                  name="tags_to_randomize"
                  value={tagsToRandomize}
                  onChange={(event) => {
                    setTagsToRandomize(event.target.value);
                  }}
                ></textarea>
              </div>
            </div>
          </div>
          <div className="mt-8">
            <div className="text-md mb-4">Global Deidentification Settings</div>
            <div className="space-y-4">
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="remove_unspecified"
                    name="remove_unspecified"
                    className="mr-2"
                    checked={removeUnspecified}
                    onChange={(event) => {
                      setRemoveUnspecified(event.target.checked);
                    }}
                  />
                  <label htmlFor="remove_unspecified">Remove unspecified tags</label>
                </div>
                <div className="text-sm text-gray-500 ml-6">
                  If checked, any tag not specified in the lists above will be removed. If
                  unchecked, unspecified tags will be preserved
                </div>
              </div>
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="remove_overlays"
                    name="remove_overlays"
                    className="mr-2"
                    checked={removeOverlays}
                    onChange={(event) => {
                      setRemoveOverlays(event.target.checked);
                    }}
                  />
                  <label htmlFor="remove_overlays">Remove overlays</label>
                </div>
                <div className="text-sm text-gray-500 ml-6">
                  Remove all elements in 60xx groups. These are overlays and are sometimes
                  removed when fully de-identifying an object because they can contain PHI.
                </div>
              </div>
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="remove_curves"
                    name="remove_curves"
                    className="mr-2"
                    checked={removeCurves}
                    onChange={(event) => {
                      setRemoveCurves(event.target.checked);
                    }}
                  />
                  <label htmlFor="remove_curves">Remove curve data</label>
                </div>
                <div className="text-sm text-gray-500 ml-6">
                  Remove all elements in 50xx groups. These are groups which contain curve data.
                </div>
              </div>
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="remove_private"
                    name="remove_private"
                    className="mr-2"
                    checked={removePrivate}
                    onChange={(event) => {
                      setRemovePrivate(event.target.checked);
                    }}
                  />
                  <label htmlFor="remove_private">Remove all private tags</label>
                </div>
                <div className="text-sm text-gray-500 ml-6">
                  Remove all elements in odd-numbered groups. These are private groups whose
                  contents are not specified by the DICOM standard
                </div>
              </div>
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="apply_default_ctp_filter_script"
                    name="apply_default_ctp_filter_script"
                    className="mr-2"
                    checked={applyDefaultCtpFilterScript}
                    onChange={(event) => {
                      setApplyDefaultCtpFilterScript(event.target.checked);
                    }}
                  />
                  <label htmlFor="apply_default_ctp_filter_script">
                    Apply default filters from known devices
                  </label>
                </div>
                <div className="text-sm text-gray-500 ml-6">
                  Remove DICOM files during the deidentification process from known devices that
                  are known to contain PHI. These files will be stored in the quarantine.
                </div>
              </div>
            </div>
          </div>
          <div className="mt-8">
            <div className="text-md mb-4">Pixel Deidentification</div>
            <div className="mb-4">
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="deid_pixels"
                  name="deid_pixels"
                  className="mr-2"
                  checked={deidPixels}
                  onChange={(event) => {
                    setDeidPixels(event.target.checked);
                  }}
                />
                <label htmlFor="deid_pixels">Deidentify pixel data from device scanners</label>
              </div>
              <div className="text-sm text-gray-500 ml-6">
                Blacks out regions of pixels in a DICOM image from known devices.
              </div>
            </div>
            <div className="text-md mb-4 mt-8">Deidentification Engine</div>
            <div className="mb-4">
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="deid_engine_rust"
                  name="deid_engine_rust"
                  className="mr-2"
                  checked={deidEngineRust}
                  onChange={(event) => {
                    setDeidEngineRust(event.target.checked);
                  }}
                />
                <label htmlFor="deid_engine_rust">Use experimental Rust engine</label>
              </div>
              <div className="text-sm text-gray-500 ml-6">
                Use dicom-deid-rs instead of CTP for deidentification. This engine is faster but
                is still under active development.
              </div>
            </div>
            <div className="text-md mb-4 mt-8">Mapping File Configuration</div>
            <div>
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="use_mapping_file"
                  name="use_mapping_file"
                  className="mr-2"
                  checked={useMappingFile}
                  onChange={(event) => {
                    setUseMappingFile(event.target.checked);
                  }}
                />
                <label htmlFor="use_mapping_file">Use Mapping File</label>
              </div>
              <div
                id="mapping_file_input_container"
                className={useMappingFile ? 'mt-2' : 'mt-2 hidden'}
              >
                <PathInput
                  className="w-full h-full border-2 border-gray-400 p-2"
                  name="mapping_file_path"
                  value={mappingFilePath}
                  onChange={setMappingFilePath}
                />
                <div className="text-sm text-gray-500 mt-1">
                  Excel file with columns: TagName, New-TagName (e.g., AccessionNumber,
                  New-AccessionNumber)
                </div>
              </div>
            </div>
            <div className="text-md mb-4 mt-8">Secondary Capture and PDF Handling</div>
            <div>
              <div className="text-sm text-gray-500 mb-2">
                Secondary Capture images, PDFs, and other embedded content files contain
                unredactable PHI and will be filtered from de-identification output.
              </div>
              <div className="ml-2">
                <div className="flex items-center mb-2">
                  <input
                    type="radio"
                    id="sc_pdf_to_quarantine"
                    name="sc_pdf_destination"
                    value="quarantine"
                    checked={scPdfDestination === 'quarantine'}
                    onChange={() => {
                      setScPdfDestination('quarantine');
                    }}
                  />
                  <label htmlFor="sc_pdf_to_quarantine" className="ml-2">
                    Send to quarantine folder
                  </label>
                </div>
                <div className="flex items-center mb-2">
                  <input
                    type="radio"
                    id="sc_pdf_to_custom"
                    name="sc_pdf_destination"
                    value="custom"
                    checked={scPdfDestination === 'custom'}
                    onChange={() => {
                      setScPdfDestination('custom');
                    }}
                  />
                  <label htmlFor="sc_pdf_to_custom" className="ml-2">
                    Send to custom location
                  </label>
                </div>
                <div
                  id="sc_pdf_custom_path_container"
                  className={scPdfDestination === 'custom' ? 'mt-2 ml-6' : 'hidden mt-2 ml-6'}
                >
                  <PathInput
                    className="w-full border-2 border-gray-400 p-2"
                    name="sc_pdf_output_dir"
                    placeholder="Drag and drop folder or enter path"
                    value={scPdfOutputDir}
                    onChange={setScPdfOutputDir}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div className="m-4">
        <ScheduleInput
          enabled={scheduleEnabled}
          onEnabledChange={setScheduleEnabled}
          scheduledTime={scheduledTime}
          onScheduledTimeChange={setScheduledTime}
        />

        <button
          className={`mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100${
            validationActive && !formValid ? ' opacity-50 cursor-not-allowed' : ''
          }`}
          onClick={() => void runImageDeidExport()}
          id="deidexport_button"
          disabled={!validationActive || !formValid}
        >
          {buttonText}
        </button>
      </div>

      {/* Column Name Modal */}
      {columnModal && (
        <ColumnNameModal
          columnType={columnModal}
          initialValue={
            columnModal === 'accession'
              ? accessionColumn
              : columnModal === 'mrn'
                ? mrnColumn
                : dateColumn
          }
          onSave={(value) => {
            if (columnModal === 'accession') {
              setAccessionColumn(value);
            } else if (columnModal === 'mrn') {
              setMrnColumn(value);
            } else {
              setDateColumn(value);
            }
          }}
          onClose={() => {
            setColumnModal(null);
          }}
        />
      )}

      {/* Date Window Modal */}
      {dateWindowModalOpen && (
        <DateWindowModal
          initialValue={dateWindow || '0'}
          onSave={(value) => {
            setDateWindow(String(value));
          }}
          onClose={() => {
            setDateWindowModalOpen(false);
          }}
        />
      )}
    </>
  );
}
