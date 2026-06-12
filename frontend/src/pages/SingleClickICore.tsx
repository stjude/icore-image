import { useEffect, useState } from 'react';

import { getJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import { submitRun, type JobRequest } from '../api/run';
import type { Settings } from '../api/types';
import { ColumnActions, type ColumnActionsState } from '../components/ColumnActions';
import { FilterList, ModalityFilterSection, useFilters, type SavedFilters } from '../components/filters';
import { PathInput } from '../components/PathInput';
import { SasUrlRequiredModal, validateSasUrlOnLoad } from '../components/SasUrlRequiredModal';
import { ScheduleInput } from '../components/ScheduleInput';
import { useConstants } from '../hooks/useConstants';

/** Extra free-form settings keys this page reads (legacy reads them straight
 * off the /load_settings/ JSON). */
interface SingleClickSettings extends Settings {
  mapping_file_path?: string;
  use_mapping_file?: boolean;
  deid_filters?: SavedFilters;
  default_text_to_keep?: string;
  default_text_to_remove?: string;
}

interface AdminSettings {
  imagine_sas_url?: string;
  [key: string]: unknown;
}

/** The green check icon repeated next to each enforced HIPAA setting. */
function EnforcedCheckIcon() {
  return (
    <svg className="h-5 w-5 text-green-600 mr-2" fill="currentColor" viewBox="0 0 20 20">
      <path
        fillRule="evenodd"
        d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
        clipRule="evenodd"
      />
    </svg>
  );
}

export function SingleClickICore() {
  const constants = useConstants();
  // This page's modality list is the export subset, not the full deid list.
  const modalities = constants?.export_modalities ?? [];
  const tagsToKeep = constants?.hipaa.tags_to_keep ?? [];
  const tagsToDateshift = constants?.hipaa.tags_to_dateshift ?? [];
  const tagsToRandomize = constants?.hipaa.tags_to_randomize ?? [];

  // The legacy page kept the whole /load_settings/ and /load_admin_settings/
  // responses in page globals and read pacs_configs / application_aet /
  // mapping_file_path / imagine_sas_url from them at submit time.
  const [settings, setSettings] = useState<SingleClickSettings>({});
  const [adminSettings, setAdminSettings] = useState<AdminSettings>({});

  const [studyName, setStudyName] = useState('');
  const [inputFile, setInputFile] = useState('');
  const [outputFolder, setOutputFolder] = useState('');

  const [deidOptionsOpen, setDeidOptionsOpen] = useState(false);

  const filters = useFilters('deid_filters');
  const { applyFromSettings } = filters;

  const [useMappingFile, setUseMappingFile] = useState(false);
  const [mappingFilePath, setMappingFilePath] = useState('');
  const [scPdfDestination, setScPdfDestination] = useState<'quarantine' | 'custom'>(
    'quarantine',
  );
  const [scPdfOutputDir, setScPdfOutputDir] = useState('');

  const [columnState, setColumnState] = useState<ColumnActionsState>({
    actions: {},
    allAssigned: false,
  });
  const [textToKeep, setTextToKeep] = useState('');
  const [textToRemove, setTextToRemove] = useState('');

  const [exportToAzure, setExportToAzure] = useState(true);
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduledTime, setScheduledTime] = useState('');

  const [showSasUrlRequiredModal, setShowSasUrlRequiredModal] = useState(false);

  // Pre-fill from saved settings, as the legacy DOMContentLoaded handler did
  // (user settings, then admin settings for the IMAGINE SAS URL).
  useEffect(() => {
    void (async () => {
      try {
        const loaded = (await loadSettings()) as SingleClickSettings;
        setSettings(loaded);

        const loadedAdmin = await getJson<AdminSettings>('/load_admin_settings/');
        setAdminSettings(loadedAdmin);

        if (loaded.default_output_folder) {
          setOutputFolder(loaded.default_output_folder);
        }

        if (loaded.mapping_file_path) {
          setMappingFilePath(loaded.mapping_file_path);
        }
        if (loaded.use_mapping_file) {
          setUseMappingFile(Boolean(loaded.use_mapping_file));
        }

        if (loaded.deid_filters) {
          applyFromSettings(loaded.deid_filters);
        }

        // Text deid defaults
        if (loaded.default_text_to_keep) {
          setTextToKeep(loaded.default_text_to_keep);
        }
        if (loaded.default_text_to_remove) {
          setTextToRemove(loaded.default_text_to_remove);
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once on mount, as the legacy DOMContentLoaded handler did
  }, []);

  // Legacy validateForm(): project name, input file, and every spreadsheet
  // column assigned an action (allColumnsAssigned()).
  const formValid = Boolean(studyName.trim() && inputFile.trim() && columnState.allAssigned);

  // Legacy updateButtonText().
  const buttonText = scheduleEnabled
    ? 'Schedule Pull, Deidentify, and Export'
    : 'Pull Images, Deidentify, and Export';

  const runSingleClickICore = async () => {
    if (exportToAzure) {
      const sasUrl = adminSettings.imagine_sas_url;
      if (!sasUrl || sasUrl.trim() === '') {
        setShowSasUrlRequiredModal(true);
        return;
      }
      const sasValidation = await validateSasUrlOnLoad(sasUrl);
      if (!sasValidation.valid) {
        setShowSasUrlRequiredModal(true);
        return;
      }
    }

    const data: JobRequest<'/run_singleclickicore/'> = {
      study_name: studyName,
      input_file: inputFile,
      output_folder: outputFolder,
      // Image deid options
      mapping_file_path: mappingFilePath || settings.mapping_file_path || '',
      use_mapping_file: useMappingFile,

      pacs_configs: settings.pacs_configs || [],
      application_aet: settings.application_aet || '',
      sas_url: adminSettings.imagine_sas_url || '',
      sc_pdf_output_dir: scPdfDestination === 'custom' && scPdfOutputDir ? scPdfOutputDir : '',
      // Text deid options
      text_to_keep: textToKeep,
      text_to_remove: textToRemove,
      column_actions: columnState.actions,
      // Export preference
      export_to_azure: exportToAzure,
      ...filters.payload(modalities),
    };

    if (!inputFile.endsWith('.xlsx')) {
      alert('Input file must be an Excel file.');
      return;
    }

    if (scheduleEnabled) {
      if (!scheduledTime) {
        alert('Please select a scheduled time');
        return;
      }
      data.scheduled_time = scheduledTime;
    }

    await submitRun('/run_singleclickicore/', data, {
      scheduled: scheduleEnabled,
      errorPrefix: 'Error starting Single-Click iCore',
    });
  };

  return (
    <>
      {showSasUrlRequiredModal && (
        <SasUrlRequiredModal
          onClose={() => {
            setShowSasUrlRequiredModal(false);
          }}
        />
      )}
      <div className="px-4">
        <h1 className="text-xl flex-1">Single-Click iCore</h1>
      </div>

      {/* HIPAA Safe Harbor Banner */}
      <div className="mx-4 mt-4 bg-blue-50 border-l-4 border-blue-500 p-4">
        <div className="flex items-start">
          <div className="flex-shrink-0">
            <svg
              className="h-6 w-6 text-blue-500"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="2"
                d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-md font-semibold text-blue-900">
              HIPAA Safe Harbor Mode (Automatic)
            </h3>
            <div className="mt-2 text-sm text-blue-800">
              <p className="font-medium">
                Single-click iCore automatically enforces HIPAA Safe Harbor de-identification
                per 45 CFR §164.514(b)(2).
              </p>
              <p className="mt-2">
                All 18 HIPAA identifiers will be removed or anonymized. Files containing
                encapsulated documents, PDFs, or burned-in annotations will be automatically
                quarantined for manual review.
              </p>
              <p className="mt-2">
                <strong>These settings are NOT user-configurable.</strong> For custom
                de-identification settings, use the Local De-identification workflow.
              </p>
            </div>
          </div>
        </div>
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
            placeholder="Drag and drop Excel file (from ptimorial/mPower)"
            value={inputFile}
            onChange={setInputFile}
          />
        </div>

        <div className="text-sm text-gray-500 mt-4">Output Folder</div>
        <PathInput
          className="mt-2 mb-2 w-full h-full border-2 border-gray-400 p-2"
          name="output_folder"
          placeholder="Folder to store deidentified images and Excel (preserved locally after export)"
          value={outputFolder}
          onChange={setOutputFolder}
        />

        {/* Combined Deidentification Options */}
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
          {/* Image Deidentification Sub-section */}
          <div className="mb-6">
            <div className="text-md font-medium mb-2">Image Deidentification</div>
            <div className="mb-4">
              <div className="text-sm text-gray-600 mb-2">General Filters</div>
              <div className="text-sm text-gray-400 mb-2">Only include images where</div>
              <FilterList filters={filters.generalFilters} onChange={filters.setGeneralFilters} />
            </div>

            <ModalityFilterSection modalities={modalities} filters={filters} />

            <div className="mt-6 bg-gray-50 border border-gray-300 rounded p-4">
              <div className="text-md mb-3 font-medium text-gray-900">
                Advanced Deidentification Options
              </div>
              <div className="text-sm text-gray-700 mb-4">
                The following HIPAA Safe Harbor settings are automatically enforced and cannot
                be modified:
              </div>

              <div className="flex space-x-4 mb-4">
                <div className="flex-1 bg-white p-3 rounded border border-gray-200">
                  <div className="text-sm font-medium text-gray-700 mb-2">To keep</div>
                  <div className="text-xs text-gray-600 space-y-1 max-h-32 overflow-y-auto">
                    {tagsToKeep.map((tagName) => (
                      <div key={tagName}>• {tagName}</div>
                    ))}
                  </div>
                </div>
                <div className="flex-1 bg-white p-3 rounded border border-gray-200">
                  <div className="text-sm font-medium text-gray-700 mb-2">To date shift</div>
                  <div className="text-xs text-gray-600 space-y-1 max-h-32 overflow-y-auto">
                    {tagsToDateshift.map((tagName) => (
                      <div key={tagName}>• {tagName}</div>
                    ))}
                  </div>
                </div>
                <div className="flex-1 bg-white p-3 rounded border border-gray-200">
                  <div className="text-sm font-medium text-gray-700 mb-2">To randomize</div>
                  <div className="text-xs text-gray-600 space-y-1 max-h-32 overflow-y-auto">
                    {tagsToRandomize.map((tagName) => (
                      <div key={tagName}>• {tagName}</div>
                    ))}
                  </div>
                </div>
              </div>
            </div>

            <div className="mt-8 bg-gray-50 border border-gray-300 rounded p-4">
              <div className="text-md mb-3 font-medium text-gray-900">
                Global Deidentification Settings
              </div>
              <div className="text-sm text-gray-700 mb-4">
                The following settings are automatically enabled for HIPAA Safe Harbor
                compliance:
              </div>

              <div className="space-y-2">
                <div className="flex items-center text-sm">
                  <EnforcedCheckIcon />
                  <span className="text-gray-700">
                    <strong>Remove unspecified tags</strong> - Any tag not specified in the
                    lists above will be removed
                  </span>
                </div>
                <div className="flex items-center text-sm">
                  <EnforcedCheckIcon />
                  <span className="text-gray-700">
                    <strong>Remove overlays</strong> - All elements in 60xx groups (may contain
                    PHI)
                  </span>
                </div>
                <div className="flex items-center text-sm">
                  <EnforcedCheckIcon />
                  <span className="text-gray-700">
                    <strong>Remove curve data</strong> - All elements in 50xx groups
                  </span>
                </div>
                <div className="flex items-center text-sm">
                  <EnforcedCheckIcon />
                  <span className="text-gray-700">
                    <strong>Remove all private tags</strong> - Odd-numbered groups (not
                    specified by DICOM standard)
                  </span>
                </div>
              </div>
            </div>

            <div className="mt-8 bg-gray-50 border border-gray-300 rounded p-4">
              <div className="text-md mb-3 font-medium text-gray-900">
                Pixel Deidentification
              </div>
              <div className="flex items-center text-sm">
                <EnforcedCheckIcon />
                <span className="text-gray-700">
                  <strong>Deidentify pixel data from device scanners</strong> - Blacks out
                  regions of pixels in DICOM images from known devices
                </span>
              </div>
            </div>

            <div className="mt-8">
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
            </div>
            <div className="mt-8">
              <div className="text-md mb-4">Secondary Capture and PDF Handling</div>
              <div>
                <div className="text-sm text-gray-500 mb-2">
                  Secondary Capture images, PDFs, and other embedded content files may contain
                  PHI that cannot be safely removed through standard de-identification and will
                  be filtered from de-identification output.
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

          {/* Text Deidentification Sub-section */}
          <div className="mb-6">
            <div className="text-md font-medium mb-2">Text Deidentification</div>
            <div className="mt-4">
              <ColumnActions inputPath={inputFile} onStateChange={setColumnState} />
            </div>
            <div className="flex space-x-4 mt-4">
              <div className="flex-1">
                <div className="text-sm text-gray-500">Phrases to keep</div>
                <textarea
                  className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
                  name="text_to_keep"
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
                  name="text_to_remove"
                  value={textToRemove}
                  onChange={(event) => {
                    setTextToRemove(event.target.value);
                  }}
                ></textarea>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div className="m-4">
        <div className="flex items-center mb-4">
          <input
            type="checkbox"
            id="export_to_azure"
            className="mr-2"
            checked={exportToAzure}
            onChange={(event) => {
              setExportToAzure(event.target.checked);
            }}
          />
          <label htmlFor="export_to_azure">Transfer Data to IMAGINE</label>
        </div>

        <ScheduleInput
          enabled={scheduleEnabled}
          onEnabledChange={setScheduleEnabled}
          scheduledTime={scheduledTime}
          onScheduledTimeChange={setScheduledTime}
        />

        <button
          className={`mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100${formValid ? '' : ' opacity-50 cursor-not-allowed'}`}
          onClick={() => void runSingleClickICore()}
          id="singleclick_button"
          disabled={!formValid}
        >
          {buttonText}
        </button>
      </div>
    </>
  );
}
