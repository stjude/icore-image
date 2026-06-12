import { useEffect, useState } from 'react';

import { getJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import { submitRun } from '../api/run';
import {
  DeidOptionsSection,
  useDeidOptions,
  type DeidOptionsSettings,
} from '../components/DeidOptionsSection';
import {
  FilterList,
  ModalityFilterSection,
  useFilters,
  type SavedFilters,
} from '../components/filters';
import { PacsQuerySection, useQueryColumns } from '../components/PacsQuerySection';
import { PathInput } from '../components/PathInput';
import { SasUrlRequiredModal, validateSasUrlOnLoad } from '../components/SasUrlRequiredModal';
import { ScheduleInput } from '../components/ScheduleInput';
import { useConstants } from '../hooks/useConstants';

/** Extra free-form settings keys this page reads (legacy reads them straight
 * off the /load_settings/ JSON). */
interface ImageDeidExportSettings extends DeidOptionsSettings {
  deid_filters?: SavedFilters;
}

export function ImageDeidExport() {
  const constants = useConstants();
  // This page's server context used the export modality list (CT first),
  // not the query-page ordering in constants.modalities.
  const modalities = constants?.export_modalities ?? [];

  // The legacy page kept the whole /load_settings/ and /load_admin_settings/
  // responses in page-global objects and read date_shift_range / site_id /
  // pacs_configs / application_aet / mapping_file_path / imagine_sas_url from
  // them at submit time.
  const [settings, setSettings] = useState<ImageDeidExportSettings>({});
  const [sasUrl, setSasUrl] = useState('');

  const [showSasUrlRequiredModal, setShowSasUrlRequiredModal] = useState(false);
  // Mirrors the legacy page: the button starts disabled (without the dimmed
  // classes) and form validation only activates after the SAS URL check
  // passes; if the check fails the button stays permanently disabled.
  const [validationActive, setValidationActive] = useState(false);

  const [studyName, setStudyName] = useState('');
  const [inputFile, setInputFile] = useState('');
  const [outputFolder, setOutputFolder] = useState('');
  const [deidOptionsOpen, setDeidOptionsOpen] = useState(false);
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduledTime, setScheduledTime] = useState('');

  const query = useQueryColumns();
  const filters = useFilters('deid_filters');
  const deid = useDeidOptions();
  const { applyFromSettings } = filters;
  const { prefill: prefillDeidOptions } = deid;
  const { prefill: prefillQuery } = query;

  // Pre-fill from saved settings, as the legacy DOMContentLoaded handler did.
  // Legacy returned early (before any pre-fill, leaving the form blank and
  // the button disabled) when the SAS URL was missing or failed validation.
  useEffect(() => {
    void (async () => {
      try {
        const loaded = (await loadSettings()) as ImageDeidExportSettings;
        setSettings(loaded);

        const adminSettings = await getJson<{ imagine_sas_url?: string }>(
          '/load_admin_settings/',
        );
        const url = adminSettings.imagine_sas_url || '';
        setSasUrl(url);

        if (!url || url.trim() === '') {
          setShowSasUrlRequiredModal(true);
          return;
        }
        const result = await validateSasUrlOnLoad(url);
        if (!result.valid) {
          setShowSasUrlRequiredModal(true);
          return;
        }

        prefillQuery(loaded);
        if (loaded.default_output_folder) {
          setOutputFolder(loaded.default_output_folder);
        }
        prefillDeidOptions(loaded);
        if (loaded.deid_filters) {
          applyFromSettings(loaded.deid_filters);
        }

        setValidationActive(true);
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once on mount, as the legacy DOMContentLoaded handler did
  }, []);

  // Legacy validateForm(): project name plus input file.
  const formValid = Boolean(studyName.trim() && inputFile.trim());

  // Legacy updateButtonText().
  const buttonText = scheduleEnabled
    ? 'Schedule Pull Images, Deidentify and Export'
    : 'Pull Images, Deidentify and Export';

  const runImageDeidExport = async () => {
    const data: Record<string, unknown> = {
      study_name: studyName,
      input_file: inputFile,
      output_folder: outputFolder,
      ...deid.payload(settings),
      sas_url: sasUrl,
      ...filters.payload(modalities),
      ...query.payload(),
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

    await submitRun('/run_imagedeidexport/', data, {
      scheduled: scheduleEnabled,
      errorPrefix: 'Error starting deidentification and export',
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

        <PacsQuerySection query={query} />

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
            <FilterList filters={filters.generalFilters} onChange={filters.setGeneralFilters} />
          </div>

          <ModalityFilterSection modalities={modalities} filters={filters} />

          <DeidOptionsSection deid={deid} />
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
    </>
  );
}
