import { useEffect, useState } from 'react';

import { loadSettings } from '../api/endpoints';
import { submitRun, type JobRequest } from '../api/run';
import type { Settings } from '../api/types';
import {
  FilterList,
  ModalityFilterSection,
  useFilters,
  type SavedFilters,
} from '../components/filters';
import { PacsQuerySection, useQueryColumns } from '../components/PacsQuerySection';
import { PathInput } from '../components/PathInput';
import { useConstants } from '../hooks/useConstants';

export function ImageQuery() {
  const constants = useConstants();
  // Server context `modalities` (baked into the legacy template).
  const modalities = constants?.modalities ?? [];

  // The run payload reuses pacs_configs/application_aet from the settings
  // loaded on mount, exactly like the legacy page-level `settings` global.
  const [settings, setSettings] = useState<Settings>({});

  const [studyName, setStudyName] = useState('');
  const [inputFile, setInputFile] = useState('');
  const [outputFolder, setOutputFolder] = useState('');
  const [optionsOpen, setOptionsOpen] = useState(false);
  // Legacy only attached the validation listeners after /load_settings/
  // succeeded, so the run button stayed disabled (without the dimmed
  // classes) until then — and forever if the settings fetch failed.
  const [validationActive, setValidationActive] = useState(false);

  const query = useQueryColumns();
  // Legacy quirk preserved: the first-check modality prefill read the
  // `filters` settings key (not `query_filters`).
  const filters = useFilters('filters');
  const { applyFromSettings } = filters;
  const { prefill: prefillQuery } = query;

  // Pre-fill from saved settings, as the legacy DOMContentLoaded handler did.
  useEffect(() => {
    void (async () => {
      try {
        const loaded = await loadSettings();
        setSettings(loaded);

        prefillQuery(loaded);
        if (loaded.default_output_folder) {
          setOutputFolder(loaded.default_output_folder);
        }
        // Legacy loadFiltersFromSettings() over the query_filters blob: only
        // the general filters take effect at load time (its modality branch
        // targeted containers that never exist yet), which applyFromSettings
        // preserves.
        const queryFilters = loaded.query_filters as SavedFilters | undefined;
        if (queryFilters) {
          applyFromSettings(queryFilters);
        }

        setValidationActive(true);
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once on mount, as the legacy DOMContentLoaded handler did
  }, []);

  const formValid = Boolean(studyName.trim() && inputFile.trim());

  const runQuery = async () => {
    const data: JobRequest<'/run_query/'> = {
      study_name: studyName,
      output_folder: outputFolder,
      input_file: inputFile,
      pacs_configs: settings.pacs_configs ?? [],
      application_aet: settings.application_aet ?? '',
      ...filters.payload(modalities),
      ...query.payload(),
    };
    console.log(data);
    await submitRun('/run_query/', data, {
      scheduled: false,
      errorPrefix: 'Error starting query',
    });
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
        <PacsQuerySection query={query} />
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
            <FilterList filters={filters.generalFilters} onChange={filters.setGeneralFilters} />
          </div>
          <ModalityFilterSection modalities={modalities} filters={filters} />
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
    </>
  );
}
