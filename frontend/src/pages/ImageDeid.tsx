import { useCallback, useEffect, useState } from 'react';

import { getJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import { submitRun, type JobRequest } from '../api/run';
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
import { ScheduleInput } from '../components/ScheduleInput';
import { useConstants } from '../hooks/useConstants';

type ImageSource = 'LOCAL' | 'PACS';

/** Extra free-form settings keys this page reads (legacy reads them straight
 * off the /load_settings/ JSON). */
interface ImageDeidSettings extends DeidOptionsSettings {
  selected_protocol?: string;
  deid_filters?: SavedFilters;
}

interface ProtocolSettingsResponse {
  protocol_settings?: {
    tags_to_keep?: string;
    tags_to_dateshift?: string;
    tags_to_randomize?: string;
    filters?: SavedFilters;
    is_restricted?: boolean;
  };
}

export function ImageDeid() {
  const constants = useConstants();
  const modalities = constants?.modalities ?? [];

  // The legacy page kept the whole /load_settings/ response in a page-global
  // `settings` object and read date_shift_range / site_id / pacs_configs /
  // application_aet / mapping_file_path from it at submit time.
  const [settings, setSettings] = useState<ImageDeidSettings>({});

  const [studyName, setStudyName] = useState('');
  const [imageSource, setImageSource] = useState<ImageSource>('LOCAL');
  const [inputFile, setInputFile] = useState('');
  const [inputFolder, setInputFolder] = useState('');
  const [outputFolder, setOutputFolder] = useState('');
  const [deidOptionsOpen, setDeidOptionsOpen] = useState(false);
  // Set once (and never cleared) when a restricted protocol loads, mirroring
  // the legacy handleProtocolChange() that disabled every input inside
  // #deid_options without ever re-enabling them.
  const [deidOptionsDisabled, setDeidOptionsDisabled] = useState(false);
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduledTime, setScheduledTime] = useState('');

  const query = useQueryColumns();
  const filters = useFilters('deid_filters');
  const deid = useDeidOptions();
  const { applyFromSettings } = filters;
  const { set: setDeidOptions, prefill: prefillDeidOptions } = deid;
  const { prefill: prefillQuery } = query;

  // Legacy handleProtocolChange(). The protocol <select> markup is commented
  // out in the template, so this is only reachable via
  // settings.selected_protocol on load. The per-protocol settings endpoint is
  // unchanged.
  const handleProtocolChange = useCallback(
    async (protocolId: string) => {
      if (!protocolId) return;
      try {
        const data = await getJson<ProtocolSettingsResponse>(
          `/get_protocol_settings/${protocolId}/`,
        );
        console.log('Received protocol settings:', data); // Debug log
        if (data.protocol_settings) {
          setDeidOptions({
            tagsToKeep: data.protocol_settings.tags_to_keep || '',
            tagsToDateshift: data.protocol_settings.tags_to_dateshift || '',
            tagsToRandomize: data.protocol_settings.tags_to_randomize || '',
          });
          if (data.protocol_settings.filters) {
            applyFromSettings(data.protocol_settings.filters);
          }
          // The legacy "Restricted Protocol" label lives in the commented-out
          // protocol selector markup, so there is nothing to toggle here.
          if (data.protocol_settings.is_restricted) {
            setDeidOptionsDisabled(true);
          }
        }
      } catch (error) {
        console.error('Error loading protocol settings:', error);
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps -- hook helpers are stable per render cycle
    [],
  );

  // Pre-fill from saved settings, as the legacy DOMContentLoaded handler did.
  useEffect(() => {
    void (async () => {
      try {
        const loaded = (await loadSettings()) as ImageDeidSettings;
        setSettings(loaded);

        if (loaded.default_image_source === 'LOCAL' || loaded.default_image_source === 'PACS') {
          setImageSource(loaded.default_image_source);
        }
        prefillQuery(loaded);
        if (loaded.default_output_folder) {
          setOutputFolder(loaded.default_output_folder);
        }
        if (loaded.selected_protocol) {
          // Fired without awaiting (as in legacy), so the protocol's tags and
          // filters land after the defaults below and win.
          void handleProtocolChange(loaded.selected_protocol);
        }
        prefillDeidOptions(loaded);
        if (loaded.deid_filters) {
          applyFromSettings(loaded.deid_filters);
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once on mount, as the legacy DOMContentLoaded handler did
  }, []);

  // Legacy validateImageDeidForm(): project name plus the source-appropriate
  // input path.
  const formValid = Boolean(
    studyName.trim() && (imageSource === 'LOCAL' ? inputFolder.trim() : inputFile.trim()),
  );

  const isPacs = imageSource === 'PACS';

  // Legacy updateButtonText().
  const buttonText = scheduleEnabled
    ? isPacs
      ? 'Schedule Pull Images and Deidentify'
      : 'Schedule Deidentify'
    : isPacs
      ? 'Pull Images and Deidentify'
      : 'Deidentify';

  const runDeid = async () => {
    const data: JobRequest<'/run_deid/'> = {
      study_name: studyName,
      image_source: imageSource,
      input_folder: inputFolder,
      output_folder: outputFolder,
      input_file: inputFile,
      ...deid.payload(settings),
      ...filters.payload(modalities),
      ...query.payload(),
    };

    if (imageSource === 'PACS' && inputFile) {
      if (!inputFile.endsWith('.xlsx')) {
        alert('Input file must be an Excel file.');
        return;
      }
    }

    if (scheduleEnabled) {
      if (!scheduledTime) {
        alert('Please select a scheduled time');
        return;
      }
      data.scheduled_time = scheduledTime;
    }

    await submitRun('/run_deid/', data, {
      scheduled: scheduleEnabled,
      errorPrefix: 'Error starting de-identification',
    });
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1">Image Deidentification</h1>
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
        {/* The protocol selector is commented out in the legacy template, so it
            is not rendered here either. If revived, the protocol list now comes
            from GET /api/protocols/ ({"protocols": [...]}) and selecting one
            should call handleProtocolChange(protocol); a restricted protocol
            shows the red "Restricted Protocol" label. */}
        <div className="text-sm mt-6 text-black flex items-center space-x-4">
          Image Source:
          <input
            className="ml-4"
            type="radio"
            id="image_source_local"
            name="image_source"
            value="LOCAL"
            checked={imageSource === 'LOCAL'}
            onChange={() => {
              setImageSource('LOCAL');
            }}
          />
          <span>Local folder</span>
          <input
            className="ml-4"
            type="radio"
            id="image_source_pacs"
            name="image_source"
            value="PACS"
            checked={imageSource === 'PACS'}
            onChange={() => {
              setImageSource('PACS');
            }}
          />{' '}
          <span>PACS</span>
        </div>
        <div id="pacs_options" className={isPacs ? 'mt-4' : 'hidden mt-4'}>
          <div className="text-sm text-gray-500">Input File</div>
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
        </div>
        <div id="local_folder_options">
          <div id="input_folder_container" className={isPacs ? 'hidden' : undefined}>
            <div className="text-sm text-gray-500 mt-4">Input Folder</div>
            <PathInput
              className="mt-2 w-full h-full border-2 border-gray-400 p-2"
              name="input_folder"
              placeholder="Drag and drop images folder"
              value={inputFolder}
              onChange={setInputFolder}
            />
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
          {/* General Filters Section */}
          <div className="mb-4">
            <div className="text-sm text-gray-600 mb-2">General Filters</div>
            <div className="text-sm text-gray-400 mb-2">Only include images where</div>
            {/* The fieldset is a zero-footprint wrapper (preflight removes its
                border/margin/padding) used to disable the shared filter rows
                when a restricted protocol is loaded. */}
            <fieldset disabled={deidOptionsDisabled} className="m-0 p-0 border-0 min-w-0">
              <FilterList filters={filters.generalFilters} onChange={filters.setGeneralFilters} />
            </fieldset>
          </div>

          <ModalityFilterSection
            modalities={modalities}
            filters={filters}
            disabled={deidOptionsDisabled}
          />

          <DeidOptionsSection deid={deid} disabled={deidOptionsDisabled} />
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
          className={`mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100${formValid ? '' : ' opacity-50 cursor-not-allowed'}`}
          onClick={() => void runDeid()}
          id="deid_button"
          disabled={!formValid}
        >
          {buttonText}
        </button>
      </div>
    </>
  );
}
