import { useCallback, useEffect, useRef, useState } from 'react';
import { useOutletContext } from 'react-router';

import { ApiError, getJson, postJson } from '../../api/client';
import { loadSettings, saveSettings as postSettings } from '../../api/endpoints';
import type { Settings, StatusResponse } from '../../api/types';
import {
  FilterList,
  ModalityFilters,
  type Filter,
  type ModalityFilterMap,
} from '../../components/filters';
import { useConstants } from '../../hooks/useConstants';
import type { SettingsOutletContext } from './SettingsLayout';

type ImageSource = 'LOCAL' | 'PACS';

interface DeidFiltersSettings {
  general_filters?: Filter[];
  modality_filters?: Record<string, Filter[]>;
}

/** Extra free-form settings keys this page reads (legacy reads them straight
 * off the /load_settings/ JSON). */
interface ImageDeidSettingsData extends Settings {
  selected_protocol?: string;
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
}

interface ProtocolSettingsResponse {
  protocol_settings?: {
    tags_to_keep?: string;
    tags_to_dateshift?: string;
    tags_to_randomize?: string;
    date_shift_days?: number | string;
    is_restricted?: boolean;
    filters?: DeidFiltersSettings;
  };
}

/** In Electron, File objects carry the absolute path of the picked file. */
type ElectronFile = File & { path?: string };

/** Legacy settings.js generateActionString(): normalize protocol-file action
 * names to the *IgnoreCase identifiers the filter dropdowns use. */
function generateActionString(action: string): string {
  if (action == 'DoesNotContain') {
    return 'not_containsIgnoreCase';
  }
  if (action == 'Contains') {
    return 'containsIgnoreCase';
  }
  if (action == 'DoesNotStartWith') {
    return 'not_startsWithIgnoreCase';
  }
  if (action == 'StartsWith') {
    return 'startsWithIgnoreCase';
  }
  if (action == 'DoesNotEndWith') {
    return 'not_endsWithIgnoreCase';
  }
  if (action == 'EndsWith') {
    return 'endsWithIgnoreCase';
  }
  if (action == 'DoesNotEqual') {
    return 'not_equalsIgnoreCase';
  }
  if (action == 'Equals') {
    return 'equalsIgnoreCase';
  }
  return action;
}

/** Legacy showSaveMessage(): appends a transient status div next to the
 * layout's #saveSettingsBtn. That button lives in SettingsLayout (outside this
 * page's tree), so this stays imperative DOM, exactly as before. */
function showSaveMessage(message: string, isError = false) {
  const saveButton = document.getElementById('saveSettingsBtn');
  const existingMessage = document.querySelector('.settings-message');
  if (existingMessage) existingMessage.remove();

  const messageDiv = document.createElement('div');
  messageDiv.textContent = message;
  messageDiv.className = `settings-message mt-4 ml-4 ${isError ? 'text-red-500' : 'text-green-500'}`;
  saveButton?.parentNode?.appendChild(messageDiv);

  setTimeout(() => {
    messageDiv.remove();
  }, 3000);
}

export function ImageDeidSettings() {
  const { registerSaveHandler } = useOutletContext<SettingsOutletContext>();
  const constants = useConstants();
  const modalities = constants?.modalities ?? [];

  // Server context replacement: the template baked `protocols` in via
  // json_script; the SPA fetches the same list from /api/protocols/.
  const [protocols, setProtocols] = useState<(string | number)[]>([]);
  const [selectedProtocol, setSelectedProtocol] = useState('');
  // Legacy loadProtocolSettings() set input.disabled = is_restricted on every
  // input/textarea/select (re-enabling on a non-restricted protocol), then
  // re-enabled the protocol selector and image-source radios. Buttons were
  // untouched by that selector list.
  const [restricted, setRestricted] = useState(false);

  const [imageSource, setImageSource] = useState<ImageSource>('LOCAL');
  const [tagsToKeep, setTagsToKeep] = useState('');
  const [tagsToDateshift, setTagsToDateshift] = useState('');
  const [tagsToRandomize, setTagsToRandomize] = useState('');

  const [mappingDisplay, setMappingDisplay] = useState('');
  const [mappingFile, setMappingFile] = useState<ElectronFile | null>(null);
  const [useMappingFile, setUseMappingFile] = useState(false);
  const mappingFileInputRef = useRef<HTMLInputElement | null>(null);

  const [removeUnspecified, setRemoveUnspecified] = useState(false);
  const [removeOverlays, setRemoveOverlays] = useState(false);
  const [removeCurves, setRemoveCurves] = useState(false);
  const [removePrivate, setRemovePrivate] = useState(false);
  const [applyDefaultCtpFilterScript, setApplyDefaultCtpFilterScript] = useState(true);
  const [deidPixels, setDeidPixels] = useState(false);
  const [deidEngineRust, setDeidEngineRust] = useState(false);

  const [generalFilters, setGeneralFilters] = useState<Filter[]>([]);
  const [modalityFilters, setModalityFilters] = useState<ModalityFilterMap>({});

  // Legacy loadFiltersFromSettings(): always clears the general filter rows
  // (the container was wiped before general_filters was read), normalizes
  // actions via generateActionString(), and replaces only the modalities named
  // in modality_filters — other modalities' rows are left as they were.
  const applyFiltersFromSettings = useCallback((filters: DeidFiltersSettings | undefined) => {
    setGeneralFilters(
      (filters?.general_filters ?? []).map((filter) => ({
        ...filter,
        action: generateActionString(filter.action),
      })),
    );
    const modalityFiltersFromSettings = filters?.modality_filters;
    if (modalityFiltersFromSettings) {
      setModalityFilters((prev) => {
        const next = { ...prev };
        Object.entries(modalityFiltersFromSettings).forEach(([modality, modalityList]) => {
          next[modality] = modalityList.map((filter) => ({
            ...filter,
            action: generateActionString(filter.action),
          }));
        });
        return next;
      });
    }
  }, []);

  // Legacy handleProtocolChange() + loadProtocolSettings(). The per-protocol
  // settings endpoint is unchanged. Note: legacy also wrote
  // settings.date_shift_days into a [name="default_date_shift_days"] input,
  // but this page never renders one (and the server has that key commented
  // out), so there is nothing to set here.
  const handleProtocolChange = useCallback(
    async (protocolId: string) => {
      if (!protocolId) return;

      try {
        const data = await getJson<ProtocolSettingsResponse>(
          `/get_protocol_settings/${protocolId}/`,
        );
        const settings = data.protocol_settings;
        if (settings) {
          if (settings.tags_to_keep) {
            setTagsToKeep(settings.tags_to_keep);
          }
          if (settings.tags_to_dateshift) {
            setTagsToDateshift(settings.tags_to_dateshift);
          }
          if (settings.tags_to_randomize) {
            setTagsToRandomize(settings.tags_to_randomize);
          }

          // Handle restricted status (shows the red label and disables every
          // input/textarea/select except the protocol selector and the
          // image-source radios).
          setRestricted(Boolean(settings.is_restricted));

          console.log(settings.filters);
          applyFiltersFromSettings(settings.filters);
        }
      } catch (error) {
        console.error('Error loading protocol settings:', error);
      }
    },
    [applyFiltersFromSettings],
  );

  useEffect(() => {
    getJson<{ protocols: (string | number)[] }>('/api/protocols/')
      .then((data) => {
        setProtocols(data.protocols);
      })
      .catch((error: unknown) => {
        console.error('Error loading protocols:', error);
      });
  }, []);

  // Legacy loadSavedSettings() (DOMContentLoaded).
  useEffect(() => {
    void (async () => {
      try {
        const settings = (await loadSettings()) as ImageDeidSettingsData;

        if (settings.selected_protocol) {
          setSelectedProtocol(settings.selected_protocol);
          // Fired without awaiting (as in legacy), so the protocol's tags and
          // filters land after the defaults below and win.
          void handleProtocolChange(settings.selected_protocol);
        }
        if (
          settings.default_image_source === 'LOCAL' ||
          settings.default_image_source === 'PACS'
        ) {
          setImageSource(settings.default_image_source);
        }
        if (settings.default_tags_to_keep) {
          setTagsToKeep(settings.default_tags_to_keep);
        }
        if (settings.default_tags_to_dateshift) {
          setTagsToDateshift(settings.default_tags_to_dateshift);
        }
        if (settings.default_tags_to_randomize) {
          setTagsToRandomize(settings.default_tags_to_randomize);
        }
        if (settings.mapping_file_path) {
          setMappingDisplay(settings.mapping_file_path);
        }
        if (settings.use_mapping_file) {
          setUseMappingFile(Boolean(settings.use_mapping_file));
        }
        if (settings.default_deid_pixels) {
          setDeidPixels(Boolean(settings.default_deid_pixels));
        }
        if (settings.default_remove_unspecified !== undefined) {
          setRemoveUnspecified(Boolean(settings.default_remove_unspecified));
        }
        if (settings.default_remove_overlays !== undefined) {
          setRemoveOverlays(Boolean(settings.default_remove_overlays));
        }
        if (settings.default_remove_curves !== undefined) {
          setRemoveCurves(Boolean(settings.default_remove_curves));
        }
        if (settings.default_remove_private !== undefined) {
          setRemovePrivate(Boolean(settings.default_remove_private));
        }
        if (settings.default_apply_default_ctp_filter_script !== undefined) {
          setApplyDefaultCtpFilterScript(
            Boolean(settings.default_apply_default_ctp_filter_script),
          );
        }
        if (settings.deid_engine !== undefined) {
          setDeidEngineRust(settings.deid_engine === 'rust');
        }
        if (settings.deid_filters) {
          applyFiltersFromSettings(settings.deid_filters);
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, [applyFiltersFromSettings, handleProtocolChange]);

  // Legacy saveSettings(): POST this page's keys to /save_settings/ (the
  // server merges them into the existing settings.json).
  const saveSettings = async () => {
    // Legacy collectFilterData(): general filters are sent as-is; modality
    // filters only for known modalities with at least one row.
    const modalityFilterData: Record<string, Filter[]> = {};
    modalities.forEach((modality) => {
      const filters = modalityFilters[modality] ?? [];
      if (filters.length > 0) {
        modalityFilterData[modality] = filters;
      }
    });
    const filterData = {
      general_filters: generalFilters,
      modality_filters: modalityFilterData,
    };

    const data: Settings = {
      default_image_source: imageSource,
      default_tags_to_keep: tagsToKeep,
      default_tags_to_dateshift: tagsToDateshift,
      default_tags_to_randomize: tagsToRandomize,
      // Legacy read the file input's current selection at save time, falling
      // back to whatever path is shown in the readonly display.
      mapping_file_path: mappingFile ? mappingFile.path || mappingFile.name : mappingDisplay,
      use_mapping_file: useMappingFile,
      default_deid_pixels: deidPixels,
      default_remove_unspecified: removeUnspecified,
      default_remove_overlays: removeOverlays,
      default_remove_curves: removeCurves,
      default_remove_private: removePrivate,
      default_apply_default_ctp_filter_script: applyDefaultCtpFilterScript,
      deid_engine: deidEngineRust ? 'rust' : 'ctp',
      selected_protocol: selectedProtocol,
      deid_filters: filterData,
    };

    try {
      await postSettings(data);
      showSaveMessage('Settings saved successfully');
    } catch (error) {
      console.error('Error saving settings:', error);
      showSaveMessage('Error saving settings', true);
    }
  };

  // The layout's "Save Settings" button calls the registered handler.
  // Re-registered every render so it always closes over current state.
  useEffect(() => {
    registerSaveHandler(() => {
      void saveSettings();
    });
    return () => {
      registerSaveHandler(null);
    };
  });

  // Legacy #resetImageDeidBtn click handler.
  const resetSettings = async () => {
    const confirmed = confirm(
      'Are you sure you want to reset all Image Deidentification settings to iCore defaults? This action cannot be undone.',
    );

    if (!confirmed) {
      return;
    }

    try {
      const result = await postJson<StatusResponse>('/reset_deid_settings/', {
        settings_type: 'image_deid',
      });

      if (result.status === 'success') {
        window.scrollTo(0, 0);
        window.location.reload();
      } else {
        alert(`Error resetting settings: ${String(result.message)}`);
      }
    } catch (error) {
      if (error instanceof ApiError) {
        // Legacy alerted the body of any non-ok response; the api client
        // surfaces the server's message as an ApiError.
        alert(
          `Server error (status ${String(error.status)}): ${error.message.substring(0, 200)}`,
        );
        return;
      }
      console.error('Error resetting settings:', error);
      alert(
        `Error resetting settings: ${error instanceof Error ? error.message : String(error)}. Please try again.`,
      );
    }
  };

  return (
    <>
      {/* Protocol Selection Section (hidden, exactly as in the template) */}
      <div className="mt-6 hidden">
        <div className="text-md mb-4">Protocol Selection</div>
        <div className="flex items-center space-x-4">
          <label htmlFor="protocol_select">Select Protocol:</label>
          <select
            id="protocol_select"
            className="bg-white border-2 border-gray-400 p-1 w-64"
            value={selectedProtocol}
            onChange={(event) => {
              setSelectedProtocol(event.target.value);
              void handleProtocolChange(event.target.value);
            }}
          >
            <option value="">Select a protocol...</option>
            {protocols.map((protocol) => (
              <option key={String(protocol)} value={String(protocol)}>
                {protocol}
              </option>
            ))}
          </select>
          <span
            id="restricted-label"
            className={`text-red-500 font-medium${restricted ? '' : ' hidden'}`}
          >
            Restricted Protocol
          </span>
        </div>
      </div>

      {/* Default Image Source Section */}
      <div className="mt-8">
        <div className="text-md mb-4">Default Image Source</div>
        <div className="flex items-center space-x-4">
          <input
            type="radio"
            id="default_image_source_local"
            name="default_image_source"
            value="LOCAL"
            checked={imageSource === 'LOCAL'}
            onChange={() => {
              setImageSource('LOCAL');
            }}
          />
          <label htmlFor="default_image_source_local">Local folder</label>
          <input
            className="ml-4"
            type="radio"
            id="default_image_source_pacs"
            name="default_image_source"
            value="PACS"
            checked={imageSource === 'PACS'}
            onChange={() => {
              setImageSource('PACS');
            }}
          />
          <label htmlFor="default_image_source_pacs">PACS</label>
        </div>
      </div>

      {/* Default De-id Lists Section */}
      <div className="mt-8">
        <div className="text-md mb-4">Default Deidentification Lists</div>
        <div className="flex space-x-4">
          <div className="flex-1">
            <div className="text-sm text-gray-500">To keep</div>
            <textarea
              className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
              name="default_tags_to_keep"
              value={tagsToKeep}
              disabled={restricted}
              onChange={(event) => {
                setTagsToKeep(event.target.value);
              }}
            ></textarea>
          </div>
          <div className="flex-1">
            <div className="text-sm text-gray-500">To date shift</div>
            <textarea
              className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
              name="default_tags_to_dateshift"
              value={tagsToDateshift}
              disabled={restricted}
              onChange={(event) => {
                setTagsToDateshift(event.target.value);
              }}
            ></textarea>
          </div>
          <div className="flex-1">
            <div className="text-sm text-gray-500">To randomize</div>
            <textarea
              className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
              name="default_tags_to_randomize"
              value={tagsToRandomize}
              disabled={restricted}
              onChange={(event) => {
                setTagsToRandomize(event.target.value);
              }}
            ></textarea>
          </div>
        </div>
      </div>

      <div className="mt-8">
        <div className="text-md mb-4">Mapping File Configuration</div>
        <div className="grid grid-cols-1 gap-4">
          <div>
            <div className="text-sm text-gray-500 mb-2">Current Mapping File</div>
            <div className="flex items-center space-x-4">
              <input
                type="text"
                className="flex-1 border-2 border-gray-400 p-1"
                id="mapping_display"
                readOnly
                value={mappingDisplay}
                disabled={restricted}
              />
              <input
                type="file"
                id="mapping_file"
                className="hidden"
                accept=".xlsx,.xls"
                ref={mappingFileInputRef}
                disabled={restricted}
                onChange={(event) => {
                  const file: ElectronFile | undefined = event.target.files?.[0];
                  if (file) {
                    setMappingFile(file);
                    setMappingDisplay(file.path || file.name);
                  }
                }}
              />
              <button
                onClick={() => {
                  mappingFileInputRef.current?.click();
                }}
                className="px-4 py-1 bg-white shadow text-sm hover:bg-blue-100"
              >
                Select File
              </button>
              <input
                type="checkbox"
                id="use_mapping_file"
                name="use_mapping_file"
                checked={useMappingFile}
                disabled={restricted}
                onChange={(event) => {
                  setUseMappingFile(event.target.checked);
                }}
              />
              <label htmlFor="use_mapping_file" className="ml-1">
                Use Mapping File
              </label>
            </div>
            <div className="text-xs text-gray-400 mt-1">
              Excel file with columns: TagName, New-TagName (e.g., AccessionNumber,
              New-AccessionNumber)
            </div>
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
                id="default_remove_unspecified"
                name="default_remove_unspecified"
                className="mr-2"
                checked={removeUnspecified}
                disabled={restricted}
                onChange={(event) => {
                  setRemoveUnspecified(event.target.checked);
                }}
              />
              <label htmlFor="default_remove_unspecified">Remove unspecified tags</label>
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
                id="default_remove_overlays"
                name="default_remove_overlays"
                className="mr-2"
                checked={removeOverlays}
                disabled={restricted}
                onChange={(event) => {
                  setRemoveOverlays(event.target.checked);
                }}
              />
              <label htmlFor="default_remove_overlays">Remove overlays</label>
            </div>
            <div className="text-sm text-gray-500 ml-6">
              Remove all elements in 60xx groups. These are overlays and are sometimes removed
              when fully de-identifying an object because they can contain PHI.
            </div>
          </div>
          <div>
            <div className="flex items-center">
              <input
                type="checkbox"
                id="default_remove_curves"
                name="default_remove_curves"
                className="mr-2"
                checked={removeCurves}
                disabled={restricted}
                onChange={(event) => {
                  setRemoveCurves(event.target.checked);
                }}
              />
              <label htmlFor="default_remove_curves">Remove curve data</label>
            </div>
            <div className="text-sm text-gray-500 ml-6">
              Remove all elements in 50xx groups. These are groups which contain curve data.
            </div>
          </div>
          <div>
            <div className="flex items-center">
              <input
                type="checkbox"
                id="default_remove_private"
                name="default_remove_private"
                className="mr-2"
                checked={removePrivate}
                disabled={restricted}
                onChange={(event) => {
                  setRemovePrivate(event.target.checked);
                }}
              />
              <label htmlFor="default_remove_private">Remove all private tags</label>
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
                id="default_apply_default_ctp_filter_script"
                name="default_apply_default_ctp_filter_script"
                className="mr-2"
                checked={applyDefaultCtpFilterScript}
                disabled={restricted}
                onChange={(event) => {
                  setApplyDefaultCtpFilterScript(event.target.checked);
                }}
              />
              <label htmlFor="default_apply_default_ctp_filter_script">
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
        <div>
          <div className="flex items-center">
            <input
              type="checkbox"
              id="default_deid_pixels"
              name="default_deid_pixels"
              checked={deidPixels}
              disabled={restricted}
              onChange={(event) => {
                setDeidPixels(event.target.checked);
              }}
            />
            <label htmlFor="default_deid_pixels" className="ml-2">
              Deidentify pixel data from known devices by default
            </label>
          </div>
          <div className="text-sm text-gray-500 ml-6">
            Blanks regions of pixels in a DICOM image from known devices.
          </div>
        </div>
      </div>

      <div className="mt-8">
        <div className="text-md mb-4">Deidentification Engine</div>
        <div>
          <div className="flex items-center">
            <input
              type="checkbox"
              id="deid_engine_rust"
              name="deid_engine_rust"
              checked={deidEngineRust}
              disabled={restricted}
              onChange={(event) => {
                setDeidEngineRust(event.target.checked);
              }}
            />
            <label htmlFor="deid_engine_rust" className="ml-2">
              Use experimental Rust engine
            </label>
          </div>
          <div className="text-sm text-gray-500 ml-6">
            Use dicom-deid-rs instead of CTP for deidentification. This engine is faster but is
            still under active development.
          </div>
        </div>
      </div>

      {/* Default De-id Filters Section */}
      <div className="mt-8">
        <div className="text-md mb-4">Default Deidentification Filters</div>

        {/* General Filters Section */}
        <div className="mb-4">
          <div className="text-sm text-gray-600 mb-2">General Filters</div>
          <div className="text-sm text-gray-400 mb-2">Only include images where</div>
          {/* The fieldset is a zero-footprint wrapper (preflight removes its
              border/margin/padding) used to disable the shared filter rows
              when a restricted protocol is loaded. */}
          <fieldset disabled={restricted} className="m-0 p-0 border-0 min-w-0">
            <FilterList filters={generalFilters} onChange={setGeneralFilters} />
          </fieldset>
        </div>

        {/* Modality Filters Section */}
        <div className="mt-4">
          <div className="text-md mb-2">Default Modality Filters</div>
          <fieldset disabled={restricted} className="m-0 p-0 border-0 min-w-0">
            <ModalityFilters
              modalities={modalities}
              value={modalityFilters}
              onChange={setModalityFilters}
            />
          </fieldset>
        </div>
      </div>

      {/* Reset Settings Section */}
      <div className="mt-12 pt-4 border-t border-gray-300">
        <div className="text-md mb-2">Reset Deidentification Settings</div>
        <a
          href="#"
          id="resetImageDeidBtn"
          className="text-sm text-blue-600 hover:underline"
          onClick={(event) => {
            event.preventDefault();
            void resetSettings();
          }}
        >
          Reset to iCore defaults.
        </a>
      </div>
    </>
  );
}
