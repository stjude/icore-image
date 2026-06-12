import { useEffect, useState } from 'react';
import { useOutletContext } from 'react-router';

import { postJson } from '../../api/client';
import { loadSettings, saveSettings } from '../../api/endpoints';
import type { Settings } from '../../api/types';
import { PathInput } from '../../components/PathInput';
import { useConstants } from '../../hooks/useConstants';
import type { SettingsOutletContext } from './SettingsLayout';

/** One editable "Connected PACS" row; mirrors the legacy .pacs-config inputs
 * (ip/port/aet all free text). */
interface PacsRow {
  ip: string;
  port: string;
  ae: string;
}

const EMPTY_PACS_ROW: PacsRow = { ip: '', port: '', ae: '' };

export function GeneralSettings() {
  const { registerSaveHandler } = useOutletContext<SettingsOutletContext>();
  const constants = useConstants();

  const [pacsConfigs, setPacsConfigs] = useState<PacsRow[]>([EMPTY_PACS_ROW]);
  const [applicationAet, setApplicationAet] = useState('');
  const [deferredDelivery, setDeferredDelivery] = useState(false);
  const [cmoveBatchSize, setCmoveBatchSize] = useState('50');
  const [timezone, setTimezone] = useState('');
  const [defaultQueryMethod, setDefaultQueryMethod] = useState('Accession');
  const [defaultAccessionHeader, setDefaultAccessionHeader] = useState('');
  const [defaultMrnHeader, setDefaultMrnHeader] = useState('');
  const [defaultDateHeader, setDefaultDateHeader] = useState('');
  const [defaultDateWindowDays, setDefaultDateWindowDays] = useState('0');
  const [defaultOutputFolder, setDefaultOutputFolder] = useState('');
  const [debugLogging, setDebugLogging] = useState(false);
  const [pacsResultVisible, setPacsResultVisible] = useState(false);
  const [pacsResultMessage, setPacsResultMessage] = useState('');

  // The legacy template baked the timezone <select> options in server-side,
  // so the first US/* timezone was selected (and saved) whenever no timezone
  // had been stored yet. Reproduce that default once the constants arrive;
  // a stored timezone (loaded below) always wins.
  useEffect(() => {
    const firstTimezone = constants?.timezones[0];
    if (firstTimezone) {
      setTimezone((previous) => previous || firstTimezone);
    }
  }, [constants]);

  // Pre-fill from saved settings, as the legacy loadSavedSettings() did on
  // DOMContentLoaded. Falsy values are skipped exactly as the legacy `if`
  // guards did (except default_date_window_days, which only skips undefined).
  useEffect(() => {
    void (async () => {
      try {
        const settings = await loadSettings();
        if (settings.pacs_configs) {
          const rows = settings.pacs_configs.map((config) => ({
            ip: config.ip,
            port: String(config.port),
            ae: config.ae,
          }));
          // Legacy kept one blank row when the saved list was empty.
          setPacsConfigs(rows.length > 0 ? rows : [EMPTY_PACS_ROW]);
        }
        if (settings.application_aet) {
          setApplicationAet(settings.application_aet);
        }
        if (settings.default_query_method) {
          setDefaultQueryMethod(settings.default_query_method);
        }
        if (settings.default_accession_header) {
          setDefaultAccessionHeader(settings.default_accession_header);
        }
        if (settings.default_mrn_header) {
          setDefaultMrnHeader(settings.default_mrn_header);
        }
        if (settings.default_date_header) {
          setDefaultDateHeader(settings.default_date_header);
        }
        if (settings.default_date_window_days !== undefined) {
          setDefaultDateWindowDays(String(settings.default_date_window_days));
        }
        if (settings.default_output_folder) {
          setDefaultOutputFolder(settings.default_output_folder);
        }
        if (settings.timezone) {
          setTimezone(settings.timezone);
        }
        if (settings.debug_logging) {
          setDebugLogging(settings.debug_logging);
        }
        if (settings.deferred_delivery) {
          setDeferredDelivery(settings.deferred_delivery);
        }
        if (settings.cmove_batch_size) {
          setCmoveBatchSize(String(settings.cmove_batch_size));
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  const addPacsConfig = () => {
    setPacsConfigs((rows) => [...rows, EMPTY_PACS_ROW]);
  };

  const removePacsConfig = (index: number) => {
    // Legacy removePacsConfig() refused to remove the last remaining row.
    setPacsConfigs((rows) => (rows.length > 1 ? rows.filter((_, i) => i !== index) : rows));
  };

  const updatePacsConfig = (index: number, field: keyof PacsRow, value: string) => {
    setPacsConfigs((rows) =>
      rows.map((row, i) => (i === index ? { ...row, [field]: value } : row)),
    );
  };

  const testPacsConnection = async (index: number) => {
    const config = pacsConfigs[index];
    if (!config) return;
    try {
      const result = await postJson<{ status: string; error?: string }>(
        '/test_pacs_connection/',
        {
          pacs_ip: config.ip,
          pacs_port: config.port,
          pacs_aet: config.ae,
          application_aet: applicationAet,
        },
      );
      console.log(result);
      if (result.status === 'success') {
        setPacsResultMessage(
          'PACS connection successful. Please save your settings before proceeding.',
        );
      } else {
        setPacsResultMessage('PACS connection failed: ' + String(result.error));
      }
    } catch (error) {
      setPacsResultMessage('Error testing PACS connection: ' + String(error));
    }
    setPacsResultVisible(true);
  };

  const persistSettings = async () => {
    const data: Settings = {
      pacs_configs: pacsConfigs.map((config) => ({
        ip: config.ip,
        port: config.port,
        ae: config.ae,
      })),
      application_aet: applicationAet,
      default_query_method: defaultQueryMethod,
      default_accession_header: defaultAccessionHeader,
      default_mrn_header: defaultMrnHeader,
      default_date_header: defaultDateHeader,
      default_date_window_days: parseInt(defaultDateWindowDays) || 0,
      default_output_folder: defaultOutputFolder,
      timezone,
      debug_logging: debugLogging,
      deferred_delivery: deferredDelivery,
      cmove_batch_size: parseInt(cmoveBatchSize) || 50,
    };

    try {
      await saveSettings(data);
      // The legacy page appended a transient "Settings Saved" toast next to
      // the layout-owned save button; recreate it the same way since the
      // button lives in SettingsLayout, outside this component's tree.
      const saveButton = document.getElementById('saveSettingsBtn');
      document.querySelector('.settings-saved-message')?.remove();
      const successMessage = document.createElement('div');
      successMessage.textContent = 'Settings Saved';
      successMessage.className = 'settings-saved-message text-green-500 mt-4 ml-4';
      saveButton?.parentNode?.appendChild(successMessage);
      setTimeout(() => {
        successMessage.remove();
      }, 3000);
    } catch (error) {
      console.error('Error saving settings:', error);
    }
  };

  // Expose the save handler to the layout's "Save Settings" button (the React
  // replacement for the legacy page-global saveSettings()). Re-registered each
  // render so the handler always sees current state.
  useEffect(() => {
    registerSaveHandler(() => {
      void persistSettings();
    });
    return () => {
      registerSaveHandler(null);
    };
  });

  return (
    <>
      {/* PACS Configuration Section */}
      <div className="mt-6">
        <div className="text-md mb-4">PACS Configuration</div>

        {/* iCore Instance Subsection */}
        <div className="mb-6">
          <div className="text-sm text-gray-600 mb-3">iCore Instance</div>
          <div>
            <div className="text-sm text-gray-500 mb-2">Application AET</div>
            <input
              type="text"
              className="w-full border-2 border-gray-400 p-1"
              name="application_aet"
              value={applicationAet}
              onChange={(event) => {
                setApplicationAet(event.target.value);
              }}
            />
          </div>
        </div>

        {/* Connected PACS Subsection */}
        <div>
          <div className="text-sm text-gray-600 mb-3">Connected PACS</div>
          <div id="pacs-configurations">
            {pacsConfigs.map((config, index) => (
              <div className="pacs-config mb-3" key={index}>
                <div className="grid grid-cols-3 gap-8">
                  <div>
                    <div className="text-sm text-gray-500 mb-2">IP Address</div>
                    <input
                      type="text"
                      className="w-full border-2 border-gray-400 p-1"
                      name="pacs_ip"
                      value={config.ip}
                      onChange={(event) => {
                        updatePacsConfig(index, 'ip', event.target.value);
                      }}
                    />
                  </div>
                  <div>
                    <div className="text-sm text-gray-500 mb-2">Port</div>
                    <input
                      type="text"
                      className="w-full border-2 border-gray-400 p-1"
                      name="pacs_port"
                      value={config.port}
                      onChange={(event) => {
                        updatePacsConfig(index, 'port', event.target.value);
                      }}
                    />
                  </div>
                  <div>
                    <div className="text-sm text-gray-500 mb-2">AET</div>
                    <input
                      type="text"
                      className="w-full border-2 border-gray-400 p-1"
                      name="pacs_aet"
                      value={config.ae}
                      onChange={(event) => {
                        updatePacsConfig(index, 'ae', event.target.value);
                      }}
                    />
                  </div>
                </div>
                <div className="flex gap-2 mt-2">
                  <button
                    onClick={() => void testPacsConnection(index)}
                    className="px-2 py-1 bg-white shadow text-sm rounded hover:bg-blue-100"
                  >
                    Test
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      removePacsConfig(index);
                    }}
                    className="px-2 py-1 bg-white shadow text-sm rounded hover:bg-blue-100"
                  >
                    Remove
                  </button>
                </div>
              </div>
            ))}
          </div>

          <div className="mt-4">
            <button
              onClick={addPacsConfig}
              className="px-2 py-1 bg-white shadow text-sm rounded hover:bg-blue-100"
            >
              + Add PACS Configuration
            </button>
          </div>
        </div>
      </div>

      {/* Deferred Delivery Option */}
      <div className="mt-4">
        <div className="flex items-center">
          <input
            type="checkbox"
            id="deferred_delivery"
            name="deferred_delivery"
            className="mr-2"
            checked={deferredDelivery}
            onChange={(event) => {
              setDeferredDelivery(event.target.checked);
            }}
          />
          <label htmlFor="deferred_delivery" className="text-sm">
            Enable deferred delivery (keep listener open until all images arrive)
          </label>
        </div>
      </div>

      {/* C-MOVE Batch Size Section */}
      <div className="mt-4">
        <div className="text-sm text-gray-500 mb-2">C-MOVE Batch Size (studies per batch)</div>
        <input
          type="number"
          className="w-64 border-2 border-gray-400 p-1"
          id="cmove_batch_size"
          min="1"
          value={cmoveBatchSize}
          onChange={(event) => {
            setCmoveBatchSize(event.target.value);
          }}
        />
      </div>

      {/* General Settings Section */}
      <div className="mt-8">
        <div className="text-md mb-4">General Settings</div>
        <div className="text-sm text-gray-500 mb-2">Timezone</div>
        <select
          className="w-64 border-2 border-gray-400 p-1"
          name="timezone"
          value={timezone}
          onChange={(event) => {
            setTimezone(event.target.value);
          }}
        >
          {(constants?.timezones ?? []).map((tz) => (
            <option key={tz} value={tz}>
              {tz}
            </option>
          ))}
        </select>
      </div>

      {/* Input Excel File Defaults Section */}
      <div className="mt-8">
        <div className="text-md mb-4">Input Excel File Defaults</div>

        <div className="mb-4">
          <div className="text-sm text-gray-500 mb-2">Default Query Method</div>
          <select
            className="w-64 bg-white border-2 border-gray-400 p-1"
            name="default_query_method"
            value={defaultQueryMethod}
            onChange={(event) => {
              setDefaultQueryMethod(event.target.value);
            }}
          >
            <option value="Accession">Accession</option>
            <option value="MRN">MRN + Date</option>
          </select>
        </div>

        <div className="grid grid-cols-3 gap-8">
          <div>
            <div className="text-sm text-gray-500 mb-2">Default Accession Header</div>
            <input
              type="text"
              className="w-full border-2 border-gray-400 p-1"
              name="default_accession_header"
              value={defaultAccessionHeader}
              onChange={(event) => {
                setDefaultAccessionHeader(event.target.value);
              }}
            />
          </div>
          <div>
            <div className="text-sm text-gray-500 mb-2">Default MRN Header</div>
            <input
              type="text"
              className="w-full border-2 border-gray-400 p-1"
              name="default_mrn_header"
              value={defaultMrnHeader}
              onChange={(event) => {
                setDefaultMrnHeader(event.target.value);
              }}
            />
          </div>
          <div>
            <div className="text-sm text-gray-500 mb-2">Default Date Header</div>
            <input
              type="text"
              className="w-full border-2 border-gray-400 p-1"
              name="default_date_header"
              value={defaultDateHeader}
              onChange={(event) => {
                setDefaultDateHeader(event.target.value);
              }}
            />
          </div>
        </div>

        <div className="mt-4">
          <div className="text-sm text-gray-500 mb-2">Default Date Window (Days)</div>
          <input
            type="number"
            className="w-64 border-2 border-gray-400 p-1"
            name="default_date_window_days"
            min="0"
            value={defaultDateWindowDays}
            onChange={(event) => {
              setDefaultDateWindowDays(event.target.value);
            }}
          />
        </div>
      </div>

      {/* Default Output Folder Section */}
      <div className="mt-8">
        <div className="text-md mb-4">Default Output Folder</div>
        <div className="flex items-center space-x-4">
          <PathInput
            className="flex-1 border-2 border-gray-400 p-1"
            name="default_output_folder"
            value={defaultOutputFolder}
            onChange={setDefaultOutputFolder}
          />
        </div>
      </div>

      {/* Export Project List Section */}
      <div className="mt-8">
        {/* <div className="text-md mb-4">Export Project List</div>
        <a href="#" className="text-blue-600 hover:text-blue-800 text-sm">Download excel file</a> */}
        <div className="flex items-center mt-4">
          <input
            type="checkbox"
            id="debug_logging"
            name="debug_logging"
            className="mr-2"
            checked={debugLogging}
            onChange={(event) => {
              setDebugLogging(event.target.checked);
            }}
          />
          <label htmlFor="debug_logging" className="text-sm">
            Enable debug logging
          </label>
        </div>
      </div>

      <div
        id="pacs-connection-result"
        className={`fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center${pacsResultVisible ? '' : ' hidden'}`}
        style={{ zIndex: 1000 }}
      >
        <div className="bg-white p-6 rounded-lg shadow-lg">
          <h3 className="text-lg font-medium" id="pacs-result-title">
            PACS Connection Test
          </h3>
          <div id="pacs-result-message" className="text-center mb-4">
            {pacsResultMessage}
          </div>
          <div className="flex justify-end">
            <button
              onClick={() => {
                setPacsResultVisible(false);
              }}
              className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </>
  );
}
