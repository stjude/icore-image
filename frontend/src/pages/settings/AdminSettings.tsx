import { useCallback, useEffect, useRef, useState } from 'react';
import { useOutletContext } from 'react-router';

import { getJson, postForm, postJson } from '../../api/client';
import type { SettingsOutletContext } from './SettingsLayout';

interface AdminSettingsResponse {
  protocol_file?: string;
  date_shift_range?: number | string;
  site_id?: string;
  imagine_sas_url?: string;
  beta_updates_enabled?: boolean;
  [key: string]: unknown;
}

interface VerifyPasswordResponse {
  valid: boolean;
}

interface ValidateSasUrlResponse {
  valid: boolean;
  error?: string;
}

interface SasValidationState {
  tone: 'info' | 'success' | 'error';
  text: string;
}

const SAS_TONE_CLASSES: Record<SasValidationState['tone'], string> = {
  info: 'bg-blue-100 text-blue-700',
  success: 'bg-green-100 text-green-700',
  error: 'bg-red-100 text-red-700',
};

export function AdminSettings() {
  const { registerSaveHandler, showSaveMessage } = useOutletContext<SettingsOutletContext>();

  // Password gate (legacy shows the modal on DOMContentLoaded and reveals
  // #settings-content only after /verify_admin_password/ succeeds).
  const [unlocked, setUnlocked] = useState(false);
  const [password, setPassword] = useState('');
  // Legacy only ever removes the 'hidden' class from #password-error; once
  // shown it stays visible for the rest of the session.
  const [passwordError, setPasswordError] = useState(false);

  // Settings form fields.
  const [protocolDisplay, setProtocolDisplay] = useState('');
  const [protocolFile, setProtocolFile] = useState<File | null>(null);
  // Legacy markup ships value="-3210" as the initial date shift range.
  const [dateShiftRange, setDateShiftRange] = useState('-3210');
  const [siteId, setSiteId] = useState('');
  const [imagineSasUrl, setImagineSasUrl] = useState('');
  const [betaUpdatesEnabled, setBetaUpdatesEnabled] = useState(false);

  // SAS URL validation UI.
  const [sasValidating, setSasValidating] = useState(false);
  const [sasValidation, setSasValidation] = useState<SasValidationState | null>(null);

  const protocolFileInput = useRef<HTMLInputElement>(null);

  const loadSettings = useCallback(async () => {
    try {
      const settings = await getJson<AdminSettingsResponse>('/load_admin_settings/');
      if (settings.protocol_file) {
        setProtocolDisplay(settings.protocol_file.split('/').pop() ?? '');
      }
      if (settings.date_shift_range) {
        setDateShiftRange(String(settings.date_shift_range));
      }
      if (settings.site_id) {
        setSiteId(settings.site_id);
      }
      if (settings.imagine_sas_url) {
        setImagineSasUrl(settings.imagine_sas_url);
      }
      setBetaUpdatesEnabled(Boolean(settings.beta_updates_enabled));
    } catch (error) {
      console.error('Error loading settings:', error);
    }
  }, []);

  // Legacy runs loadSettings() on DOMContentLoaded (before the password is
  // verified) and again after a successful verification.
  useEffect(() => {
    void loadSettings();
  }, [loadSettings]);

  const verifyPassword = useCallback(async () => {
    try {
      const data = await postJson<VerifyPasswordResponse>('/verify_admin_password/', { password });
      if (data.valid) {
        setUnlocked(true);
        await loadSettings();
      } else {
        setPasswordError(true);
        setPassword('');
      }
    } catch (error) {
      console.error('Error verifying password:', error);
      setPasswordError(true);
    }
  }, [password, loadSettings]);

  const saveSettings = useCallback(async () => {
    try {
      const formData = new FormData();
      if (protocolFile) {
        formData.append('protocol_file', protocolFile);
      }
      if (dateShiftRange) {
        formData.append('default_date_shift_days', dateShiftRange);
      }
      if (siteId) {
        formData.append('site_id', siteId);
      }
      if (imagineSasUrl) {
        formData.append('imagine_sas_url', imagineSasUrl);
      }
      formData.append('beta_updates_enabled', betaUpdatesEnabled ? 'true' : 'false');

      await postForm<unknown>('/save_admin_settings/', formData);

      showSaveMessage('Settings saved successfully');
    } catch (error) {
      console.error('Error saving settings:', error);
      showSaveMessage('Error saving settings', true);
    }
  }, [protocolFile, dateShiftRange, siteId, imagineSasUrl, betaUpdatesEnabled, showSaveMessage]);

  // Legacy wires #saveSettingsBtn to saveSettings() on DOMContentLoaded, i.e.
  // even before the password gate is passed (the full-screen modal overlay
  // keeps the button unreachable while it is shown).
  useEffect(() => {
    registerSaveHandler(() => {
      void saveSettings();
    });
    return () => {
      registerSaveHandler(null);
    };
  }, [registerSaveHandler, saveSettings]);

  const validateSasUrlInAdmin = useCallback(async () => {
    const sasUrl = imagineSasUrl.trim();

    if (!sasUrl) {
      setSasValidation({ tone: 'error', text: 'Please enter a SAS URL' });
      return;
    }

    setSasValidating(true);
    setSasValidation({ tone: 'info', text: 'Validating SAS URL...' });

    try {
      const result = await postJson<ValidateSasUrlResponse>('/validate_sas_url/', {
        sas_url: sasUrl,
      });

      if (result.valid) {
        setSasValidation({
          tone: 'success',
          text: 'SAS URL is valid! Remember to click "Save Settings" to save it.',
        });
      } else {
        setSasValidation({ tone: 'error', text: `Validation failed: ${String(result.error)}` });
      }
    } catch (error) {
      setSasValidation({
        tone: 'error',
        text: `Error: ${error instanceof Error ? error.message : String(error)}`,
      });
    } finally {
      setSasValidating(false);
    }
  }, [imagineSasUrl]);

  return (
    <>
      <div
        id="password-modal"
        className="fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center"
        style={{ zIndex: 1000, display: unlocked ? 'none' : 'flex' }}
      >
        <div className="bg-white p-6 rounded-lg shadow-lg">
          <h2 className="text-lg mb-4">Enter Administrator Password</h2>
          <input
            type="password"
            id="admin-password"
            className="border-2 border-gray-300 p-2 mb-4 w-full"
            placeholder="Enter password"
            value={password}
            onChange={(event) => {
              setPassword(event.target.value);
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                void verifyPassword();
              }
            }}
          />
          <div className="flex justify-end space-x-2">
            <button
              onClick={() => void verifyPassword()}
              className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              Submit
            </button>
          </div>
          <div className={`text-red-500 mt-2${passwordError ? '' : ' hidden'}`} id="password-error">
            Incorrect password
          </div>
        </div>
      </div>

      <div id="settings-content" className={unlocked ? '' : 'hidden'}>
        <div className="mt-6">
          <div className="text-md mb-4">Protocol Configuration</div>
          <div className="grid grid-cols-1 gap-4">
            <div>
              <div className="text-sm text-gray-500 mb-2">Current Protocol File</div>
              <div className="flex items-center space-x-4">
                <input
                  type="text"
                  className="flex-1 border-2 border-gray-400 p-1"
                  id="protocol_display"
                  value={protocolDisplay}
                  readOnly
                />
                <input
                  type="file"
                  id="protocol_file"
                  className="hidden"
                  accept=".xlsx,.xls"
                  ref={protocolFileInput}
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) {
                      setProtocolDisplay(file.name);
                    }
                    setProtocolFile(file ?? null);
                  }}
                />
                <button
                  onClick={() => protocolFileInput.current?.click()}
                  className="px-4 py-1 bg-white shadow text-sm hover:bg-blue-100"
                >
                  Upload New
                </button>
              </div>
            </div>
          </div>
        </div>

        <div className="mt-8">
          <div className="text-md mb-4">Site Data Configuration</div>
          <div className="grid grid-cols-2 gap-8">
            <div>
              <div className="text-sm text-gray-500 mb-2">Date Shift Range (days)</div>
              <input
                type="number"
                className="w-full border-2 border-gray-400 p-1"
                id="date_shift_range"
                min="0"
                value={dateShiftRange}
                onChange={(event) => {
                  setDateShiftRange(event.target.value);
                }}
              />
              <div className="text-xs text-gray-400 mt-1">Number of days to shift dates</div>
            </div>
            <div>
              <div className="text-sm text-gray-500 mb-2">Site ID</div>
              <input
                type="text"
                className="w-full border-2 border-gray-400 p-1"
                id="site_id"
                placeholder="Enter site ID"
                value={siteId}
                onChange={(event) => {
                  setSiteId(event.target.value);
                }}
              />
            </div>
          </div>
          <div className="mt-8">
            <div className="text-md mb-4">Azure Storage SAS URL</div>
            <div className="text-sm text-gray-500 mb-2">SAS URL for Data Transfer to IMAGINE</div>
            <div className="flex gap-2">
              <input
                type="text"
                className="flex-1 border-2 border-gray-400 p-1"
                id="imagine_sas_url"
                placeholder="https://account.blob.core.windows.net/container?sas_token"
                value={imagineSasUrl}
                onChange={(event) => {
                  setImagineSasUrl(event.target.value);
                }}
              />
              <button
                type="button"
                onClick={() => void validateSasUrlInAdmin()}
                id="validateSasBtnAdmin"
                className="px-4 py-1 bg-white shadow text-sm rounded hover:bg-blue-100"
                disabled={sasValidating}
              >
                {sasValidating ? 'Validating...' : 'Validate'}
              </button>
            </div>
            <p className="text-xs text-gray-500 mt-1">
              Azure Blob Storage SAS URL for Data Transfer to IMAGINE
            </p>
            <div
              id="sasValidationResultAdmin"
              className={`mt-2 p-2 text-sm ${
                sasValidation ? SAS_TONE_CLASSES[sasValidation.tone] : 'hidden'
              }`}
            >
              {sasValidation?.text}
            </div>
          </div>
        </div>

        <div className="mt-8">
          <div className="text-md mb-4">Software Updates</div>
          <label className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="beta_updates_enabled"
              className="h-4 w-4"
              checked={betaUpdatesEnabled}
              onChange={(event) => {
                setBetaUpdatesEnabled(event.target.checked);
              }}
            />
            <span className="text-sm">Enable beta releases</span>
          </label>
          <div className="text-xs text-gray-400 mt-1">
            Includes pre-release builds. Takes effect on next launch.
          </div>
        </div>
      </div>
    </>
  );
}
