import { useEffect, useState } from 'react';

import { getJson, postJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import type { RunResponse } from '../api/types';
import { PathInput } from '../components/PathInput';

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

export function ImageExport() {
  const [studyName, setStudyName] = useState('');
  const [inputFolder, setInputFolder] = useState('');
  const [sasUrl, setSasUrl] = useState('');
  const [showSasUrlRequiredModal, setShowSasUrlRequiredModal] = useState(false);
  // Mirrors the legacy page: the button starts disabled (without the dimmed
  // classes) and form validation only activates after the SAS URL check
  // passes; if the check fails the button stays permanently disabled.
  const [validationActive, setValidationActive] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        // Legacy page fetches /load_settings/ too, but never uses the result.
        await loadSettings();
        const adminSettings = await getJson<AdminSettings>('/load_admin_settings/');
        const url = adminSettings.imagine_sas_url;

        if (!url || url.trim() === '') {
          setShowSasUrlRequiredModal(true);
          return;
        }
        const result = await validateSasUrlOnLoad(url);
        if (!result.valid) {
          setShowSasUrlRequiredModal(true);
          return;
        }

        setSasUrl(url);
        setValidationActive(true);
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  const isValid = Boolean(studyName.trim() && inputFolder.trim() && sasUrl);

  const runExport = async () => {
    const data = {
      study_name: studyName.trim(),
      input_folder: inputFolder.trim(),
      sas_url: sasUrl,
    };

    setSubmitting(true);

    try {
      const result = await postJson<RunResponse>('/run_export/', data);
      if (result.status === 'success') {
        window.location.href = `/task_progress?project_id=${result.project_id}`;
      } else {
        setSubmitting(false);
        alert(result.message ? result.message : 'Error starting export. Please try again.');
        console.error('Error starting export:', result);
      }
    } catch (error) {
      setSubmitting(false);
      alert(`Error starting export: ${error instanceof Error ? error.message : String(error)}`);
      console.error('Error:', error);
    }
  };

  return (
    <>
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
                  Please contact your administrator or navigate to Admin Settings to configure the
                  Azure Blob Storage SAS URL for data transfer to IMAGINE.
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
        <h1 className="text-xl flex-1">Transfer Data to IMAGINE</h1>
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
          onKeyPress={(event) => {
            // Legacy: onkeypress="return event.charCode !== 32" (blocks spaces).
            if (event.charCode === 32) event.preventDefault();
          }}
        />

        <div className="text-sm text-black-5 mt-4">Select Folder for Data Transfer to IMAGINE</div>
        <PathInput
          className="mt-2 w-full h-full border-2 border-gray-400 p-2"
          name="input_folder"
          placeholder="Drag and drop folder to export"
          value={inputFolder}
          onChange={setInputFolder}
        />
      </div>

      <div className="m-4">
        <button
          className={`mt-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100${
            validationActive && !isValid ? ' opacity-50 cursor-not-allowed' : ''
          }`}
          onClick={() => void runExport()}
          id="export_button"
          disabled={submitting || !validationActive || !isValid}
        >
          {submitting ? 'Starting Export...' : 'Run Export'}
        </button>
      </div>
    </>
  );
}
