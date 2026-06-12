import { useEffect, useState } from 'react';

import { getJson, postJson } from '../api/client';
import type { JobRequest } from '../api/run';
import { loadSettings } from '../api/endpoints';
import type { RunResponse } from '../api/types';
import { PathInput } from '../components/PathInput';
import { SasUrlRequiredModal, validateSasUrlOnLoad } from '../components/SasUrlRequiredModal';

interface AdminSettings {
  imagine_sas_url?: string;
  [key: string]: unknown;
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
    const data: JobRequest<'/run_export/'> = {
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
        <SasUrlRequiredModal
          onClose={() => {
            setShowSasUrlRequiredModal(false);
          }}
        />
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
