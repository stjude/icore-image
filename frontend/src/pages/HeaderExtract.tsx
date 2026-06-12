import { useEffect, useState } from 'react';

import { ApiError, postJson } from '../api/client';
import type { JobRequest } from '../api/run';
import { loadSettings } from '../api/endpoints';
import type { RunResponse } from '../api/types';
import { PathInput } from '../components/PathInput';

export function HeaderExtract() {
  const [studyName, setStudyName] = useState('');
  const [inputFolder, setInputFolder] = useState('');
  const [outputFolder, setOutputFolder] = useState('');
  const [headersToExtract, setHeadersToExtract] = useState('');
  const [extractAllHeaders, setExtractAllHeaders] = useState(false);
  const [optionsOpen, setOptionsOpen] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        const settings = await loadSettings();
        if (settings.default_output_folder) {
          setOutputFolder(settings.default_output_folder);
        }
        const defaultHeaders = settings.default_headers_to_extract;
        if (typeof defaultHeaders === 'string' && defaultHeaders) {
          setHeadersToExtract(defaultHeaders);
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  const runHeaderExtract = async () => {
    const trimmedStudyName = studyName.trim();
    const trimmedInputFolder = inputFolder.trim();
    const trimmedOutputFolder = outputFolder.trim();
    const trimmedHeadersToExtract = headersToExtract.trim();

    if (!trimmedStudyName) {
      alert('Project name is required');
      return;
    }
    if (!trimmedInputFolder) {
      alert('Input folder is required');
      return;
    }
    if (!trimmedOutputFolder) {
      alert('Output folder is required');
      return;
    }

    if (!trimmedHeadersToExtract && !extractAllHeaders) {
      alert('Please either enter headers to extract or check "Extract All Headers"');
      return;
    }

    const data: JobRequest<'/run_header_extract/'> = {
      study_name: trimmedStudyName,
      input_folder: trimmedInputFolder,
      output_folder: trimmedOutputFolder,
      extract_all_headers: extractAllHeaders,
      headers_to_extract: trimmedHeadersToExtract,
    };
    console.log(data);

    try {
      const result = await postJson<RunResponse>('/run_header_extract/', data);
      if (result.status === 'success') {
        window.location.href = `/task_progress?project_id=${String(result.project_id)}`;
      } else {
        console.error('Error starting header extract:', result);
        alert(result.message ?? 'Error starting header extraction');
      }
    } catch (error) {
      if (error instanceof ApiError) {
        // The legacy page parsed error responses as JSON and alerted the
        // server-provided message; postJson surfaces that as an ApiError.
        console.error('Error starting header extract:', error);
        alert(error.message || 'Error starting header extraction');
      } else {
        console.error('Error:', error);
        alert(
          `Error starting header extraction: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1">Header Extraction</h1>
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
        />

        <div className="text-sm text-black-5 mt-4">Input Folder</div>
        <PathInput
          className="mt-2 w-full h-full border-2 border-gray-400 p-2"
          name="input_folder"
          placeholder="Drag and drop images folder"
          value={inputFolder}
          onChange={setInputFolder}
        />

        <div className="text-sm text-black-5 mt-4">Output Folder</div>
        <PathInput
          className="mt-2 w-full h-full border-2 border-gray-400 p-2"
          name="output_folder"
          value={outputFolder}
          onChange={setOutputFolder}
        />

        <div className="flex items-center mt-4">
          <button
            className="text-black"
            id="headerExtractOptionsButton"
            onClick={() => {
              setOptionsOpen((open) => !open);
            }}
          >
            {optionsOpen ? '▲' : '▼'}
          </button>
          <div className="text-sm text-black ml-4">Header Extraction Options</div>
        </div>
        <div
          id="header_extract_options"
          className={optionsOpen ? 'mt-4 ml-4' : 'mt-4 hidden ml-4'}
        >
          <div className="text-sm text-black-5 mt-4">Headers to Extract</div>
          <textarea
            id="headers_to_extract"
            name="headers_to_extract"
            className="mt-2 w-full border-2 border-gray-400 p-2"
            rows={6}
            placeholder={
              'Enter one header per line, e.g.:\nPatientID\nStudyDate\nModality\nAccessionNumber'
            }
            // The legacy page toggled display block/none on load and on
            // checkbox change, so the textarea always renders as a block.
            style={{ display: extractAllHeaders ? 'none' : 'block' }}
            value={headersToExtract}
            onChange={(event) => {
              setHeadersToExtract(event.target.value);
            }}
          />
          <div
            id="headersToExtractInstructions"
            className="text-xs text-gray-500 mt-2"
            style={{ display: extractAllHeaders ? 'none' : 'block' }}
          >
            Enter DICOM tag names (one per line) to extract. Check &quot;Extract All Headers&quot;
            to extract all headers.
          </div>

          <div className="flex items-center mt-4">
            <input
              type="checkbox"
              id="extract_all_headers"
              name="extract_all_headers"
              className="mr-2"
              checked={extractAllHeaders}
              onChange={(event) => {
                setExtractAllHeaders(event.target.checked);
              }}
            />
            <label htmlFor="extract_all_headers" className="text-sm text-gray-700">
              Extract All Headers
            </label>
          </div>
          <div className="text-xs text-gray-500 mt-2 ml-6">
            If checked, extracts all DICOM headers found in the images.
          </div>
        </div>
      </div>

      <div className="m-4">
        <button
          className="mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100"
          id="header_extract_button"
          onClick={() => void runHeaderExtract()}
        >
          Run Extraction
        </button>
      </div>
    </>
  );
}
