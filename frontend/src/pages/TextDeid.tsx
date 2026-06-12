import { useEffect, useState } from 'react';

import { postJson } from '../api/client';
import { loadSettings } from '../api/endpoints';
import type { RunResponse } from '../api/types';
import { ColumnActions, type ColumnActionsState } from '../components/ColumnActions';
import { PathInput } from '../components/PathInput';

export function TextDeid() {
  const [studyName, setStudyName] = useState('');
  const [inputFile, setInputFile] = useState('');
  const [outputFolder, setOutputFolder] = useState('');
  const [textToKeep, setTextToKeep] = useState('');
  const [textToRemove, setTextToRemove] = useState('');
  // Legacy default is 0; overwritten by settings.date_shift_range when truthy.
  const [dateShiftDays, setDateShiftDays] = useState<number>(0);
  const [optionsOpen, setOptionsOpen] = useState(false);
  const [columnState, setColumnState] = useState<ColumnActionsState>({
    actions: {},
    allAssigned: false,
  });

  // Pre-fill from saved settings, as the legacy loadSavedSettings() did on
  // DOMContentLoaded (default output folder, remembered keep/remove phrases,
  // and the date shift range used in the run payload).
  useEffect(() => {
    void (async () => {
      try {
        const settings = await loadSettings();
        console.log(settings);
        if (settings.default_output_folder) {
          setOutputFolder(settings.default_output_folder);
        }
        const defaultTextToKeep = settings.default_text_to_keep;
        if (typeof defaultTextToKeep === 'string' && defaultTextToKeep) {
          setTextToKeep(defaultTextToKeep);
        }
        const defaultTextToRemove = settings.default_text_to_remove;
        if (typeof defaultTextToRemove === 'string' && defaultTextToRemove) {
          setTextToRemove(defaultTextToRemove);
        }
        const dateShiftRange = settings.date_shift_range;
        if (typeof dateShiftRange === 'number' && dateShiftRange) {
          setDateShiftDays(dateShiftRange);
        }
        if (settings.selected_protocol) {
          // If you want to show the selected protocol somewhere
          console.log('Selected protocol:', settings.selected_protocol);
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  const formValid = Boolean(studyName.trim() && inputFile.trim() && columnState.allAssigned);

  const runDeid = async () => {
    const data = {
      study_name: studyName,
      input_file: inputFile,
      output_folder: outputFolder,
      column_actions: columnState.actions,
      text_to_keep: textToKeep,
      text_to_remove: textToRemove,
      date_shift_days: dateShiftDays,
    };
    console.log(data);
    try {
      const result = await postJson<RunResponse>('/run_text_deid/', data);
      if (result.status === 'success') {
        window.location.href = `/task_progress?project_id=${result.project_id}`;
      } else {
        console.error('Error starting de-identification:', result);
      }
    } catch (error) {
      console.error('Error:', error);
    }
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1">Text Deidentification</h1>
      </div>
      <div className="flex-1 bg-white p-4 ml-4 mt-4">
        {/* Project Name Section */}
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

        {/* Input File Section */}
        <div className="text-sm text-black-5 mt-4">Input File</div>
        <PathInput
          className="mt-2 w-full h-full border-2 border-gray-400 p-2"
          name="input_file"
          placeholder="Drag and drop excel file to deidentify"
          value={inputFile}
          onChange={setInputFile}
        />

        {/* Output Folder Section */}
        <div className="text-sm text-gray-500 mt-4">Output Folder</div>
        <PathInput
          className="mt-2 mb-2 w-full h-full border-2 border-gray-400 p-2"
          name="output_folder"
          value={outputFolder}
          onChange={setOutputFolder}
        />

        {/* Deidentification Options Section */}
        <div className="flex items-center mt-4">
          <button
            className="text-black"
            id="deidOptionsButton"
            onClick={() => {
              setOptionsOpen((open) => !open);
            }}
          >
            {optionsOpen ? '▲' : '▼'}
          </button>
          <div className="text-sm text-black ml-4">Deidentification Options</div>
        </div>
        <div id="deid_options" className={optionsOpen ? 'mt-4 ml-4' : 'mt-4 hidden ml-4'}>
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

      {/* Run Button Section */}
      <div className="m-4">
        <button
          id="text_deid_button"
          className={`mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100${formValid ? '' : ' opacity-50 cursor-not-allowed'}`}
          disabled={!formValid}
          onClick={() => void runDeid()}
        >
          Deidentify
        </button>
      </div>
    </>
  );
}
