import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';

import { cancelTask, getTaskStatus } from '../api/endpoints';
import { usePolling } from '../hooks/usePolling';
import { openFolder } from '../lib/electron';

interface FolderButtonProps {
  id: string;
  folder: string | null;
  className: string;
  children: string;
}

function FolderButton({ id, folder, className, children }: FolderButtonProps) {
  return (
    <button
      id={id}
      className={`${className}${folder ? '' : ' opacity-50 cursor-not-allowed'}`}
      disabled={!folder}
      onClick={() => {
        if (folder) openFolder(folder);
      }}
    >
      {children}
    </button>
  );
}

// Stop polling after this many consecutive identical log reads (matches the
// legacy page's behavior so an idle finished task stops hitting the server).
const MAX_SAME_CONTENT_POLLS = 200;

export function TaskProgress() {
  const [searchParams] = useSearchParams();
  const projectId = searchParams.get('project_id') ?? '';

  const [title, setTitle] = useState('');
  const [taskStatus, setTaskStatus] = useState<string | null>(null);
  const [logPath, setLogPath] = useState<string | null>(null);
  const [logsFolder, setLogsFolder] = useState<string | null>(null);
  const [outputFolder, setOutputFolder] = useState<string | null>(null);
  const [appdataFolder, setAppdataFolder] = useState<string | null>(null);
  const [logContent, setLogContent] = useState('');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [cancelledPopup, setCancelledPopup] = useState<{ title: string; message: string } | null>(
    null,
  );
  const [stopped, setStopped] = useState(false);

  const sameContentCount = useRef(0);
  const previousContent = useRef('');
  const logRef = useRef<HTMLPreElement>(null);

  usePolling(
    () => {
      void (async () => {
        try {
          const data = await getTaskStatus(projectId);
          setTitle(`${data.task_type_display || 'Task'} (${data.name})`);
          setTaskStatus(data.status);
          if (data.log_path) setLogPath(data.log_path);
          if (data.logs_folder) setLogsFolder(data.logs_folder);
          if (data.output_folder) setOutputFolder(data.output_folder);
          if (data.appdata_folder) setAppdataFolder(data.appdata_folder);

          if (data.status === 'FAILED') {
            setErrorMessage('Task has failed. Please check the logs for details.');
            setStopped(true);
          } else if (data.status === 'CANCELLED') {
            setCancelledPopup({ title: 'Task Cancelled', message: 'Task has been cancelled.' });
            setStopped(true);
            setTimeout(() => {
              window.location.href = '/task_list';
            }, 3000);
          }
        } catch (error) {
          console.error('Error checking task status:', error);
        }
      })();
    },
    5000,
    !stopped,
  );

  usePolling(
    () => {
      if (!logPath) {
        setLogContent('Loading. Please wait...');
        return;
      }
      void (async () => {
        try {
          const response = await fetch(
            `/get_log_content/?log_path=${encodeURIComponent(logPath)}`,
          );
          const data = await response.text();
          if (data === previousContent.current) {
            sameContentCount.current++;
            if (sameContentCount.current >= MAX_SAME_CONTENT_POLLS) {
              setStopped(true);
            }
            return;
          }
          sameContentCount.current = 0;
          previousContent.current = data;
          setLogContent(data);
        } catch (error) {
          console.error('Error fetching log:', error);
        }
      })();
    },
    1000,
    !stopped,
  );

  // Keep the log view pinned to the bottom as new content streams in.
  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [logContent]);

  const handleCancel = async () => {
    if (!confirm('Are you sure you want to cancel this task?')) return;
    try {
      await cancelTask(projectId);
      setCancelledPopup({ title: 'Cancelling Task', message: 'Task is being cancelled...' });
      setStopped(true);
      setTimeout(() => {
        window.location.href = '/task_list';
      }, 2000);
    } catch (error) {
      alert(`Failed to cancel task: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const showFolderLinks = Boolean(logsFolder ?? outputFolder ?? appdataFolder);
  const showCancel = taskStatus === 'PENDING' || taskStatus === 'RUNNING';

  return (
    <div className="px-4">
      <h1 className="text-xl flex-1">{title}</h1>
      {showFolderLinks && (
        <div id="folderLinks" className="mt-4 bg-white p-3 rounded-t text-xs flex justify-between">
          <FolderButton
            id="openOutputFolder"
            folder={outputFolder}
            className="px-2 py-1 bg-white shadow text-xs hover:bg-gray-100"
          >
            Open output folder
          </FolderButton>
          <div>
            <FolderButton
              id="openAppdataFolder"
              folder={appdataFolder}
              className="px-2 py-1 bg-white shadow text-xs hover:bg-gray-100 mr-2"
            >
              Open appdata folder
            </FolderButton>
            <FolderButton
              id="openLogsFolder"
              folder={logsFolder}
              className="px-2 py-1 bg-white shadow text-xs hover:bg-gray-100 mr-2"
            >
              Open logs folder
            </FolderButton>
            {showCancel && (
              <button
                id="cancelTask"
                className="px-2 py-1 bg-white shadow text-xs hover:bg-gray-100 text-orange-600 hover:text-orange-900"
                onClick={() => void handleCancel()}
              >
                Cancel task
              </button>
            )}
          </div>
        </div>
      )}
      {errorMessage !== null && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full">
          <div className="relative top-40 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white">
            <div className="mt-3 text-center">
              <h3 className="text-lg leading-6 font-medium text-gray-900">Error</h3>
              <div className="mt-2 px-7 py-3">
                <p className="text-sm text-gray-500">{errorMessage}</p>
              </div>
              <div className="items-center px-4 py-3 flex justify-center gap-3">
                <button
                  className={`px-4 py-2 bg-blue-500 text-white text-base font-medium rounded-md shadow-sm hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300${logsFolder ? '' : ' opacity-50 cursor-not-allowed'}`}
                  disabled={!logsFolder}
                  onClick={() => {
                    if (logsFolder) openFolder(logsFolder);
                  }}
                >
                  Open Logs
                </button>
                <button
                  className="px-4 py-2 bg-red-500 text-white text-base font-medium rounded-md shadow-sm hover:bg-red-600 focus:outline-none focus:ring-2 focus:ring-red-300"
                  onClick={() => {
                    setErrorMessage(null);
                    window.history.back();
                  }}
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      {cancelledPopup && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full">
          <div className="relative top-20 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white">
            <div className="mt-3 text-center">
              <h3 className="text-lg leading-6 font-medium text-gray-900">
                {cancelledPopup.title}
              </h3>
              <div className="mt-2 px-7 py-3">
                <p className="text-sm text-gray-500">{cancelledPopup.message}</p>
              </div>
              <div className="items-center px-4 py-3">
                <button
                  className="px-4 py-2 bg-green-500 text-white text-base font-medium rounded-md shadow-sm hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-300"
                  onClick={() => {
                    setCancelledPopup(null);
                  }}
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      <pre
        ref={logRef}
        className="bg-black text-white p-4 font-mono text-xs h-[calc(100vh-13rem-45px)] overflow-y-auto whitespace-pre-wrap"
        id="logContent"
      >
        {logContent}
      </pre>
    </div>
  );
}
