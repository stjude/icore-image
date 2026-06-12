import { getJson, postJson } from './client';
import type { ColumnAction, Settings, StatusResponse } from './types';

export function loadSettings(): Promise<Settings> {
  return getJson<Settings>('/load_settings/');
}

export function saveSettings(settings: Settings): Promise<StatusResponse> {
  return postJson<StatusResponse>('/save_settings/', settings);
}

export interface TaskSummary {
  id: number;
  name: string;
  status: string;
  status_display: string;
  task_type_display: string;
  created_at: string;
  scheduled_time: string | null;
  /** The task's output folder on disk, deleted along with the project. */
  output_dir: string;
}

export function getTasks(): Promise<{ tasks: TaskSummary[] }> {
  return getJson<{ tasks: TaskSummary[] }>('/api/tasks/');
}

export interface TaskStatus {
  status: string;
  log_path: string;
  name: string;
  task_type: string;
  task_type_display: string;
  logs_folder: string;
  output_folder: string;
  appdata_folder: string;
}

export function getTaskStatus(projectId: string): Promise<TaskStatus> {
  return getJson<TaskStatus>(`/api/task_status/${projectId}/`);
}

export function cancelTask(taskId: number | string): Promise<StatusResponse> {
  return postJson<StatusResponse>(`/cancel_task/${taskId}/`);
}

export function deleteTask(taskId: number | string): Promise<StatusResponse> {
  return postJson<StatusResponse>(`/delete_task/${taskId}/`);
}

export interface SpreadsheetColumnsResponse {
  status: 'success' | 'error';
  columns?: string[];
  message?: string;
}

export function getSpreadsheetColumns(
  inputFile: string,
): Promise<SpreadsheetColumnsResponse> {
  return postJson<SpreadsheetColumnsResponse>('/get_spreadsheet_columns/', {
    input_file: inputFile,
  });
}

export interface ColumnActionsPayload {
  column_actions: Record<string, ColumnAction>;
}
