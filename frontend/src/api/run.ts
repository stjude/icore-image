import { ApiError, postJson } from './client';
import type { RunResponse } from './types';

interface SubmitOptions {
  /** Navigate to the task list instead of task progress on success. */
  scheduled: boolean;
  /** Page-specific alert wording, e.g. "Error starting de-identification". */
  errorPrefix: string;
}

/** Shared submit flow for every run form: POST, navigate on success, alert
 * with the legacy wording on failure. */
export async function submitRun(
  url: string,
  data: Record<string, unknown>,
  { scheduled, errorPrefix }: SubmitOptions,
): Promise<void> {
  try {
    const result = await postJson<RunResponse>(url, data);
    console.log(result);
    if (result.status === 'success') {
      window.location.href = scheduled
        ? '/task_list'
        : `/task_progress?project_id=${result.project_id}`;
    } else {
      console.error(`${errorPrefix}:`, result);
      alert(result.message || errorPrefix);
    }
  } catch (error) {
    if (error instanceof ApiError) {
      // Legacy parsed error responses as JSON and alerted the
      // server-provided message; postJson surfaces that as an ApiError.
      console.error(`${errorPrefix}:`, error);
      alert(error.message || errorPrefix);
    } else {
      console.error('Error:', error);
      alert(`${errorPrefix}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}
