/** settings.json is a free-form store; known keys are typed, the rest pass
 * through untouched (save_settings round-trips the whole object). */
export interface Settings {
  icore_usecase?: 'internal' | 'imagine' | '';
  default_output_folder?: string;
  default_image_source?: string;
  default_query_method?: string;
  default_accession_header?: string;
  default_mrn_header?: string;
  default_date_header?: string;
  default_date_window_days?: number | string;
  pacs_configs?: PacsConfig[];
  application_aet?: string;
  timezone?: string;
  debug_logging?: boolean;
  deferred_delivery?: boolean;
  cmove_batch_size?: number | string;
  column_actions?: Record<string, ColumnAction>;
  default_column_actions?: Record<string, ColumnAction>;
  text_to_keep?: string;
  text_to_remove?: string;
  [key: string]: unknown;
}

export interface PacsConfig {
  ip: string;
  port: number | string;
  ae: string;
}

export type ColumnAction = 'keep' | 'deid' | 'drop';

export interface RunResponse {
  status: 'success' | 'error';
  project_id: number;
  log_path: string;
  message?: string;
}

export interface StatusResponse {
  status: 'success' | 'error';
  message?: string;
}
