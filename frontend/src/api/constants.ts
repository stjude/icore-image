import { getJson } from './client';

export interface Constants {
  /** [attributeId, displayName] pairs for the DICOM filter dropdowns. */
  dicom_fields: [string, string][];
  modalities: string[];
  export_modalities: string[];
  timezones: string[];
  hipaa: {
    tags_to_keep: string[];
    tags_to_dateshift: string[];
    tags_to_randomize: string[];
  };
}

let cached: Promise<Constants> | null = null;

/** Backend-defined lists that the template engine used to bake into pages.
 * Immutable per app run, so fetched once and shared. */
export function getConstants(): Promise<Constants> {
  cached ??= getJson<Constants>('/api/constants/');
  return cached;
}
