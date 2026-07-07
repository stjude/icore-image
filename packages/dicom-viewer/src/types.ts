import type { DicomDataSource } from './dataSource';

/**
 * Minimal series shape the viewer needs. Field names are kept snake_case to
 * match the typical DICOM-derived API payloads, so host mapping is near-identity.
 */
export interface ViewerSeries {
    id: string;
    series_description?: string | null;
    series_number?: number | null;
    modality: string;
    instance_count: number;
}

export interface ViewerStudy {
    id: string;
    study_description?: string | null;
    study_name?: string | null;
    series: ViewerSeries[];
}

export interface ViewerProps {
    studies: ViewerStudy[];
    dataSource: DicomDataSource;
    /** Called when a series download/parse fails. Defaults to console.error. */
    onError?: (error: unknown) => void;
    /**
     * Whether the viewer should run the one-time global Cornerstone init on mount.
     * Set false if the host application already initializes Cornerstone elsewhere.
     * Defaults to true.
     */
    initCornerstone?: boolean;
}
