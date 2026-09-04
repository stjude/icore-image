import { ZipReader, BlobReader, BlobWriter, FileEntry } from '@zip.js/zip.js';
import type { ViewerSeries } from './types';

/** Reports download progress as a fraction 0..1, or undefined when indeterminate. */
export type ProgressCallback = (fraction: number | undefined) => void;

export interface LoadSeriesOptions {
    onProgress?: ProgressCallback;
    signal?: AbortSignal;
}

/** A single DICOM instance whose bytes are resolved lazily. */
export interface DicomInstanceSource {
    /** Stable identifier (e.g. filename or instance UID) used for ordering/debug. */
    readonly name: string;
    getBytes(): Promise<Blob>;
}

export interface SeriesLoadResult {
    instances: DicomInstanceSource[];
}

/**
 * Describes how a host application supplies DICOM bytes to the viewer. The viewer
 * owns everything downstream (Cornerstone file manager, parsing, multi-frame handling,
 * stack rendering); the data source only has to produce ordered per-instance bytes
 * and, optionally, a thumbnail.
 */
export interface DicomDataSource {
    loadSeries(series: ViewerSeries, options?: LoadSeriesOptions): Promise<SeriesLoadResult>;
    /**
     * Optional. Resolve an image URL (typically an object URL) for the series
     * thumbnail, or null when none exists. Sources without thumbnails (e.g. loading
     * from disk) should omit this method entirely.
     */
    getThumbnailUrl?(series: ViewerSeries): Promise<string | null>;
}

export interface ZipDataSourceOptions {
    /** Fetch the series as a zip blob. Should honor onProgress/signal when possible. */
    fetchZip: (series: ViewerSeries, options: LoadSeriesOptions) => Promise<Blob>;
    fetchThumbnail?: (series: ViewerSeries) => Promise<string | null>;
}

/**
 * Builds a {@link DicomDataSource} from a function that delivers a per-series zip blob.
 * Handles unzipping, filtering to `.dcm` entries, and stable ordering by filename so
 * host adapters never touch zip internals.
 */
export function createZipDataSource(options: ZipDataSourceOptions): DicomDataSource {
    const cache = new Map<string, Blob>();

    const source: DicomDataSource = {
        async loadSeries(series, loadOptions = {}) {
            let blob = cache.get(series.id);
            if (!blob) {
                blob = await options.fetchZip(series, loadOptions);
                cache.set(series.id, blob);
            }

            const reader = new ZipReader(new BlobReader(blob));
            try {
                const entries = await reader.getEntries();
                const imageEntries = entries
                    .filter((e) => !e.directory && e.filename.endsWith('.dcm') && e.getData)
                    .sort((a, b) => a.filename.localeCompare(b.filename)) as FileEntry[];

                const instances: DicomInstanceSource[] = await Promise.all(
                    imageEntries.map(async (entry) => {
                        const data = await entry.getData!(new BlobWriter());
                        return {
                            name: entry.filename,
                            getBytes: async () => data,
                        };
                    }),
                );
                return { instances };
            } finally {
                await reader.close();
            }
        },
    };

    if (options.fetchThumbnail) {
        source.getThumbnailUrl = options.fetchThumbnail;
    }

    return source;
}
