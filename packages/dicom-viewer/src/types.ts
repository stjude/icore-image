import type { DataSet } from 'dicom-parser';
import type { IImage } from '@cornerstonejs/core/types';
import type { DicomDataSource } from './dataSource';

export type OverlayCorner = 'topLeft' | 'topRight' | 'bottomLeft' | 'bottomRight';

/**
 * A per-frame attribute nested inside the Per-Frame / Shared Functional Groups
 * Sequences of a multi-frame instance: `sequenceTag` selects the nested
 * sequence, `attributeTag` the attribute within it.
 */
export interface FrameAttributeTag {
    sequenceTag: string;
    attributeTag: string;
    parseType?: 'double';
}

/** Everything an overlay item needs to compute its displayed value. */
export interface OverlayContext {
    /** dicom-parser DataSet of the currently displayed image. */
    dataset: DataSet;
    /** Cornerstone image, for values derived from pixel geometry (FOV, matrix). */
    image: IImage;
    /** 0-based frame within a multi-frame instance; 0 for single-frame. */
    frameIndex: number;
    isMultiFrame: boolean;
    /** Total images (frames) in the currently displayed series stack. */
    imageCount: number;
    modality: string;
    voi: { windowWidth: number; windowCenter: number } | null;
    /** Resolve a raw string value by keyword ("KVP") or hex tag ("x00180060"). */
    getAttribute: (keywordOrTag: string) => string | undefined;
}

export interface OverlayItem {
    /** Keyword ("KVP") or hex tag ("x00180060"). Optional when `format` supplies the value. */
    attribute?: string;
    /** Prefix shown as "label: value". Pass '' (or omit with no attribute) for a value-only line. */
    label?: string;
    /** Appended to the resolved value, e.g. " mm", " kVp". */
    unit?: string;
    /** Render only when the image's Modality (0008,0060) is one of these. */
    modalities?: string[];
    /** For multi-frame images, pull from per-frame → shared functional groups before `attribute`. */
    frame?: FrameAttributeTag;
    /** Compute/format the displayed value. Receives the resolved raw value and full context. */
    format?: (value: string | undefined, ctx: OverlayContext) => string | null | undefined;
}

export type OverlayConfig = Partial<Record<OverlayCorner, OverlayItem[]>>;

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
    /**
     * Which DICOM attributes to display in each viewport corner. Defaults to
     * DEFAULT_OVERLAYS, which reproduces the built-in metadata overlays.
     */
    overlays?: OverlayConfig;
}
