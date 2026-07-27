import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { MantineProvider } from '@mantine/core';
import {
    ViewerCornerstone,
    type DicomDataSource,
    type OverlayConfig,
    type OverlayContext,
    type OverlayItem,
    type ViewerStudy,
} from '@stjude/dicom-viewer';

// Mantine core styles must load before the viewer's own styles. Vite emits both
// into qc-viewer.css; we do NOT let it apply to the whole document (Mantine's
// reset targets body/button/etc. and would clobber the host Django page).
// Instead the bundle injects that CSS into a shadow root at mount time (see
// mountQcViewer), and these imports keep the emitted stylesheet in the build.
import '@mantine/core/styles.css';
import '@stjude/dicom-viewer/styles.css';

// URL of the emitted stylesheet, resolved next to this module regardless of the
// static base path. Fetched and injected into the viewer's shadow root.
const STYLESHEET_URL = new URL('./qc-viewer.css', import.meta.url).href;

// Shared bottom-right items reused across modalities. Slice thickness prefers the
// per-frame value (multi-frame) and falls back to the top-level SliceThickness.
const sliceThicknessSpacing: OverlayItem = {
    label: 'Thickness/Spacing',
    attribute: 'SliceThickness',
    frame: { sequenceTag: 'x00289110', attributeTag: 'x00180050' },
    format: (thk, ctx) => `${thk ?? 'N/A'} mm / ${ctx.getAttribute('SpacingBetweenSlices') ?? 'N/A'} mm`,
};

const fieldOfView: OverlayItem = {
    label: 'FOV',
    format: (_v, ctx) =>
        `${(ctx.image.columnPixelSpacing * ctx.image.columns).toFixed(0)} x ${(ctx.image.rowPixelSpacing * ctx.image.rows).toFixed(0)} mm`,
};

// Projection-radiography modality codes that share the same X-ray technique overlay.
const XRAY_MODALITIES = ['CR', 'DX', 'XA', 'RF', 'MG'];

// PET radiopharmaceutical values. In source data these often live only in
// RadiopharmaceuticalInformationSequence (0054,0016), but HIPAA Safe Harbor de-id
// removes that sequence (it embeds a datetime) while keeping the top-level scalar
// tags. Prefer the top-level tag; fall back to the sequence for non-de-id'd data.
const radiopharmaceutical = (ctx: OverlayContext, topLevelTag: string, seqTag: string): string | null =>
    ctx.getAttribute(topLevelTag) ??
    ctx.dataset.elements.x00540016?.items?.[0]?.dataSet?.string(seqTag) ??
    null;

// QC overlay layout. Attributes are referenced by DICOM keyword; the bottom-right
// corner is modality-gated (CT vs MR) via each item's `modalities` filter.
const QC_OVERLAYS: OverlayConfig = {
    topLeft: [
        { label: 'Patient', attribute: 'PatientName' },
        { label: 'MRN', attribute: 'PatientID' },
        { label: 'Acc', attribute: 'AccessionNumber' },
        { label: 'DOB', attribute: 'PatientBirthDate' },
        { label: 'Age', attribute: 'PatientAge' },
        { label: 'Sex', attribute: 'PatientSex' },
    ],
    topRight: [
        { label: 'Institution', attribute: 'InstitutionName' },
        { label: 'Manufacturer', attribute: 'Manufacturer' },
        { label: 'Model', attribute: 'ManufacturerModelName' },
        { label: 'Protocol', attribute: 'ProtocolName' },
    ],
    bottomLeft: [
        { label: 'Study', attribute: 'StudyDescription' },
        { label: 'Series', attribute: 'SeriesDescription' },
        { label: 'Series Date', attribute: 'SeriesDate' },
        { label: 'Images', format: (_v, ctx) => String(ctx.imageCount) },
    ],
    bottomRight: [
        { label: 'Modality', attribute: 'Modality' },
        // CT
        { ...sliceThicknessSpacing, modalities: ['CT'] },
        { ...fieldOfView, modalities: ['CT'] },
        {
            label: 'KVP',
            unit: ' kVp',
            modalities: ['CT'],
            attribute: 'x00180060',
            frame: { sequenceTag: 'x00189325', attributeTag: 'x00180060' },
        },
        {
            label: 'Tube Current',
            unit: ' mA',
            modalities: ['CT'],
            attribute: 'x00181151',
            frame: { sequenceTag: 'x00189321', attributeTag: 'x00189330' },
        },
        // MR
        { ...sliceThicknessSpacing, modalities: ['MR'] },
        { ...fieldOfView, modalities: ['MR'] },
        { label: 'Echo Train Length', attribute: 'EchoTrainLength', modalities: ['MR'] },
        {
            label: 'TR',
            unit: ' ms',
            modalities: ['MR'],
            attribute: 'x00180080',
            frame: { sequenceTag: 'x00189112', attributeTag: 'x00180080' },
        },
        {
            label: 'TE',
            unit: ' ms',
            modalities: ['MR'],
            attribute: 'x00180081',
            frame: { sequenceTag: 'x00189114', attributeTag: 'x00189082', parseType: 'double' },
        },
        // PET
        { ...sliceThicknessSpacing, modalities: ['PT'] },
        { ...fieldOfView, modalities: ['PT'] },
        { label: 'Tracer', modalities: ['PT'], format: (_v, ctx) => radiopharmaceutical(ctx, 'x00180031', 'x00180031') },
        { label: 'Dose', unit: ' Bq', modalities: ['PT'], format: (_v, ctx) => radiopharmaceutical(ctx, 'x00181074', 'x00181074') },
        { label: 'Half-life', unit: ' s', modalities: ['PT'], format: (_v, ctx) => radiopharmaceutical(ctx, 'x00181075', 'x00181075') },
        { label: 'Units', attribute: 'Units', modalities: ['PT'] },
        { label: 'Corrections', attribute: 'CorrectedImage', modalities: ['PT'] },
        { label: 'Frame Duration', attribute: 'ActualFrameDuration', unit: ' ms', modalities: ['PT'] },
        // X-ray (projection radiography)
        { label: 'kVp', attribute: 'KVP', modalities: XRAY_MODALITIES },
        { label: 'mAs', attribute: 'Exposure', modalities: XRAY_MODALITIES },
        { label: 'Exposure Time', attribute: 'ExposureTime', unit: ' ms', modalities: XRAY_MODALITIES },
        { label: 'Tube Current', attribute: 'XRayTubeCurrent', unit: ' mA', modalities: XRAY_MODALITIES },
        { label: 'SID', attribute: 'DistanceSourceToDetector', unit: ' mm', modalities: XRAY_MODALITIES },
        { label: 'View', attribute: 'ViewPosition', modalities: XRAY_MODALITIES },
        { label: 'Body Part', attribute: 'BodyPartExamined', modalities: XRAY_MODALITIES },
        { label: 'Filter', attribute: 'FilterMaterial', modalities: XRAY_MODALITIES },
        { label: 'Laterality', attribute: 'ImageLaterality', modalities: ['MG'] },
        { label: 'Compression', attribute: 'CompressionForce', unit: ' N', modalities: ['MG'] },
        // Ultrasound (no FOV — spacing is per-region)
        { label: 'Transducer', attribute: 'TransducerData', modalities: ['US'] },
        { label: 'Type', attribute: 'TransducerType', modalities: ['US'] },
        { label: 'MI', attribute: 'MechanicalIndex', modalities: ['US'] },
        { label: 'TIs', attribute: 'SoftTissueThermalIndex', modalities: ['US'] },
        { label: 'Frame Rate', attribute: 'CineRate', unit: ' fps', modalities: ['US'] },
        { label: 'Heart Rate', attribute: 'HeartRate', unit: ' bpm', modalities: ['US'] },
        { label: 'Processing', attribute: 'ProcessingFunction', modalities: ['US'] },
    ],
};

interface InstanceRef {
    name: string;
    url: string;
}

/**
 * Build a filesystem-backed data source. The viewer never touches the backend
 * directly: for each series it asks us for the ordered instance list, then pulls
 * each instance's bytes lazily as Cornerstone prefetches the stack. Bytes come
 * straight from the project's de-identified output directory via the /api/qc
 * endpoints (see deid/home/views.py).
 */
function buildDataSource(projectId: string): DicomDataSource {
    return {
        async loadSeries(series, options) {
            const listUrl = encodeURI(
                `/api/qc/${projectId}/series/${series.id}/instances/`,
            );
            const res = await fetch(listUrl, { signal: options?.signal });
            if (!res.ok) {
                throw new Error(`Failed to list instances for series ${series.id}`);
            }
            const { instances } = (await res.json()) as { instances: InstanceRef[] };
            return {
                instances: instances.map((inst) => ({
                    name: inst.name,
                    getBytes: async () => {
                        const r = await fetch(inst.url, { signal: options?.signal });
                        if (!r.ok) {
                            throw new Error(`Failed to fetch instance ${inst.name}`);
                        }
                        return r.blob();
                    },
                })),
            };
        },
        // Middle-slice preview PNG rendered during de-identification (served
        // from appdata). Returns an object URL the viewer revokes on unmount, or
        // null when the series has no thumbnail (shows "No Preview").
        async getThumbnailUrl(series) {
            const res = await fetch(
                encodeURI(`/api/qc/${projectId}/series/${series.id}/thumbnail/`),
            );
            if (!res.ok) {
                return null;
            }
            return URL.createObjectURL(await res.blob());
        },
    };
}

function QcViewerApp({ projectId }: { projectId: string }) {
    const [studies, setStudies] = useState<ViewerStudy[] | null>(null);
    const [error, setError] = useState<string | null>(null);
    // The viewer can mount before per-series preview thumbnails have finished
    // generating. The host page (task_progress.html) fires
    // `qc-thumbnails-ready` once the thumbnails marker appears; bumping this
    // epoch rebuilds the data source, whose new identity makes the viewer
    // re-run its thumbnail fetch and backfill the previews. The marker is
    // written only after *all* thumbnails complete, so one re-fetch fills them
    // in together.
    const [thumbnailEpoch, setThumbnailEpoch] = useState(0);
    useEffect(() => {
        const onReady = () => setThumbnailEpoch((n) => n + 1);
        window.addEventListener('qc-thumbnails-ready', onReady);
        return () => window.removeEventListener('qc-thumbnails-ready', onReady);
    }, []);
    // Rebuilt only when the thumbnail epoch changes; its identity is otherwise
    // stable, so nothing but the thumbnail fetch reacts to it.
    const dataSource = useMemo(
        () => buildDataSource(projectId),
        [projectId, thumbnailEpoch],
    );

    // Track which series the operator has reviewed. A series counts once its
    // images have been displayed AND its Metadata tab opened. We report the
    // running count to the host page (task_progress.html) via a window event so
    // it can gate the Approve button.
    const reviewRef = useRef<Map<string, { displayed: boolean; metadata: boolean }>>(new Map());

    const emitReviewProgress = useCallback(() => {
        const totalSeries = (studies ?? []).reduce((n, s) => n + s.series.length, 0);
        const required = Math.min(2, totalSeries);
        let reviewedCount = 0;
        for (const flags of reviewRef.current.values()) {
            if (flags.displayed && flags.metadata) reviewedCount += 1;
        }
        window.dispatchEvent(
            new CustomEvent('qc-review-progress', { detail: { reviewedCount, required } }),
        );
    }, [studies]);

    const markReviewed = useCallback(
        (seriesId: string, key: 'displayed' | 'metadata') => {
            const flags = reviewRef.current.get(seriesId) ?? { displayed: false, metadata: false };
            if (flags[key]) return; // no change
            flags[key] = true;
            reviewRef.current.set(seriesId, flags);
            emitReviewProgress();
        },
        [emitReviewProgress],
    );

    // Emit the initial 0 / N so the page shows the requirement immediately.
    useEffect(() => {
        if (studies) emitReviewProgress();
    }, [studies, emitReviewProgress]);

    useEffect(() => {
        const controller = new AbortController();
        fetch(`/api/qc/${projectId}/studies/`, { signal: controller.signal })
            .then((r) => {
                if (!r.ok) {
                    throw new Error('Failed to load de-identified studies');
                }
                return r.json() as Promise<{ studies: ViewerStudy[] }>;
            })
            .then((data) => setStudies(data.studies))
            .catch((e: unknown) => {
                if (e instanceof Error && e.name === 'AbortError') {
                    return;
                }
                setError(String(e));
            });
        return () => controller.abort();
    }, [projectId]);

    if (error) {
        return <div style={{ padding: 16, color: '#b91c1c' }}>QC viewer error: {error}</div>;
    }
    if (!studies) {
        return <div style={{ padding: 16 }}>Loading de-identified images…</div>;
    }
    if (studies.length === 0) {
        return <div style={{ padding: 16 }}>No de-identified images found in the output directory.</div>;
    }

    return (
        <ViewerCornerstone
            studies={studies}
            dataSource={dataSource}
            overlays={QC_OVERLAYS}
            onError={(e) => console.error('DICOM viewer error', e)}
            onSeriesDisplayed={(seriesId) => markReviewed(seriesId, 'displayed')}
            onMetadataViewed={(seriesId) => markReviewed(seriesId, 'metadata')}
        />
    );
}

// Track roots per container so a re-triggered mount is a no-op rather than a
// second React tree on the same node.
const roots = new Map<HTMLElement, Root>();

// Mantine's Tabs.List wraps its pills (flex-wrap: wrap) and the viewer sizes it
// to width:25%, which makes the two tabs stack vertically. Keep them on one row.
const OVERRIDES = '[role="tablist"]{flex-wrap:nowrap !important;width:auto !important; margin-top: 0.5rem;}';

// Inject the emitted stylesheet into a shadow root, rewriting :root -> :host so
// Mantine's CSS variables (defined on :root in the sheet, including the
// color-scheme-gated :root[data-mantine-color-scheme=light] block) land on the
// shadow host and cascade into the viewer. Shadow scoping keeps Mantine's global
// body/button resets from leaking onto the host page.
async function injectStyles(shadow: ShadowRoot) {
    try {
        const res = await fetch(STYLESHEET_URL);
        const css = (await res.text()).replace(/:root/g, ':host');
        const style = document.createElement('style');
        style.textContent = css + OVERRIDES;
        shadow.appendChild(style);
    } catch (e) {
        console.error('Failed to load QC viewer styles', e);
    }
}

window.mountQcViewer = (container, { projectId }) => {
    if (roots.has(container)) {
        return;
    }
    // The scheme-gated Mantine variables live under
    // :host[data-mantine-color-scheme=light]; the host must carry the attribute
    // for those (the "filled" colors used by the slider, its tooltip, and the
    // active tab pill) to resolve.
    container.setAttribute('data-mantine-color-scheme', 'light');
    const shadow = container.attachShadow({ mode: 'open' });
    void injectStyles(shadow);
    const appRoot = document.createElement('div');
    shadow.appendChild(appRoot);

    const root = createRoot(appRoot);
    roots.set(container, root);
    root.render(
        // Variables come from the injected sheet (:host), so Mantine needn't
        // also inject them; force light scheme to match the static values.
        <MantineProvider defaultColorScheme="light" withCssVariables={false}>
            <QcViewerApp projectId={projectId} />
        </MantineProvider>,
    );
};

window.unmountQcViewer = (container) => {
    const root = roots.get(container);
    if (root) {
        root.unmount();
        roots.delete(container);
    }
};

// Signal the host page that the mount API is available. The progress page may
// have already reached COMPLETED (and stopped polling) before this deferred
// module finished loading, so it retries the mount on this event.
window.dispatchEvent(new Event('qc-viewer-ready'));
