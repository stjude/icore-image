import { useEffect, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { MantineProvider } from '@mantine/core';
import { ViewerCornerstone, type DicomDataSource, type ViewerStudy } from '@stjude/dicom-viewer';

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
    };
}

function QcViewerApp({ projectId }: { projectId: string }) {
    const [studies, setStudies] = useState<ViewerStudy[] | null>(null);
    const [error, setError] = useState<string | null>(null);
    // Build the data source once so its identity is stable across renders.
    const [dataSource] = useState(() => buildDataSource(projectId));

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
            onError={(e) => console.error('DICOM viewer error', e)}
        />
    );
}

// Track roots per container so a re-triggered mount is a no-op rather than a
// second React tree on the same node.
const roots = new Map<HTMLElement, Root>();

// Mantine's Tabs.List wraps its pills (flex-wrap: wrap) and the viewer sizes it
// to width:25%, which makes the two tabs stack vertically. Keep them on one row.
const OVERRIDES = '[role="tablist"]{flex-wrap:nowrap !important;width:auto !important;}';

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
