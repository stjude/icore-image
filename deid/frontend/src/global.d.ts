/// <reference types="vite/client" />

// Side-effect CSS imports (@mantine/core/styles.css, @stjude/dicom-viewer/styles.css).
declare module '*.css';

interface QcViewerMountOptions {
    projectId: string;
}

declare global {
    interface Window {
        mountQcViewer?: (container: HTMLElement, options: QcViewerMountOptions) => void;
        unmountQcViewer?: (container: HTMLElement) => void;
    }
}

export {};
