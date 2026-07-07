import * as CornerstoneCore from '@cornerstonejs/core';
import * as cornerstoneTools from '@cornerstonejs/tools';
import dicomLoader from '@cornerstonejs/dicom-image-loader/imageLoader';
import { configure as configureZip } from '@zip.js/zip.js';

let initialized = false;

/**
 * Runs the one-time global initialization required by Cornerstone, its DICOM image
 * loader, the tools system, and zip.js web workers. Safe to call repeatedly and from
 * multiple component mounts; the work happens only once per page.
 */
export function ensureCornerstoneInitialized(): void {
    if (initialized) {
        return;
    }
    CornerstoneCore.init();
    dicomLoader.init();
    cornerstoneTools.init();
    configureZip({ useWebWorkers: true });
    initialized = true;
}
