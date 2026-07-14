import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The QC viewer is served by Django from deid/static/qc-viewer/ and loaded into
// the progress page via <script type="module">. This is an *application* build
// (not a library build): every dependency the vendored @stjude/dicom-viewer
// externalizes — React, Mantine, Cornerstone, etc. — is bundled here from a
// single copy in node_modules, so React/Mantine resolve to one instance and
// there is no "Invalid hook call".
export default defineConfig({
    // All emitted chunk/worker/WASM URLs must resolve under Django's static URL.
    base: '/static/qc-viewer/',
    build: {
        outDir: '../static/qc-viewer',
        emptyOutDir: true,
        target: 'es2020',
        // Keep all CSS in one asset with a stable name so the Django template can
        // reference it with a plain {% static %} tag.
        cssCodeSplit: false,
        rollupOptions: {
            input: 'src/main.tsx',
            output: {
                entryFileNames: 'qc-viewer.js',
                chunkFileNames: 'assets/[name]-[hash].js',
                assetFileNames: (assetInfo) => {
                    if (assetInfo.name && assetInfo.name.endsWith('.css')) {
                        return 'qc-viewer.css';
                    }
                    return 'assets/[name]-[hash][extname]';
                },
            },
        },
    },
    // Cornerstone's DICOM image loader and zip.js run in module workers.
    worker: { format: 'es' },
    plugins: [react()],
});
