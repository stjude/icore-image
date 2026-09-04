import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import dts from 'vite-plugin-dts';

export default defineConfig({
    plugins: [react(), dts({ rollupTypes: true, tsconfigPath: './tsconfig.json' })],
    build: {
        lib: {
            entry: 'src/index.ts',
            formats: ['es'],
            fileName: 'index',
        },
        cssCodeSplit: false,
        rollupOptions: {
            // Externalize every bare/absolute import (react, react/jsx-runtime, @mantine/*,
            // @cornerstonejs/* subpaths, dicom-parser, @zip.js/zip.js, react-resize-detector,
            // @tabler/icons-react). Only local source and the CSS module get bundled.
            external: (id) => !id.startsWith('.') && !id.startsWith('/'),
            output: {
                // The whole package is client-only (a browser DICOM viewer); mark the
                // emitted bundle as a Next.js App Router client module.
                banner: '"use client";',
                assetFileNames: 'dicom-viewer.css',
            },
        },
    },
});
