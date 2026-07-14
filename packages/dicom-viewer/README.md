# @stjude/dicom-viewer

A self-contained React DICOM viewer + metadata table built on Cornerstone.js, distributed
as a prebuilt npm package for same-stack (React 18 + Mantine v7 + Cornerstone.js) apps.

## What's in here

- `ViewerCornerstone` — a stack viewer with series sidebar, slice slider, window/level
  (mouse drag + `1`/`2`/`3` presets), multi-frame support, and a metadata tab.
- `DicomMetadataTable` — a full DICOM tag dump table (uses the bundled `DicomDataDictionary`).
- `createZipDataSource` — helper to build a data source from per-series zip blobs.
- `ensureCornerstoneInitialized` — idempotent global Cornerstone/zip.js init.

## Installing

This package is distributed as a **prebuilt, packed tarball**
(`stjude-dicom-viewer-<version>.tgz`, committed at the package root). Install that tarball via
a `file:` dependency — **not** the directory. Installing the tarball makes npm extract a real
copy into `node_modules/@stjude/dicom-viewer` with no nested `node_modules`, so React,
Mantine, and Cornerstone resolve to **your app's** single copies. (A directory `file:` dep
symlinks instead, and the package's own build-time `node_modules` then shadow those
singletons, producing duplicate React/Mantine instances — `Invalid hook call` or
`MantineProvider was not found in component tree`. The tarball avoids this with no bundler
config.)

```jsonc
// consumer package.json (reference the exact versioned tarball filename)
"dependencies": {
    "@stjude/dicom-viewer": "file:../path/to/packages/dicom-viewer/stjude-dicom-viewer-0.1.1.tgz"
}
```

Provide the **peer dependencies** in the consumer (kept as singletons): `react` `react-dom`
`@mantine/core` (^7.15) `@cornerstonejs/core` `@cornerstonejs/tools`
`@cornerstonejs/dicom-image-loader` (all ^4.15) `@tabler/icons-react` (^3). Its runtime
`dependencies` (`dicom-parser`, `@zip.js/zip.js`, `react-resize-detector`) are installed and
hoisted by npm. It does **not** depend on `jotai`, `next`, or `@mantine/notifications`.

Then, in the consuming app:

1. Import the stylesheet **once** in your root layout (after the Mantine core styles):

   ```ts
   import '@mantine/core/styles.css';
   import '@stjude/dicom-viewer/styles.css';
   ```

2. Wrap your app in a Mantine `<MantineProvider>` (the components use Mantine theming).
3. Ensure your bundler can resolve **web workers** for `@zip.js/zip.js` and
   `@cornerstonejs/dicom-image-loader`. Without workers, compressed DICOM transfer
   syntaxes (JPEG2000, etc.) will not decode. With webpack you typically also need:

   ```js
   config.resolve.fallback = { fs: false, module: false, worker_threads: false };
   ```

## Rebuilding / re-vendoring the tarball

The committed `stjude-dicom-viewer-<version>.tgz` is a build artifact. Each re-vendor **bumps
the version** so the tarball filename changes: npm keys its lockfile by the `file:` spec, so
reusing the same filename would leave a stale integrity and break `npm ci`. A new versioned
filename forces a fresh, correct lockfile entry.

In this repo, run:

```sh
task pack:dicom_viewer
```

which bumps the patch version, rebuilds + repacks, repoints the frontend's dependency at the
new tarball, and refreshes the frontend lockfile. Then commit together: the new
`packages/dicom-viewer/stjude-dicom-viewer-<version>.tgz`, `packages/dicom-viewer/package.json`
(+ its `package-lock.json`), and the consumer's `package.json` + `package-lock.json`. The old
versioned tarball is removed by the task, so `git` will show it deleted.

To do it by hand: `npm version patch --no-git-tag-version && npm run pack` in the package,
then update the consumer's `file:` reference to the new filename and run `npm install` there.

## Usage

The viewer never talks to your backend directly — you supply a `DicomDataSource` that
describes how to fetch DICOM bytes for a series (and, optionally, a thumbnail).

### Streaming zips from an HTTP API

```tsx
import { ViewerCornerstone, createZipDataSource } from '@stjude/dicom-viewer';

const dataSource = createZipDataSource({
    fetchZip: (series, { onProgress, signal }) =>
        downloadBlob(`/api/image-series/${series.id}/download/`, { onProgress, signal }),
    fetchThumbnail: async (series) => {
        const res = await fetch(`/api/image-series/${series.id}/thumbnail/`);
        return res.ok ? URL.createObjectURL(await res.blob()) : null;
    },
});

<ViewerCornerstone studies={studies} dataSource={dataSource} onError={console.error} />;
```

### Loading files from disk

No zip, no thumbnail — just wrap the `File` objects:

```tsx
const diskDataSource = {
    loadSeries: async (series) => ({
        instances: filesForSeries(series).map((file) => ({
            name: file.name,
            getBytes: async () => file,
        })),
    }),
};

<ViewerCornerstone studies={studies} dataSource={diskDataSource} />;
```

### Configuring overlays

The metadata shown in the viewport corners is configurable via the `overlays` prop. Each of the four
corners (`topLeft`, `topRight`, `bottomLeft`, `bottomRight`) takes a list of items. An item names a DICOM
attribute — by keyword (`"KVP"`, `"EchoTime"`) or hex tag (`"x00180060"`) — plus an optional `label`,
`unit`, `modalities` filter, and `format` hook for computed values. Items with no value (or a non-matching
modality) are omitted.

Omitting the prop uses `DEFAULT_OVERLAYS`, which reproduces the built-in overlays. Import it to extend
rather than replace the defaults.

```tsx
import { ViewerCornerstone, DEFAULT_OVERLAYS } from '@stjude/dicom-viewer';

<ViewerCornerstone
    studies={studies}
    dataSource={dataSource}
    overlays={{
        topLeft: [
            { attribute: 'InstanceNumber', label: 'Instance' },
            { attribute: 'PatientID', label: 'MRN' },
            // Multi-frame per-frame lookup with a shared-attribute fallback, CT only:
            {
                label: 'KVP',
                unit: ' kVp',
                modalities: ['CT'],
                attribute: 'x00180060',
                frame: { sequenceTag: 'x00189325', attributeTag: 'x00180060' },
            },
            // Computed value via the format hook:
            {
                label: 'WW/WL',
                format: (_v, ctx) =>
                    ctx.voi ? `${Math.round(ctx.voi.windowWidth)} / ${Math.round(ctx.voi.windowCenter)}` : null,
            },
        ],
        bottomRight: DEFAULT_OVERLAYS.bottomRight,
    }}
/>;
```

## Notes / gotchas

- **Multi-frame preload workaround**: the loader pre-loads the first frame of every
  multi-frame instance (Cornerstone3D PR #2260) before enabling lazy prefetch. Do not
  remove this.
- **The Cornerstone file manager is never purged** between series on purpose — purging
  causes stale images to render due to colliding internal image IDs. As a result the
  file manager accumulates blobs for the viewer's lifetime.
- **Version skew**: pin Mantine to v7 and Cornerstone to ^4.15 — mismatches break silently.
- Each mounted viewer uses unique Cornerstone engine/viewport/tool-group IDs, so multiple
  viewers can coexist on one page. Pass `initCornerstone={false}` if your app already runs
  the global Cornerstone init elsewhere.
