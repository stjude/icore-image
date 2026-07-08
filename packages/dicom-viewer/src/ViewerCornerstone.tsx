import { useResizeDetector } from 'react-resize-detector';
import { useEffect, useState, useRef, useCallback, useId } from 'react';
import {
    Progress,
    Paper,
    Text,
    Badge,
    ScrollArea,
    Group,
    Stack,
    Collapse,
    ActionIcon,
    Slider,
    Tabs,
    Box,
} from '@mantine/core';
import { IconChevronDown, IconChevronRight, IconPhoto } from '@tabler/icons-react';
import { DataSet, parseDicom } from 'dicom-parser';

import * as CornerstoneCore from '@cornerstonejs/core';
import { Enums as CornerstoneEnums } from '@cornerstonejs/core';
import type { IStackViewport, IImage } from '@cornerstonejs/core/types';
import dicomLoader from '@cornerstonejs/dicom-image-loader/imageLoader';
import * as cornerstoneTools from '@cornerstonejs/tools';
import { MouseBindings } from '@cornerstonejs/tools/enums';

import DicomMetadataTable from './DicomMetadataTable';
import { ensureCornerstoneInitialized } from './cornerstone';
import type { ViewerProps, ViewerSeries } from './types';
import classes from './viewer.module.css';

const isMultiFrameDicom = (dataSet: DataSet): boolean => {
    const numberOfFrames = parseInt(dataSet.string('x00280008')!);
    return numberOfFrames !== undefined && numberOfFrames > 1;
};

/**
 * Release a previously displayed series so viewer memory does not grow without
 * bound. Cornerstone's decoded-image cache and the wadouri fileManager are
 * global singletons that are never freed on their own, so every opened series
 * otherwise stays resident for the page's lifetime (hundreds of MB per series).
 *
 * We remove only THIS series' entries — never `cache.purgeCache()`, which would
 * also evict images belonging to other viewers on the page — and we do NOT
 * reset the fileManager counter: reusing indices would collide with any
 * residual cache entries (the stale-render bug that previously forced us to
 * leak instead of purge).
 */
function releaseSeries(imageIds: string[], fileIndices: number[]): void {
    // removeImageLoadObject exists on the concrete cache but is absent from the
    // exported ICache type, so reach it through a narrow cast.
    const cache = CornerstoneCore.cache as unknown as {
        getImageLoadObject: (imageId: string) => unknown;
        removeImageLoadObject: (imageId: string, options?: { force?: boolean }) => void;
    };
    for (const imageId of imageIds) {
        try {
            if (cache.getImageLoadObject(imageId)) {
                cache.removeImageLoadObject(imageId, { force: true });
            }
        } catch {
            // Already evicted — safe to ignore.
        }
    }
    for (const index of fileIndices) {
        try {
            dicomLoader.wadouri.fileManager.remove(index);
        } catch {
            // Already removed — safe to ignore.
        }
    }
}

/** Parse the numeric fileManager index out of a `dicomfile:<n>` image id. */
const fileIndexOf = (baseImageId: string): number => Number(baseImageId.split(':')[1]);

interface WLPreset {
    key: string;
    name: string;
    windowWidth: number;
    windowCenter: number;
}

const WL_PRESETS: WLPreset[] = [
    { key: '1', name: 'Soft tissue', windowWidth: 400, windowCenter: 40 },
    { key: '2', name: 'Lung', windowWidth: 1500, windowCenter: -600 },
    { key: '3', name: 'Bone', windowWidth: 1800, windowCenter: 400 },
];

const voiRangeFromWindowLevel = (windowWidth: number, windowCenter: number) => ({
    lower: windowCenter - windowWidth / 2,
    upper: windowCenter + windowWidth / 2,
});

const windowLevelFromVoiRange = ({ lower, upper }: { lower: number; upper: number }) => ({
    windowWidth: upper - lower,
    windowCenter: (upper + lower) / 2,
});

interface FrameAttributeTag {
    sequenceTag: string;
    attributeTag: string;
    parseType?: 'double';
}

/**
 * Looks up a specific frame attribute from the per-frame functional groups sequence,
 * falling back to the shared functional groups sequence if not found.
 */
const lookupFrameAttribute = (dataset: DataSet, frameIndex: number, frameAttributeTag: FrameAttributeTag) => {
    const perFrameSequence = dataset.elements.x52009230;
    if (!perFrameSequence || !perFrameSequence.items || frameIndex >= perFrameSequence.items.length) {
        console.error('Per-frame Functional Groups Sequence not found or frame index out of bounds');
        return;
    }
    const frameData = perFrameSequence.items[frameIndex];
    const sequenceElement = frameData?.dataSet?.elements?.[frameAttributeTag.sequenceTag];
    if (sequenceElement?.items?.[0]?.dataSet) {
        let value;
        if (frameAttributeTag.parseType === 'double') {
            value = sequenceElement.items[0].dataSet.double(frameAttributeTag.attributeTag);
        } else {
            value = sequenceElement.items[0].dataSet.string(frameAttributeTag.attributeTag);
        }
        if (value) return value;
    }
    // Fallback to shared functional group sequence
    const sharedSequence = dataset.elements.x52009229;
    if (sharedSequence?.items?.[0]?.dataSet) {
        const sharedElement = sharedSequence.items[0].dataSet.elements?.[frameAttributeTag.sequenceTag];
        if (sharedElement?.items?.[0]?.dataSet) {
            let value;
            if (frameAttributeTag.parseType === 'double') {
                value = sharedElement.items[0].dataSet.double(frameAttributeTag.attributeTag);
            } else {
                value = sharedElement.items[0].dataSet.string(frameAttributeTag.attributeTag);
            }
            if (value) return value;
        }
    }
};

const getPerFrameAttributes = (dataSet: DataSet, frameIndex: number) => {
    // Define the attributes to extract along with their sequence and attribute tags
    const attributeTags: Record<string, FrameAttributeTag> = {
        sliceThickness: { sequenceTag: 'x00289110', attributeTag: 'x00180050' },
        kvp: { sequenceTag: 'x00189325', attributeTag: 'x00180060' },
        xRayTubeCurrent: { sequenceTag: 'x00189321', attributeTag: 'x00189330' },
        repetitionTime: { sequenceTag: 'x00189112', attributeTag: 'x00180080' },
        echoTime: { sequenceTag: 'x00189114', attributeTag: 'x00189082', parseType: 'double' },
    };
    const attributes: Record<string, string | number | undefined> = {};
    for (const key in attributeTags) {
        const value = lookupFrameAttribute(dataSet, frameIndex, attributeTags[key]);
        if (value) {
            attributes[key] = value;
        }
    }
    return attributes;
};

/**
 * Given a base image ID and number of frames, generates an array of image IDs for each frame.
 * These IDs are used to reference the individual frames within the cornerstone WADO URI image
 * loader.
 */
const createMultiFrameImageIds = (baseImageId: string, numberOfFrames: number): string[] => {
    const imageIds: string[] = [];
    if (numberOfFrames === 1) {
        imageIds.push(`${baseImageId}`);
        return imageIds;
    }

    for (let frame = 0; frame < numberOfFrames; frame++) {
        const frameImageId = `${baseImageId}?frame=${frame + 1}`;
        imageIds.push(frameImageId);
    }

    return imageIds;
};

export default function ViewerCornerstone({ studies, dataSource, onError, initCornerstone = true }: ViewerProps) {
    const [seriesThumbnails, setSeriesThumbnails] = useState<Record<string, string>>({});
    const [loadingThumbnails, setLoadingThumbnails] = useState<Record<string, boolean>>({});
    const [currentSeriesId, setCurrentSeriesId] = useState<string | null>(null);
    const [loadProgress, setLoadProgress] = useState(0);
    const [loadInProgress, setLoadInProgress] = useState(false);
    const [expandedStudies, setExpandedStudies] = useState<Record<string, boolean>>({});
    const [currentSliceIndex, setCurrentSliceIndex] = useState(0);
    const [canDisplaySeries, setCanDisplaySeries] = useState(true);
    const [totalSlices, setTotalSlices] = useState(0);
    const [currentDataset, setCurrentDataset] = useState<DataSet | null>(null);
    const [isCurrentSeriesMultiFrame, setIsCurrentSeriesMultiFrame] = useState(false);
    const [multiFrameInfo, setMultiFrameInfo] = useState<{
        totalFiles: number;
        totalFrames: number;
        framesPerFile: Record<string, number>;
    } | null>(null);
    const totalSlicesTracker = useRef(0);
    const [currentMetadata, setCurrentMetadata] = useState<{
        instanceNumber?: string;
        sliceThickness?: string;
        sliceSpacing?: string;
        seriesName?: string;
        imageComments?: string;
        FOV?: string;
        kvp?: string;
        current?: string;
        tr?: string;
        te?: string;
        sequenceName?: string;
        fieldStrength?: string;
        acquisitionMatrix?: string;
        modality?: string;
        imagePosition?: string;
        imageOrientation?: string;
        pixelSpacing?: string;
        stackId?: string;
    }>({});
    const [activeTab, setActiveTab] = useState<'viewer' | 'metadata'>('viewer');
    const [voi, setVoi] = useState<{ windowWidth: number; windowCenter: number } | null>(null);

    const containerRef = useRef<HTMLDivElement>(null);
    const thumbnailUrlsRef = useRef<Set<string>>(new Set());
    // The rendering engine created for this mount. Held in a ref (rather than looked up by ID
    // through Cornerstone's global registry on every call) so viewport access is cheap and
    // never races with mount ordering -- it is simply null until setup runs.
    const renderingEngineRef = useRef<CornerstoneCore.RenderingEngine | null>(null);

    // imageIds (incl. per-frame variants) and fileManager indices of the
    // currently displayed series, tracked so the series can be released from the
    // cache + fileManager when a new one is loaded or the viewer unmounts.
    const loadedImageIdsRef = useRef<string[]>([]);
    const loadedFileIndicesRef = useRef<number[]>([]);

    // Concurrency guard for loadSeries. Rapidly clicking series would otherwise
    // run multiple loads at once; their interleaved viewport.setStack() calls and
    // shared cache/fileManager corrupt the stack (slices from other series appear).
    // loadGenRef stamps each request; loadChainRef serializes them so only one
    // runs at a time and only the latest request actually renders.
    const loadGenRef = useRef(0);
    const loadChainRef = useRef<Promise<void>>(Promise.resolve());

    // Unique per-mount Cornerstone identifiers so multiple viewers can coexist on one page.
    const instanceId = useId().replace(/:/g, '');
    const renderingEngineId = `re-${instanceId}`;
    const viewportId = `vp-${instanceId}`;
    const toolGroupId = `tg-${instanceId}`;

    const handleError = useCallback(
        (error: unknown) => {
            if (onError) {
                onError(error);
            } else {
                console.error(error);
            }
        },
        [onError],
    );

    // Initialize all studies as expanded by default
    useEffect(() => {
        const initialExpanded = studies.reduce(
            (acc, study) => {
                acc[study.id] = true;
                return acc;
            },
            {} as Record<string, boolean>,
        );
        setExpandedStudies(initialExpanded);
    }, [studies]);

    const toggleStudy = (studyId: string) => {
        setExpandedStudies((prev) => ({
            ...prev,
            [studyId]: !prev[studyId],
        }));
    };

    const downloadThumbnail = useCallback(
        async (series: ViewerSeries): Promise<void> => {
            if (!dataSource.getThumbnailUrl) {
                return;
            }
            setLoadingThumbnails((prev) => ({ ...prev, [series.id]: true }));
            try {
                const url = await dataSource.getThumbnailUrl(series);
                if (url) {
                    thumbnailUrlsRef.current.add(url);
                    setSeriesThumbnails((prev) => ({ ...prev, [series.id]: url }));
                }
            } catch (error) {
                console.error(`Error downloading thumbnail for series ${series.id}:`, error);
            } finally {
                setLoadingThumbnails((prev) => ({ ...prev, [series.id]: false }));
            }
        },
        [dataSource],
    );

    const getViewport = useCallback(() => {
        return renderingEngineRef.current?.getViewport(viewportId) as IStackViewport | undefined;
    }, [viewportId]);

    // The actual load. Runs serialized via loadSeries(); bails early if a newer
    // request has superseded it so concurrent clicks can't corrupt the stack.
    const loadSeriesImpl = async (series: ViewerSeries, myGen: number): Promise<void> => {
        const superseded = () => myGen !== loadGenRef.current;
        if (superseded()) return;
        // NOTE: Interestingly, purging the file manager actually can cause images from previous series to
        // remain rendered in the viewport, presumably due to the internal image IDs being the same. Therefore,
        // DO NOT purge the file manager when loading a new series.
        // dicomLoader.wadouri.fileManager.purge();
        setLoadInProgress(true);
        setLoadProgress(0);

        let instances;
        try {
            const result = await dataSource.loadSeries(series, {
                onProgress: (fraction) => setLoadProgress(fraction === undefined ? 0 : Math.round(fraction * 100)),
            });
            instances = result.instances;
        } catch (error) {
            handleError(error);
            return;
        } finally {
            setLoadInProgress(false);
        }
        if (superseded()) return;

        const extracted = await Promise.all(
            instances.map(async (instance) => {
                const fileData = await instance.getBytes();
                const baseImageId = dicomLoader.wadouri.fileManager.add(fileData);
                return { filename: instance.name, fileData, baseImageId };
            }),
        );

        if (extracted.length === 0) {
            setCurrentSeriesId(series.id);
            setCanDisplaySeries(false);
            setTotalSlices(0);
            setCurrentSliceIndex(0);
            return;
        }

        const firstBytes = new Uint8Array(await extracted[0].fileData.arrayBuffer());
        const firstDataSet = parseDicom(firstBytes);
        const hasPixelData = firstDataSet.elements.x7fe00010 !== undefined;
        const numberOfFrames = isMultiFrameDicom(firstDataSet)
            ? parseInt(firstDataSet.string('x00280008')!) || 1
            : 1;

        setCanDisplaySeries(hasPixelData);
        if (!hasPixelData) {
            setCurrentSeriesId(series.id);
            setTotalSlices(1);
            setCurrentSliceIndex(0);
            console.warn('Series has no displayable data');
            return;
        }

        const hasMultiFrame = numberOfFrames > 1;
        const imageIds: string[] = [];
        // NOTE: To work around multi-frame indexing bugs during loading, we still need to load the _first_
        // slice first for each multi-frame instance to prevent the viewer from crashing.
        // See https://github.com/cornerstonejs/cornerstone3D/pull/2260
        const firstFrameIds: string[] = [];
        const framesPerFile: Record<string, number> = {};
        for (const { filename, baseImageId } of extracted) {
            if (hasMultiFrame) {
                const frameImageIds = createMultiFrameImageIds(baseImageId, numberOfFrames);
                firstFrameIds.push(frameImageIds[0]);
                imageIds.push(...frameImageIds);
                framesPerFile[filename] = frameImageIds.length;
            } else {
                imageIds.push(baseImageId);
                framesPerFile[filename] = 1;
            }
        }
        const totalFiles = extracted.length;
        const totalFramesCount = imageIds.length;
        const myFileIndices = extracted.map(({ baseImageId }) => fileIndexOf(baseImageId));

        // A newer series was requested while we were fetching: drop what we added
        // and bail so we never setStack over the newer load.
        if (superseded()) {
            releaseSeries(imageIds, myFileIndices);
            return;
        }

        const renderingEngine = renderingEngineRef.current;
        if (!renderingEngine) {
            console.error('Rendering engine is not initialized');
            releaseSeries(imageIds, myFileIndices);
            return;
        }
        const viewport = renderingEngine.getViewport(viewportId) as IStackViewport;
        await viewport.setStack(imageIds);
        if (superseded()) {
            releaseSeries(imageIds, myFileIndices);
            return;
        }
        viewport.resetProperties();
        viewport.render();

        // Workaround for cornerstone3D #2260: pre-load the first frame of each multi-frame instance.
        // This is the only upfront load we keep; everything else streams lazily via stackContextPrefetch.
        if (firstFrameIds.length > 0) {
            await Promise.all(firstFrameIds.map((imageId) => CornerstoneCore.imageLoader.loadAndCacheImage(imageId)));
        }

        cornerstoneTools.utilities.stackContextPrefetch.enable(viewport.element);

        // The new stack is now displayed, so free the previous series' cache and
        // fileManager entries. Doing this here (rather than before load) means we
        // never disturb the images the viewport is actively using.
        releaseSeries(loadedImageIdsRef.current, loadedFileIndicesRef.current);
        loadedImageIdsRef.current = imageIds;
        loadedFileIndicesRef.current = myFileIndices;

        setCurrentSeriesId(series.id);
        setTotalSlices(totalFramesCount);
        setCurrentSliceIndex(0);
        setIsCurrentSeriesMultiFrame(hasMultiFrame);
        setMultiFrameInfo({
            totalFiles,
            totalFrames: totalFramesCount,
            framesPerFile,
        });
    };

    // Public entry point: stamp a generation and serialize onto the load chain so
    // only one loadSeriesImpl runs at a time and only the latest click wins.
    const loadSeries = (series: ViewerSeries): void => {
        const myGen = ++loadGenRef.current;
        loadChainRef.current = loadChainRef.current.then(
            () => loadSeriesImpl(series, myGen),
            () => loadSeriesImpl(series, myGen),
        );
    };

    // This tracking ref is required due to how React caches event handlers. Certain events
    // (notably, drag events) will not grab the latest version of an event handler callback,
    // and thus we need to supply refs to these callbacks rather than rely on the update
    // mechanisms of `useCallback`. See
    // https://github.com/mantinedev/mantine/issues/7620#issuecomment-2778292861
    useEffect(() => {
        totalSlicesTracker.current = totalSlices;
    }, [totalSlices]);

    const handleSliderChange = useCallback(
        async (value: number) => {
            const viewport = getViewport();
            if (!viewport) {
                console.error('Viewport reference is not set');
                return;
            }

            const imageIds: string[] = viewport.getImageIds();
            if (imageIds && imageIds.length > 0) {
                // Use the actual number of images from the viewport to ensure consistency
                const actualTotalSlices = imageIds.length;
                const targetIndex = Math.floor((value / 100) * (actualTotalSlices - 1));
                await viewport.setImageIdIndex(targetIndex);
                setCurrentSliceIndex(targetIndex);
            }
        },
        [getViewport],
    );

    useEffect(() => {
        const fetchMetadataForCurrentSlice = async () => {
            const viewport = getViewport();
            if (!viewport || totalSlices === 0) return;

            try {
                const image = viewport.getCornerstoneImage() as IImage & { data: DataSet };
                const ds = image.data as DataSet;

                // For multi-frame images, extract frame-specific metadata if available
                const currentImageId = viewport.getCurrentImageId();
                const frameMatch = currentImageId?.match(/frame=(\d+)/);
                const currentFrame = frameMatch ? parseInt(frameMatch[1], 10) : 0;

                // Extract per-frame attributes for multi-frame images
                const isMultiFrame = isMultiFrameDicom(ds);
                const perFrameAttrs = isMultiFrame ? getPerFrameAttributes(ds, currentFrame - 1) : null;

                const FOV = `${(image.columnPixelSpacing * image.columns).toFixed(2)} mm x ${(image.rowPixelSpacing * image.rows).toFixed(2)} mm`;
                const acquisitionMatrix = `${image.columns} x ${image.rows}`;

                // Handle frame-specific instance numbers for multi-frame images
                let instanceNumber = ds.string('x00200013') || 'N/A';
                if (isMultiFrame) {
                    // For multi-frame images, show frame number as instance identifier
                    instanceNumber += ` (frame ${currentFrame})`;
                }

                // Use per-frame attributes when available, fallback to shared attributes
                const getAttributeValue = (perFrameKey: string, sharedTag: string, unit?: string) => {
                    const perFrameValue = perFrameAttrs?.[perFrameKey];
                    const sharedValue = ds.string(sharedTag);
                    const value = perFrameValue || sharedValue;
                    return value ? `${value}${unit || ''}` : 'N/A';
                };

                setCurrentMetadata({
                    instanceNumber,
                    sliceThickness: getAttributeValue('sliceThickness', 'x00180050', ' mm'),
                    sliceSpacing: `${ds.string('x00180088')} mm` || 'N/A',
                    seriesName: ds.string('x0008103E') || 'N/A',
                    imageComments: ds.string('x00204000') || 'N/A',
                    FOV,
                    kvp: getAttributeValue('kvp', 'x00180060', ' kVp'),
                    current: getAttributeValue('xRayTubeCurrent', 'x00181151', ' mA'),
                    tr: getAttributeValue('repetitionTime', 'x00180080', ' ms'),
                    te: getAttributeValue('echoTime', 'x00180081', ' ms'),
                    sequenceName: ds.string('x00180024') || 'N/A',
                    fieldStrength: `${ds.string('x00180087')}T` || 'N/A',
                    modality: ds.string('x00080060') || 'N/A',
                    acquisitionMatrix,
                });

                setCurrentDataset(ds);
            } catch (error) {
                console.warn('Error fetching metadata for current slice:', error);
                setCurrentMetadata({
                    instanceNumber: 'N/A',
                    sliceThickness: 'N/A',
                    seriesName: 'N/A',
                });
            }
            // NOTE: currentSliceIndex and currentSeriesId are implicit dependencies of `viewport.getCornerstoneImage()`
            // in that when they change, it indicates the cornerstone image has changed and metadata must be re-fetched.
        };
        fetchMetadataForCurrentSlice();
    }, [currentSliceIndex, totalSlices, currentSeriesId, getViewport]);

    useEffect(() => {
        const viewport = getViewport();
        if (!viewport || totalSlices === 0) {
            return;
        }

        const handleImageChange = () => {
            const currentIndex = viewport!.getCurrentImageIdIndex() || 0;
            setCurrentSliceIndex(currentIndex);
        };

        const handleVoiModified = (e: Event) => {
            const range = (e as CustomEvent<{ range?: { lower: number; upper: number } }>).detail?.range;
            if (!range) return;
            setVoi(windowLevelFromVoiRange(range));
        };

        // Add event listener for image changes on the element
        const element = viewport!.element;
        element.addEventListener('CORNERSTONE_IMAGE_RENDERED', handleImageChange);
        element.addEventListener(CornerstoneEnums.Events.VOI_MODIFIED, handleVoiModified);

        return () => {
            element.removeEventListener('CORNERSTONE_IMAGE_RENDERED', handleImageChange);
            element.removeEventListener(CornerstoneEnums.Events.VOI_MODIFIED, handleVoiModified);
        };
    }, [totalSlices, getViewport]);

    useEffect(() => {
        const run = async () => {
            if (!containerRef.current) {
                console.error('Container ref is not set');
                return;
            }
            if (initCornerstone) {
                ensureCornerstoneInitialized();
            }

            const renderingEngine = new CornerstoneCore.RenderingEngine(renderingEngineId);
            renderingEngineRef.current = renderingEngine;
            const viewportInput = {
                viewportId,
                type: CornerstoneCore.Enums.ViewportType.STACK,
                element: containerRef.current,
            };

            renderingEngine.enableElement(viewportInput);

            cornerstoneTools.addTool(cornerstoneTools.StackScrollTool);
            cornerstoneTools.addTool(cornerstoneTools.WindowLevelTool);
            const toolGroup = cornerstoneTools.ToolGroupManager.createToolGroup(toolGroupId);
            if (!toolGroup) {
                console.error('Failed to create tool group');
                return;
            }
            toolGroup.addTool(cornerstoneTools.StackScrollTool.toolName);
            toolGroup.setToolActive(cornerstoneTools.StackScrollTool.toolName, {
                bindings: [{ mouseButton: MouseBindings.Wheel }],
            });
            toolGroup.addTool(cornerstoneTools.WindowLevelTool.toolName);
            toolGroup.setToolActive(cornerstoneTools.WindowLevelTool.toolName, {
                bindings: [{ mouseButton: MouseBindings.Primary }],
            });
            toolGroup.addViewport(viewportId, renderingEngineId);
        };
        run();
        return () => {
            // Cleanup function to destroy the rendering engine and tool group
            const renderingEngine = renderingEngineRef.current;
            const element = renderingEngine?.getViewport(viewportId)?.element;
            if (element) {
                cornerstoneTools.utilities.stackContextPrefetch.disable(element);
            }
            renderingEngine?.destroy();
            renderingEngineRef.current = null;
            cornerstoneTools.ToolGroupManager.destroyToolGroup(toolGroupId);
            // Release this viewer's cached images + file blobs so they do not
            // outlive the mount.
            releaseSeries(loadedImageIdsRef.current, loadedFileIndicesRef.current);
            loadedImageIdsRef.current = [];
            loadedFileIndicesRef.current = [];
        };
    }, [renderingEngineId, viewportId, toolGroupId, initCornerstone]);

    // Download thumbnails whenever the study/series set changes.
    useEffect(() => {
        const downloadPromises = studies.flatMap((study) => study.series.map((series) => downloadThumbnail(series)));
        Promise.all(downloadPromises);
    }, [studies, downloadThumbnail]);

    // Revoke thumbnail object URLs on unmount to avoid leaks.
    useEffect(() => {
        const urls = thumbnailUrlsRef.current;
        return () => {
            urls.forEach((url) => URL.revokeObjectURL(url));
            urls.clear();
        };
    }, []);

    useResizeDetector({
        targetRef: containerRef,
        onResize: ({ width, height }) => {
            if (width && height) {
                renderingEngineRef.current?.resize();
            }
        },
    });

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            const preset = WL_PRESETS.find((p) => p.key === e.key);
            if (!preset) return;
            const viewport = getViewport();
            if (!viewport) return;
            viewport.setProperties({
                voiRange: voiRangeFromWindowLevel(preset.windowWidth, preset.windowCenter),
            });
            viewport.render();
            e.preventDefault();
        };
        const handleMouseEnter = () => el.focus({ preventScroll: true });
        // Stop wheel scrolling over the viewport from also scrolling the page.
        // Must be non-passive so preventDefault is honored; this only cancels the
        // default page scroll — Cornerstone's StackScrollTool still receives the
        // event and advances the slice.
        const handleWheel = (e: WheelEvent) => e.preventDefault();
        el.addEventListener('keydown', handleKeyDown);
        el.addEventListener('mouseenter', handleMouseEnter);
        el.addEventListener('wheel', handleWheel, { passive: false });
        return () => {
            el.removeEventListener('keydown', handleKeyDown);
            el.removeEventListener('mouseenter', handleMouseEnter);
            el.removeEventListener('wheel', handleWheel);
        };
    }, [getViewport]);

    const noSeriesSelectedPlaceholder = (
        <div className={classes.placeholder}>
            <Box ta="center">
                <IconPhoto size={64} color="var(--mantine-color-gray-5)" style={{ display: 'block', margin: '0 auto 16px' }} />
                <Text size="lg" fw={500} mb="xs" c="gray.6">
                    No Series Selected
                </Text>
                <Text size="sm" c="gray.5">
                    Select a series from the sidebar to view DICOM images
                </Text>
            </Box>
        </div>
    );

    const cantDisplaySeriesPlaceholder = (
        <div className={classes.placeholder}>
            <Box ta="center">
                <IconPhoto size={64} color="var(--mantine-color-gray-5)" style={{ display: 'block', margin: '0 auto 16px' }} />
                <Text size="lg" fw={500} mb="xs" c="gray.6">
                    Cannot Display Series
                </Text>
                <Text size="sm" c="gray.5">
                    Selected series does not have displayable pixel data
                </Text>
            </Box>
        </div>
    );

    let placeholderMessage;
    if (!canDisplaySeries) {
        placeholderMessage = cantDisplaySeriesPlaceholder;
    } else if (!currentSeriesId && !loadInProgress) {
        placeholderMessage = noSeriesSelectedPlaceholder;
    }

    const currentSeries = currentSeriesId ? studies.flatMap((s) => s.series).find((s) => s.id === currentSeriesId) : null;

    return (
        <div className={classes.root}>
            {/* Series Thumbnails Sidebar */}
            {studies.length > 0 && (
                <Paper w={200} p="xs" mr="xs" bg="gray.0">
                    <ScrollArea h="100%">
                        <Stack gap="xs">
                            {studies.map((study) => (
                                <div key={study.id}>
                                    {/* Study Header */}
                                    <Paper
                                        withBorder
                                        p="xs"
                                        bg="white"
                                        className={classes.studyHeader}
                                        onClick={() => toggleStudy(study.id)}
                                    >
                                        <Group justify="space-between" align="center">
                                            <div>
                                                <Text size="sm" fw={500}>
                                                    {study.study_description || `Study ${study.study_name || 'N/A'}`}
                                                </Text>
                                                <Text size="xs" c="dimmed">
                                                    {study.series.length} series
                                                </Text>
                                            </div>
                                            <ActionIcon variant="subtle" size="sm">
                                                {expandedStudies[study.id] ? (
                                                    <IconChevronDown size={16} />
                                                ) : (
                                                    <IconChevronRight size={16} />
                                                )}
                                            </ActionIcon>
                                        </Group>
                                    </Paper>

                                    {/* Series Thumbnails */}
                                    <Collapse in={expandedStudies[study.id]}>
                                        <Stack gap="xs" mt="xs">
                                            {study.series.map((series) => (
                                                <Paper
                                                    key={series.id}
                                                    withBorder
                                                    p="xs"
                                                    className={classes.seriesCard}
                                                    bg={currentSeriesId === series.id ? 'blue.1' : 'white'}
                                                    style={
                                                        currentSeriesId === series.id
                                                            ? { borderColor: 'var(--mantine-color-blue-5)', borderWidth: 2 }
                                                            : undefined
                                                    }
                                                    onClick={() => loadSeries(series)}
                                                >
                                                    <Stack gap="xs" align="center">
                                                        {/* Thumbnail */}
                                                        <div className={classes.thumbnailBox}>
                                                            {loadingThumbnails[series.id] ? (
                                                                <Text size="xs" c="gray.5">
                                                                    Loading...
                                                                </Text>
                                                            ) : seriesThumbnails[series.id] ? (
                                                                <img
                                                                    src={seriesThumbnails[series.id]}
                                                                    alt={`Series ${series.series_number || 'N/A'}`}
                                                                    className={classes.thumbnailImg}
                                                                    width={128}
                                                                    height={128}
                                                                />
                                                            ) : (
                                                                <Text size="xs" ta="center" c="gray.5">
                                                                    No Preview
                                                                </Text>
                                                            )}
                                                        </div>

                                                        {/* Series Info */}
                                                        <Box ta="center">
                                                            <Text size="xs" fw={500} maw={120}>
                                                                {series.series_description ||
                                                                    `Series ${series.series_number || 'N/A'}`}
                                                            </Text>
                                                            <Group gap="xs" justify="center">
                                                                <Badge size="xs" variant="outline">
                                                                    {series.modality}
                                                                </Badge>
                                                                <Text size="xs" c="dimmed">
                                                                    {series.instance_count} slices
                                                                </Text>
                                                                {/* Add visual indicator for multi-frame detection */}
                                                                {currentSeriesId === series.id &&
                                                                    isCurrentSeriesMultiFrame && (
                                                                        <Badge size="xs" variant="filled" color="blue">
                                                                            Multi-frame
                                                                        </Badge>
                                                                    )}
                                                            </Group>
                                                        </Box>
                                                    </Stack>
                                                </Paper>
                                            ))}
                                        </Stack>
                                    </Collapse>
                                </div>
                            ))}
                        </Stack>
                    </ScrollArea>
                </Paper>
            )}

            {/* Main Viewer */}
            <Box style={{ display: 'flex', flex: 1, flexDirection: 'column', overflow: 'hidden' }}>
                {loadInProgress ? <Progress value={loadProgress} size="md" radius="md" m="md" /> : null}

                <Tabs
                    variant="pills"
                    defaultValue="viewer"
                    style={{ display: 'flex', flex: 1, flexDirection: 'column', height: 'calc(100% - 85px)' }}
                    value={activeTab}
                    onChange={(value) => setActiveTab(value as 'viewer' | 'metadata')}
                >
                    <Tabs.List style={{ width: '25%', gap: 4 }}>
                        <Tabs.Tab value="viewer" style={{ flex: 1 }}>
                            Viewer
                        </Tabs.Tab>
                        <Tabs.Tab value="metadata" style={{ flex: 1 }}>
                            Metadata
                        </Tabs.Tab>
                    </Tabs.List>
                    <Tabs.Panel value="viewer" style={{ position: 'relative', display: 'flex', height: '100%' }}>
                        <div ref={containerRef} id="layerGroup0" tabIndex={0} className={classes.viewport} />
                        {placeholderMessage}

                        {/* DICOM Metadata Overlay */}
                        {totalSlices > 0 && Object.keys(currentMetadata).length > 0 && (
                            <>
                                <div className={classes.overlayTopLeft}>
                                    <div className={classes.overlayStack}>
                                        <div>
                                            <strong>Instance:</strong> {currentMetadata.instanceNumber}
                                            {isCurrentSeriesMultiFrame && (
                                                <span style={{ marginLeft: 8, color: 'var(--mantine-color-blue-2)' }}>
                                                    [Multi-frame]
                                                </span>
                                            )}
                                        </div>
                                        <div>
                                            <strong>Series Description:</strong> {currentMetadata.seriesName}
                                        </div>
                                        {voi && (
                                            <div>
                                                <strong>WW:</strong> {Math.round(voi.windowWidth)}
                                                {' / '}
                                                <strong>WL:</strong> {Math.round(voi.windowCenter)}
                                            </div>
                                        )}
                                        {/* Per-frame specific information for multi-frame images */}
                                        {isCurrentSeriesMultiFrame && currentMetadata.stackId && (
                                            <div>
                                                <strong>Stack ID:</strong> {currentMetadata.stackId}
                                            </div>
                                        )}
                                        {isCurrentSeriesMultiFrame && currentMetadata.imagePosition && (
                                            <div>
                                                <strong>Image Position:</strong>{' '}
                                                {currentMetadata.imagePosition
                                                    .split('\\')
                                                    .map((coord) => parseFloat(coord).toFixed(1))
                                                    .join(', ')}
                                            </div>
                                        )}
                                        {currentMetadata.modality === 'CT' && (
                                            <>
                                                <div>
                                                    <strong>KVP:</strong> {currentMetadata.kvp}
                                                </div>
                                                <div>
                                                    <strong>Current:</strong> {currentMetadata.current}
                                                </div>
                                            </>
                                        )}
                                        {currentMetadata.modality === 'MR' && (
                                            <>
                                                <div>
                                                    <strong>TR:</strong> {currentMetadata.tr}
                                                </div>
                                                <div>
                                                    <strong>TE:</strong> {currentMetadata.te}
                                                </div>
                                            </>
                                        )}
                                        {isCurrentSeriesMultiFrame && multiFrameInfo && (
                                            <div className={classes.overlayFrameInfo}>
                                                <div>
                                                    <strong>Frame Info:</strong> {multiFrameInfo.totalFrames} total
                                                    frames from {multiFrameInfo.totalFiles} files
                                                </div>
                                                {currentMetadata.pixelSpacing && (
                                                    <div>
                                                        <strong>Pixel Spacing:</strong> {currentMetadata.pixelSpacing}{' '}
                                                        mm
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                </div>
                                <div className={classes.overlayBottomRight}>
                                    {currentMetadata.modality === 'MR' && (
                                        <div>
                                            <strong>Sequence Name:</strong> {currentMetadata.sequenceName}
                                        </div>
                                    )}
                                    <div>
                                        <strong>Img Comments:</strong> {currentMetadata.imageComments || 'N/A'}
                                    </div>
                                    <div>
                                        <strong>FOV:</strong> {currentMetadata.FOV}
                                    </div>
                                    <div>
                                        <strong>Acq Matrix:</strong> {currentMetadata.acquisitionMatrix}
                                    </div>
                                    {currentMetadata.modality === 'MR' && (
                                        <div>
                                            <strong>Field Strength:</strong> {currentMetadata.fieldStrength}
                                        </div>
                                    )}
                                    <div>
                                        {currentMetadata.sliceThickness} thk / {currentMetadata.sliceSpacing} sep
                                    </div>
                                </div>
                            </>
                        )}
                    </Tabs.Panel>
                    <Tabs.Panel
                        value="metadata"
                        style={{ position: 'relative', display: 'flex', flex: 1, flexDirection: 'column', overflow: 'hidden' }}
                    >
                        <ScrollArea style={{ height: 0, flex: 1 }}>
                            {currentDataset && (
                                <DicomMetadataTable ds={currentDataset} visible={activeTab === 'metadata'} />
                            )}
                        </ScrollArea>
                    </Tabs.Panel>
                </Tabs>

                {/* Slice Slider - Horizontal at bottom */}
                {totalSlices > 0 && (
                    <Group h={24} justify="center" p="xs" bg="gray.1">
                        <Slider
                            size="md"
                            value={totalSlices > 1 ? (currentSliceIndex / (totalSlices - 1)) * 100 : 0}
                            onChange={handleSliderChange}
                            min={0}
                            max={100}
                            step={totalSlices > 1 ? 100 / (totalSlices - 1) : 1}
                            label={(value) => `${Math.round((value / 100) * (totalSlices - 1)) + 1}/${totalSlices}`}
                            style={{ width: '100%', maxWidth: 448 }}
                        />
                    </Group>
                )}

                {/* Current Series Info - Below slider */}
                {currentSeries && (
                    <Paper p="md" bg="gray.0">
                        <Group justify="space-between">
                            <div>
                                <Text size="sm" fw={500}>
                                    {currentSeries.series_description || `Series ${currentSeries.series_number || 'N/A'}`}
                                </Text>
                                <Text size="xs" c="dimmed">
                                    {currentSeries.modality} • {currentSeries.instance_count} instances
                                    {isCurrentSeriesMultiFrame && multiFrameInfo && (
                                        <>
                                            {' '}
                                            • Multi-frame ({multiFrameInfo.totalFiles} files,{' '}
                                            {multiFrameInfo.totalFrames} frames)
                                        </>
                                    )}
                                </Text>
                            </div>
                            <Group gap="xs">
                                <Badge variant="light">Series {currentSeries.series_number || 'N/A'}</Badge>
                                <Badge variant="outline">{currentSeries.modality}</Badge>
                                {isCurrentSeriesMultiFrame && (
                                    <Badge variant="filled" color="blue" size="sm">
                                        Multi-frame
                                    </Badge>
                                )}
                            </Group>
                        </Group>
                    </Paper>
                )}
            </Box>
        </div>
    );
}
