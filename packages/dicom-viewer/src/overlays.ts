import type { DataSet } from 'dicom-parser';
import { TAG_DICT } from './DicomDataDictionary';
import type { FrameAttributeTag, OverlayConfig, OverlayContext, OverlayItem } from './types';

// Reverse of TAG_DICT: DICOM keyword ("KVP") -> hex tag ("x00180060"). Built once
// so config can reference attributes by their human-readable keyword.
const KEYWORD_TO_TAG: Record<string, string> = {};
for (const entry of Object.values(TAG_DICT)) {
    // entry.tag is "(GGGG,EEEE)"
    KEYWORD_TO_TAG[entry.name] = `x${entry.tag.slice(1, 5)}${entry.tag.slice(6, 10)}`.toLowerCase();
}

/** Resolve a raw string value from a dataset by keyword or hex tag. */
export const getAttribute = (ds: DataSet, keywordOrTag: string): string | undefined => {
    const tag = keywordOrTag.startsWith('x') ? keywordOrTag : KEYWORD_TO_TAG[keywordOrTag];
    return tag ? ds.string(tag) : undefined;
};

/**
 * Looks up a specific frame attribute from the per-frame functional groups sequence,
 * falling back to the shared functional groups sequence if not found.
 */
export const lookupFrameAttribute = (
    dataset: DataSet,
    frameIndex: number,
    frameAttributeTag: FrameAttributeTag,
): string | number | undefined => {
    const read = (ds: DataSet) =>
        frameAttributeTag.parseType === 'double'
            ? ds.double(frameAttributeTag.attributeTag)
            : ds.string(frameAttributeTag.attributeTag);

    const perFrameSequence = dataset.elements.x52009230;
    if (perFrameSequence?.items && frameIndex >= 0 && frameIndex < perFrameSequence.items.length) {
        const sequenceElement = perFrameSequence.items[frameIndex]?.dataSet?.elements?.[frameAttributeTag.sequenceTag];
        if (sequenceElement?.items?.[0]?.dataSet) {
            const value = read(sequenceElement.items[0].dataSet);
            if (value) return value;
        }
    }
    // Fallback to shared functional group sequence.
    const sharedSequence = dataset.elements.x52009229;
    const sharedElement = sharedSequence?.items?.[0]?.dataSet?.elements?.[frameAttributeTag.sequenceTag];
    if (sharedElement?.items?.[0]?.dataSet) {
        const value = read(sharedElement.items[0].dataSet);
        if (value) return value;
    }
    return undefined;
};

/**
 * Resolve one overlay item against the current image context. Returns the string
 * to display, or null when the item should be omitted (modality mismatch or no value).
 */
export const resolveOverlayItem = (item: OverlayItem, ctx: OverlayContext): string | null => {
    if (item.modalities && !item.modalities.includes(ctx.modality)) return null;

    let raw: string | undefined;
    if (item.frame && ctx.isMultiFrame) {
        const frameValue = lookupFrameAttribute(ctx.dataset, ctx.frameIndex, item.frame);
        raw = frameValue === undefined ? undefined : String(frameValue);
    }
    if (raw === undefined && item.attribute) {
        raw = ctx.getAttribute(item.attribute);
    }

    const value = item.format ? item.format(raw, ctx) : raw;
    if (value === undefined || value === null || value === '') return null;
    return `${value}${item.unit ?? ''}`;
};

const na = (value: string | undefined) => value ?? 'N/A';

/** Built-in overlays, reproducing the viewer's original corner content. */
export const DEFAULT_OVERLAYS: OverlayConfig = {
    topLeft: [
        {
            attribute: 'InstanceNumber',
            label: 'Instance',
            format: (v, ctx) => (ctx.isMultiFrame ? `${na(v)} (frame ${ctx.frameIndex + 1})` : na(v)),
        },
        { attribute: 'SeriesDescription', label: 'Series Description', format: na },
        {
            label: 'WW/WL',
            format: (_v, ctx) =>
                ctx.voi ? `${Math.round(ctx.voi.windowWidth)} / ${Math.round(ctx.voi.windowCenter)}` : null,
        },
        {
            label: 'KVP',
            unit: ' kVp',
            modalities: ['CT'],
            attribute: 'x00180060',
            frame: { sequenceTag: 'x00189325', attributeTag: 'x00180060' },
        },
        {
            label: 'Current',
            unit: ' mA',
            modalities: ['CT'],
            attribute: 'x00181151',
            frame: { sequenceTag: 'x00189321', attributeTag: 'x00189330' },
        },
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
    ],
    bottomRight: [
        { attribute: 'SequenceName', label: 'Sequence Name', modalities: ['MR'] },
        { attribute: 'ImageComments', label: 'Img Comments', format: na },
        {
            label: 'FOV',
            format: (_v, ctx) =>
                `${(ctx.image.columnPixelSpacing * ctx.image.columns).toFixed(2)} mm x ${(ctx.image.rowPixelSpacing * ctx.image.rows).toFixed(2)} mm`,
        },
        { label: 'Acq Matrix', format: (_v, ctx) => `${ctx.image.columns} x ${ctx.image.rows}` },
        { attribute: 'x00180087', label: 'Field Strength', unit: 'T', modalities: ['MR'] },
        {
            attribute: 'x00180050',
            frame: { sequenceTag: 'x00289110', attributeTag: 'x00180050' },
            format: (thk, ctx) => `${na(thk)} mm thk / ${na(ctx.getAttribute('x00180088'))} mm sep`,
        },
    ],
};
