import { useState, type ReactNode } from 'react';

import type { Settings } from '../api/types';
import { PathInput } from './PathInput';

/** Settings keys the deid options pre-fill reads (legacy reads them straight
 * off the /load_settings/ JSON). */
export interface DeidOptionsSettings extends Settings {
  default_tags_to_keep?: string;
  default_tags_to_dateshift?: string;
  default_tags_to_randomize?: string;
  mapping_file_path?: string;
  use_mapping_file?: boolean;
  default_deid_pixels?: boolean;
  default_remove_unspecified?: boolean;
  default_remove_overlays?: boolean;
  default_remove_curves?: boolean;
  default_remove_private?: boolean;
  default_apply_default_ctp_filter_script?: boolean;
  deid_engine?: string;
  date_shift_range?: number;
  site_id?: string;
}

export interface DeidOptions {
  tagsToKeep: string;
  tagsToDateshift: string;
  tagsToRandomize: string;
  removeUnspecified: boolean;
  removeOverlays: boolean;
  removeCurves: boolean;
  removePrivate: boolean;
  applyDefaultCtpFilterScript: boolean;
  deidPixels: boolean;
  deidEngineRust: boolean;
  useMappingFile: boolean;
  mappingFilePath: string;
  scPdfDestination: 'quarantine' | 'custom';
  scPdfOutputDir: string;
}

/** Consolidated state + settings pre-fill + payload fragment for the
 * "Advanced Deidentification Options" block shared by the Image Deid and
 * Image Deid+Export pages. */
export function useDeidOptions() {
  const [options, setOptions] = useState<DeidOptions>({
    tagsToKeep: '',
    tagsToDateshift: '',
    tagsToRandomize: '',
    removeUnspecified: false,
    removeOverlays: false,
    removeCurves: false,
    removePrivate: false,
    applyDefaultCtpFilterScript: true,
    deidPixels: false,
    deidEngineRust: false,
    useMappingFile: false,
    mappingFilePath: '',
    scPdfDestination: 'quarantine',
    scPdfOutputDir: '',
  });

  const set = (patch: Partial<DeidOptions>) => {
    setOptions((current) => ({ ...current, ...patch }));
  };

  /** Legacy pre-fill conditionals preserved exactly: tags/mapping/pixels keys
   * apply only when truthy; the remove flags, apply-default flag, and
   * deid_engine apply whenever present. */
  const prefill = (settings: DeidOptionsSettings) => {
    const patch: Partial<DeidOptions> = {};
    if (settings.default_tags_to_keep) patch.tagsToKeep = settings.default_tags_to_keep;
    if (settings.default_tags_to_dateshift) {
      patch.tagsToDateshift = settings.default_tags_to_dateshift;
    }
    if (settings.default_tags_to_randomize) {
      patch.tagsToRandomize = settings.default_tags_to_randomize;
    }
    if (settings.mapping_file_path) patch.mappingFilePath = settings.mapping_file_path;
    if (settings.use_mapping_file) patch.useMappingFile = Boolean(settings.use_mapping_file);
    if (settings.default_deid_pixels) patch.deidPixels = Boolean(settings.default_deid_pixels);
    if (settings.default_remove_unspecified !== undefined) {
      patch.removeUnspecified = Boolean(settings.default_remove_unspecified);
    }
    if (settings.default_remove_overlays !== undefined) {
      patch.removeOverlays = Boolean(settings.default_remove_overlays);
    }
    if (settings.default_remove_curves !== undefined) {
      patch.removeCurves = Boolean(settings.default_remove_curves);
    }
    if (settings.default_remove_private !== undefined) {
      patch.removePrivate = Boolean(settings.default_remove_private);
    }
    if (settings.default_apply_default_ctp_filter_script !== undefined) {
      patch.applyDefaultCtpFilterScript = Boolean(
        settings.default_apply_default_ctp_filter_script,
      );
    }
    if (settings.deid_engine !== undefined) {
      patch.deidEngineRust = settings.deid_engine === 'rust';
    }
    set(patch);
  };

  /** The deid-option request keys both pages build identically at submit
   * time, including the values read off the saved settings object. */
  const payload = (settings: DeidOptionsSettings) => ({
    tags_to_keep: options.tagsToKeep,
    tags_to_dateshift: options.tagsToDateshift,
    tags_to_randomize: options.tagsToRandomize,
    mapping_file_path: options.mappingFilePath || settings.mapping_file_path || '',
    use_mapping_file: options.useMappingFile,
    deid_pixels: options.deidPixels,
    remove_unspecified: options.removeUnspecified,
    remove_overlays: options.removeOverlays,
    remove_curves: options.removeCurves,
    remove_private: options.removePrivate,
    apply_default_ctp_filter_script: options.applyDefaultCtpFilterScript,
    deid_engine: options.deidEngineRust ? ('rust' as const) : ('ctp' as const),
    date_shift_days: settings.date_shift_range || 0,
    site_id: settings.site_id || '',
    pacs_configs: settings.pacs_configs || [],
    application_aet: settings.application_aet || '',
    sc_pdf_output_dir:
      options.scPdfDestination === 'custom' && options.scPdfOutputDir
        ? options.scPdfOutputDir
        : '',
  });

  return { options, set, prefill, payload };
}

interface CheckboxFieldProps {
  id: string;
  label: string;
  description: ReactNode;
  checked: boolean;
  disabled: boolean;
  onChange: (checked: boolean) => void;
}

function CheckboxField({ id, label, description, checked, disabled, onChange }: CheckboxFieldProps) {
  return (
    <div>
      <div className="flex items-center">
        <input
          type="checkbox"
          id={id}
          name={id}
          className="mr-2"
          checked={checked}
          disabled={disabled}
          onChange={(event) => {
            onChange(event.target.checked);
          }}
        />
        <label htmlFor={id}>{label}</label>
      </div>
      <div className="text-sm text-gray-500 ml-6">{description}</div>
    </div>
  );
}

interface TagsTextareaProps {
  label: string;
  name: string;
  value: string;
  disabled: boolean;
  onChange: (value: string) => void;
}

function TagsTextarea({ label, name, value, disabled, onChange }: TagsTextareaProps) {
  return (
    <div className="flex-1">
      <div className="text-sm text-gray-500">{label}</div>
      <textarea
        className="w-full border-2 border-gray-400 pt-1 px-2 pb-2 h-32 overflow-y-auto resize-none"
        name={name}
        value={value}
        disabled={disabled}
        onChange={(event) => {
          onChange(event.target.value);
        }}
      ></textarea>
    </div>
  );
}

interface Props {
  deid: ReturnType<typeof useDeidOptions>;
  disabled?: boolean;
}

/** The advanced deidentification options block (tag lists, global settings,
 * pixel deid, engine, mapping file, SC/PDF handling) — identical markup on
 * the Image Deid and Image Deid+Export pages. */
export function DeidOptionsSection({ deid, disabled = false }: Props) {
  const { options, set } = deid;

  return (
    <>
      {/* Advanced Deidentification Options Section */}
      <div className="mt-6">
        <div className="text-md mb-2">Advanced Deidentification Options</div>
        <div className="flex space-x-4">
          <TagsTextarea
            label="To keep"
            name="tags_to_keep"
            value={options.tagsToKeep}
            disabled={disabled}
            onChange={(value) => {
              set({ tagsToKeep: value });
            }}
          />
          <TagsTextarea
            label="To date shift"
            name="tags_to_dateshift"
            value={options.tagsToDateshift}
            disabled={disabled}
            onChange={(value) => {
              set({ tagsToDateshift: value });
            }}
          />
          <TagsTextarea
            label="To randomize"
            name="tags_to_randomize"
            value={options.tagsToRandomize}
            disabled={disabled}
            onChange={(value) => {
              set({ tagsToRandomize: value });
            }}
          />
        </div>
      </div>
      <div className="mt-8">
        <div className="text-md mb-4">Global Deidentification Settings</div>
        <div className="space-y-4">
          <CheckboxField
            id="remove_unspecified"
            label="Remove unspecified tags"
            description="If checked, any tag not specified in the lists above will be removed. If unchecked, unspecified tags will be preserved"
            checked={options.removeUnspecified}
            disabled={disabled}
            onChange={(checked) => {
              set({ removeUnspecified: checked });
            }}
          />
          <CheckboxField
            id="remove_overlays"
            label="Remove overlays"
            description="Remove all elements in 60xx groups. These are overlays and are sometimes removed when fully de-identifying an object because they can contain PHI."
            checked={options.removeOverlays}
            disabled={disabled}
            onChange={(checked) => {
              set({ removeOverlays: checked });
            }}
          />
          <CheckboxField
            id="remove_curves"
            label="Remove curve data"
            description="Remove all elements in 50xx groups. These are groups which contain curve data."
            checked={options.removeCurves}
            disabled={disabled}
            onChange={(checked) => {
              set({ removeCurves: checked });
            }}
          />
          <CheckboxField
            id="remove_private"
            label="Remove all private tags"
            description="Remove all elements in odd-numbered groups. These are private groups whose contents are not specified by the DICOM standard"
            checked={options.removePrivate}
            disabled={disabled}
            onChange={(checked) => {
              set({ removePrivate: checked });
            }}
          />
          <CheckboxField
            id="apply_default_ctp_filter_script"
            label="Apply default filters from known devices"
            description="Remove DICOM files during the deidentification process from known devices that are known to contain PHI. These files will be stored in the quarantine."
            checked={options.applyDefaultCtpFilterScript}
            disabled={disabled}
            onChange={(checked) => {
              set({ applyDefaultCtpFilterScript: checked });
            }}
          />
        </div>
      </div>
      <div className="mt-8">
        <div className="text-md mb-4">Pixel Deidentification</div>
        <div className="mb-4">
          <CheckboxField
            id="deid_pixels"
            label="Deidentify pixel data from device scanners"
            description="Blacks out regions of pixels in a DICOM image from known devices."
            checked={options.deidPixels}
            disabled={disabled}
            onChange={(checked) => {
              set({ deidPixels: checked });
            }}
          />
        </div>
        <div className="text-md mb-4 mt-8">Deidentification Engine</div>
        <div className="mb-4">
          <CheckboxField
            id="deid_engine_rust"
            label="Use experimental Rust engine"
            description="Use dicom-deid-rs instead of CTP for deidentification. This engine is faster but is still under active development."
            checked={options.deidEngineRust}
            disabled={disabled}
            onChange={(checked) => {
              set({ deidEngineRust: checked });
            }}
          />
        </div>
        <div className="text-md mb-4 mt-8">Mapping File Configuration</div>
        <div>
          <div className="flex items-center">
            <input
              type="checkbox"
              id="use_mapping_file"
              name="use_mapping_file"
              className="mr-2"
              checked={options.useMappingFile}
              disabled={disabled}
              onChange={(event) => {
                set({ useMappingFile: event.target.checked });
              }}
            />
            <label htmlFor="use_mapping_file">Use Mapping File</label>
          </div>
          <div
            id="mapping_file_input_container"
            className={options.useMappingFile ? 'mt-2' : 'mt-2 hidden'}
          >
            <PathInput
              className="w-full h-full border-2 border-gray-400 p-2"
              name="mapping_file_path"
              value={options.mappingFilePath}
              disabled={disabled}
              onChange={(value) => {
                set({ mappingFilePath: value });
              }}
            />
            <div className="text-sm text-gray-500 mt-1">
              Excel file with columns: TagName, New-TagName (e.g., AccessionNumber,
              New-AccessionNumber)
            </div>
          </div>
        </div>
        <div className="text-md mb-4 mt-8">Secondary Capture and PDF Handling</div>
        <div>
          <div className="text-sm text-gray-500 mb-2">
            Secondary Capture images, PDFs, and other embedded content files contain
            unredactable PHI and will be filtered from de-identification output.
          </div>
          <div className="ml-2">
            <div className="flex items-center mb-2">
              <input
                type="radio"
                id="sc_pdf_to_quarantine"
                name="sc_pdf_destination"
                value="quarantine"
                checked={options.scPdfDestination === 'quarantine'}
                disabled={disabled}
                onChange={() => {
                  set({ scPdfDestination: 'quarantine' });
                }}
              />
              <label htmlFor="sc_pdf_to_quarantine" className="ml-2">
                Send to quarantine folder
              </label>
            </div>
            <div className="flex items-center mb-2">
              <input
                type="radio"
                id="sc_pdf_to_custom"
                name="sc_pdf_destination"
                value="custom"
                checked={options.scPdfDestination === 'custom'}
                disabled={disabled}
                onChange={() => {
                  set({ scPdfDestination: 'custom' });
                }}
              />
              <label htmlFor="sc_pdf_to_custom" className="ml-2">
                Send to custom location
              </label>
            </div>
            <div
              id="sc_pdf_custom_path_container"
              className={options.scPdfDestination === 'custom' ? 'mt-2 ml-6' : 'hidden mt-2 ml-6'}
            >
              <PathInput
                className="w-full border-2 border-gray-400 p-2"
                name="sc_pdf_output_dir"
                placeholder="Drag and drop folder or enter path"
                value={options.scPdfOutputDir}
                disabled={disabled}
                onChange={(value) => {
                  set({ scPdfOutputDir: value });
                }}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
