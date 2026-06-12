import { useEffect, useState } from 'react';
import { useOutletContext } from 'react-router';

import { loadSettings, saveSettings } from '../../api/endpoints';
import {
  FILTER_ACTIONS,
  FilterList,
  ModalityFilters,
  type Filter,
  type ModalityFilterMap,
} from '../../components/filters';
import { useConstants } from '../../hooks/useConstants';
import type { SettingsOutletContext } from './SettingsLayout';

/** Shape of the saved `filters` blob in settings.json, as read and written by
 * the legacy settings/image_query.html page (settings.js
 * loadFiltersFromSettings/collectFilterData). */
interface SavedFilters {
  general_filters?: Filter[];
  modality_filters?: Record<string, Filter[]>;
}

/** Port of settings.js generateActionString(): translate legacy action names
 * into the values used by the operation dropdown. Unknown actions pass
 * through unchanged. */
function generateActionString(action: string): string {
  if (action === 'DoesNotContain') {
    return 'not_containsIgnoreCase';
  }
  if (action === 'Contains') {
    return 'containsIgnoreCase';
  }
  if (action === 'DoesNotStartWith') {
    return 'not_startsWithIgnoreCase';
  }
  if (action === 'StartsWith') {
    return 'startsWithIgnoreCase';
  }
  if (action === 'DoesNotEndWith') {
    return 'not_endsWithIgnoreCase';
  }
  if (action === 'EndsWith') {
    return 'endsWithIgnoreCase';
  }
  if (action === 'DoesNotEqual') {
    return 'not_equalsIgnoreCase';
  }
  if (action === 'Equals') {
    return 'equalsIgnoreCase';
  }
  return action;
}

/** Legacy assigned the mapped action to a <select>; an unrecognized value
 * left the cloned template's default (the first option) selected, and that
 * default is what a later save collected. Coerce the same way so the
 * controlled select never renders an empty selection. */
function normalizeSavedFilter(filter: Filter): Filter {
  const action = generateActionString(filter.action);
  return {
    tag: filter.tag,
    action: FILTER_ACTIONS.some(([actionId]) => actionId === action)
      ? action
      : (FILTER_ACTIONS[0]?.[0] ?? ''),
    value: filter.value,
  };
}

export function ImageQuerySettings() {
  const { registerSaveHandler, showSaveMessage } = useOutletContext<SettingsOutletContext>();
  const constants = useConstants();
  // Server context `modalities` (baked into the legacy template via json_script).
  const modalities = constants?.modalities ?? [];

  const [generalFilters, setGeneralFilters] = useState<Filter[]>([]);
  const [modalityFilters, setModalityFilters] = useState<ModalityFilterMap>({});

  // Legacy loadSavedSettings() on DOMContentLoaded: fetch /load_settings/ and
  // rebuild the filter rows from settings.filters via loadFiltersFromSettings().
  useEffect(() => {
    void (async () => {
      try {
        const settings = await loadSettings();
        const filters = settings.filters as SavedFilters | undefined;
        if (filters) {
          if (filters.general_filters) {
            setGeneralFilters(filters.general_filters.map(normalizeSavedFilter));
          }
          if (filters.modality_filters) {
            const loaded: ModalityFilterMap = {};
            for (const [modality, savedFilters] of Object.entries(filters.modality_filters)) {
              loaded[modality] = savedFilters.map(normalizeSavedFilter);
            }
            setModalityFilters(loaded);
          }
        }
      } catch (error) {
        console.error('Error loading settings:', error);
      }
    })();
  }, []);

  // Legacy saveSettings(): POST { filters: collectFilterData() } to
  // /save_settings/, then flash a "Settings Saved" message under the layout's
  // save button. collectFilterData() takes every general filter row verbatim
  // and, per modality, includes the modality key only when it has rows; no
  // trailing Modality-equals entry is added on save.
  const handleSave = async () => {
    const collectedModalityFilters: Record<string, Filter[]> = {};
    for (const modality of modalities) {
      const filters = modalityFilters[modality] ?? [];
      if (filters.length > 0) {
        collectedModalityFilters[modality] = filters;
      }
    }
    const filterData: SavedFilters = {
      general_filters: generalFilters,
      modality_filters: collectedModalityFilters,
    };

    try {
      await saveSettings({ filters: filterData });
      showSaveMessage('Settings Saved');
    } catch (error) {
      console.error('Error saving settings:', error);
    }
  };

  // Register with the layout's Save Settings button (the React replacement
  // for the legacy page-global saveSettings()). No dependency array so the
  // registered closure always sees the latest filter state.
  useEffect(() => {
    registerSaveHandler(() => {
      void handleSave();
    });
    return () => {
      registerSaveHandler(null);
    };
  });

  // components/filters_section.html with section_title="Default Query Filters".
  return (
    <div className="mt-8">
      <div className="text-md mb-4">Default Query Filters</div>

      {/* General Filters Section */}
      <div className="mb-4">
        <div className="text-sm text-gray-600 mb-2">General Filters</div>
        <div className="text-sm text-gray-400 mb-2">Only include images where</div>
        <FilterList filters={generalFilters} onChange={setGeneralFilters} />
      </div>

      {/* Modality Filters Section */}
      {modalities.length > 0 && (
        <div className="mt-4">
          <div className="text-md mb-2">Default Modality Filters</div>
          <ModalityFilters
            modalities={modalities}
            value={modalityFilters}
            onChange={setModalityFilters}
          />
        </div>
      )}
    </div>
  );
}
