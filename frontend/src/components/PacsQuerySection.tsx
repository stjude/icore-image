import { useState } from 'react';

import type { Settings } from '../api/types';
import { ColumnNameModal, DateWindowModal, type ColumnType } from './QueryColumnModals';

export type QueryType = 'accession' | 'mrn_date';

export interface QueryColumns {
  queryType: QueryType;
  useFallbackQuery: boolean;
  accessionColumn: string;
  mrnColumn: string;
  dateColumn: string;
  dateWindow: string;
}

/** The request-payload slice this hook contributes (a subset of the
 * generated PacsQueryFields contract). */
export interface QueryColumnsPayload {
  acc_col: string;
  mrn_col: string;
  date_col: string;
  date_window: number;
  use_fallback_query?: boolean;
}

/** Consolidated state for the PACS query-method UI shared by the Image
 * Query / Image Deid / Image Deid+Export pages, plus the payload fragment
 * those pages' submit functions all build identically. */
export function useQueryColumns() {
  const [columns, setColumns] = useState<QueryColumns>({
    queryType: 'accession',
    useFallbackQuery: false,
    accessionColumn: 'AccessionNumber',
    mrnColumn: 'PatientID',
    dateColumn: 'StudyDate',
    dateWindow: '0',
  });

  const set = (patch: Partial<QueryColumns>) => {
    setColumns((current) => ({ ...current, ...patch }));
  };

  /** Settings pre-fill, with the legacy quirk that any default_query_method
   * other than the literal "Accession" (including missing) selects MRN+Date. */
  const prefill = (settings: Settings) => {
    set({
      queryType: settings.default_query_method === 'Accession' ? 'accession' : 'mrn_date',
      accessionColumn: settings.default_accession_header || 'AccessionNumber',
      mrnColumn: settings.default_mrn_header || 'PatientID',
      dateColumn: settings.default_date_header || 'StudyDate',
      dateWindow:
        settings.default_date_window_days !== undefined
          ? String(settings.default_date_window_days)
          : '0',
    });
  };

  /** acc_col / mrn_col / date_col / date_window / use_fallback_query exactly
   * as every legacy run*() built them. The query keys are sent even when the
   * page is in LOCAL mode, matching legacy. */
  const payload = (): QueryColumnsPayload => {
    const parsedWindow = parseInt(columns.dateWindow) || 0;
    if (columns.queryType === 'accession') {
      return {
        mrn_col: columns.mrnColumn,
        acc_col: columns.accessionColumn,
        date_col: columns.useFallbackQuery ? columns.dateColumn : '',
        date_window: columns.useFallbackQuery ? parsedWindow : 0,
        ...(columns.useFallbackQuery ? { use_fallback_query: true } : {}),
      };
    }
    return {
      mrn_col: columns.mrnColumn,
      acc_col: '',
      date_col: columns.dateColumn,
      date_window: parsedWindow,
    };
  };

  return { columns, set, prefill, payload };
}

interface Props {
  query: ReturnType<typeof useQueryColumns>;
}

/** The "Query using:" radios, fallback checkbox, column displays with their
 * (Change) links, and the two edit modals — identical markup on every PACS
 * query page (legacy image_deid/image_query/image_deid_export templates). */
export function PacsQuerySection({ query }: Props) {
  const { columns, set } = query;
  const [columnModal, setColumnModal] = useState<ColumnType | null>(null);
  const [dateWindowModalOpen, setDateWindowModalOpen] = useState(false);

  const mrnDateVisible = columns.queryType === 'mrn_date' || columns.useFallbackQuery;

  const changeLink = (onClick: () => void) => (
    <a
      href="#"
      onClick={(event) => {
        event.preventDefault();
        onClick();
      }}
      className="text-blue-600 hover:text-blue-800 ml-2"
    >
      (Change)
    </a>
  );

  return (
    <>
      <div className="flex items-center mt-4">
        <div className="text-sm text-gray-600">Query using:</div>
        <input
          className="ml-4"
          type="radio"
          id="query_accession"
          name="query_type"
          value="accession"
          checked={columns.queryType === 'accession'}
          onChange={() => {
            set({ queryType: 'accession' });
          }}
        />
        <span className="text-sm ml-1">Accession</span>
        <input
          className="ml-4"
          type="radio"
          id="query_mrn_date"
          name="query_type"
          value="mrn_date"
          checked={columns.queryType === 'mrn_date'}
          onChange={() => {
            // Legacy toggleQueryTypeInputs() unchecks the fallback checkbox
            // when switching to MRN + Date.
            set({ queryType: 'mrn_date', useFallbackQuery: false });
          }}
        />
        <span className="text-sm ml-1">MRN + Date</span>
      </div>
      <div
        id="fallback_query_option"
        className={columns.queryType === 'accession' ? 'mt-2' : 'mt-2 hidden'}
      >
        <label className="flex items-center cursor-pointer">
          <input
            type="checkbox"
            id="use_fallback_query"
            className="mr-2"
            checked={columns.useFallbackQuery}
            onChange={(event) => {
              set({ useFallbackQuery: event.target.checked });
            }}
          />
          <span className="text-sm text-gray-600">Enable MRN + Date fallback</span>
        </label>
        <div className="text-xs text-gray-400 ml-6">
          When accession query returns no results, retry using MRN + Study Date
        </div>
      </div>

      <div
        id="accession_display"
        className={columns.queryType === 'accession' ? 'mt-2' : 'mt-2 hidden'}
      >
        <div className="text-sm text-gray-500">
          Accession column: <span id="accession_column_display">{columns.accessionColumn}</span>{' '}
          {changeLink(() => {
            setColumnModal('accession');
          })}
        </div>
      </div>

      <div id="mrn_date_display" className={mrnDateVisible ? 'mt-2' : 'hidden mt-2'}>
        <div className="text-sm text-gray-500">
          MRN column: <span id="mrn_column_display">{columns.mrnColumn}</span>{' '}
          {changeLink(() => {
            setColumnModal('mrn');
          })}{' '}
          <span className="ml-6">
            Date column: <span id="date_column_display">{columns.dateColumn}</span>
          </span>{' '}
          {changeLink(() => {
            setColumnModal('date');
          })}{' '}
          <span className="ml-6">
            Date window (days): <span id="date_window_display">{columns.dateWindow}</span>
          </span>{' '}
          {changeLink(() => {
            setDateWindowModalOpen(true);
          })}
        </div>
      </div>

      {columnModal && (
        <ColumnNameModal
          columnType={columnModal}
          initialValue={
            columnModal === 'accession'
              ? columns.accessionColumn
              : columnModal === 'mrn'
                ? columns.mrnColumn
                : columns.dateColumn
          }
          onSave={(value) => {
            if (columnModal === 'accession') {
              set({ accessionColumn: value });
            } else if (columnModal === 'mrn') {
              set({ mrnColumn: value });
            } else {
              set({ dateColumn: value });
            }
          }}
          onClose={() => {
            setColumnModal(null);
          }}
        />
      )}

      {dateWindowModalOpen && (
        <DateWindowModal
          initialValue={columns.dateWindow || '0'}
          onSave={(value) => {
            set({ dateWindow: String(value) });
          }}
          onClose={() => {
            setDateWindowModalOpen(false);
          }}
        />
      )}
    </>
  );
}
