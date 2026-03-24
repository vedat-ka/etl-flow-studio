import { useId, useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const PAGE_SIZE = 20;
const CHART_COLORS = ['#2f8f9d', '#f6a609', '#4f7cff', '#cf5a5a', '#7a8f1d'];

function toCsv(rows) {
  if (!rows.length) {
    return '';
  }

  const headerSet = new Set();
  rows.forEach((row) => {
    Object.keys(row ?? {}).forEach((key) => headerSet.add(key));
  });
  const headers = [...headerSet];

  const escapeCell = (value) => {
    const text = value == null ? '' : String(value);
    const escaped = text.replace(/"/g, '""');
    return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
  };

  const headerLine = headers.join(',');
  const body = rows
    .map((row) => headers.map((header) => escapeCell(row?.[header])).join(','))
    .join('\n');

  return `${headerLine}\n${body}`;
}

function downloadBlob(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

export default function NodeDataDialog({ step, onClose, onUploadSourceFile, onLoadPostgresSource }) {
  const [page, setPage] = useState(1);
  const uploadInputId = useId();
  const records = useMemo(() => step?.records ?? step?.preview ?? [], [step]);
  const sampledRecords = useMemo(() => records.slice(0, 80), [records]);
  const allColumns = useMemo(() => {
    const columnSet = new Set();
    sampledRecords.forEach((row) => {
      Object.keys(row ?? {}).forEach((key) => columnSet.add(key));
    });
    return [...columnSet];
  }, [sampledRecords]);
  const completenessData = useMemo(
    () =>
      allColumns.map((column) => ({
        name: column,
        filled: sampledRecords.filter((row) => row?.[column] !== null && row?.[column] !== undefined && row?.[column] !== '').length,
      })),
    [allColumns, sampledRecords],
  );
  const numericColumns = useMemo(
    () =>
      allColumns.filter((column) =>
        sampledRecords.some((row) => {
          const value = row?.[column];
          if (value === null || value === undefined || value === '') {
            return false;
          }
          return Number.isFinite(Number(value));
        }),
      ),
    [allColumns, sampledRecords],
  );
  const numericTrendData = useMemo(() => {
    const numericColumn = numericColumns[0];
    if (!numericColumn) {
      return [];
    }

    return sampledRecords
      .map((row, index) => ({
        index: index + 1,
        value: Number(row?.[numericColumn]),
      }))
      .filter((entry) => Number.isFinite(entry.value));
  }, [numericColumns, sampledRecords]);
  const numericDistributionData = useMemo(() => {
    const numericColumn = numericColumns[0];
    if (!numericColumn) {
      return [];
    }

    const counts = new Map();
    sampledRecords.forEach((row) => {
      const value = Number(row?.[numericColumn]);
      if (!Number.isFinite(value)) {
        return;
      }
      const key = Number.isInteger(value) ? String(value) : value.toFixed(2);
      counts.set(key, (counts.get(key) ?? 0) + 1);
    });

    return [...counts.entries()]
      .map(([name, count]) => ({ name, count }))
      .sort((left, right) => Number(left.name) - Number(right.name));
  }, [numericColumns, sampledRecords]);
  const useDistributionChart = numericDistributionData.length > 0 && numericDistributionData.length <= 12;
  const totalPages = Math.max(1, Math.ceil(records.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const pageRows = useMemo(() => {
    const start = (safePage - 1) * PAGE_SIZE;
    return records.slice(start, start + PAGE_SIZE);
  }, [records, safePage]);

  if (!step) {
    return null;
  }

  const onDownloadJson = () => {
    const content = JSON.stringify(records, null, 2);
    downloadBlob(`${step.label.replace(/\s+/g, '_')}.json`, content, 'application/json');
  };

  const onDownloadCsv = () => {
    const csvContent = toCsv(records);
    downloadBlob(`${step.label.replace(/\s+/g, '_')}.csv`, csvContent, 'text/csv;charset=utf-8');
  };

  const isFileSource =
    step.kind === 'source' &&
    (step.source_type === 'file' || step.label === 'CSV Source' || step.label.startsWith('CSV File:') || step.label.startsWith('JSON File:'));
  const isPostgresSource =
    step.kind === 'source' && (step.source_type === 'postgres' || step.label === 'PostgreSQL Source' || step.label.startsWith('PostgreSQL Source:'));
  const isLoadNode = step.kind === 'load';

  const onFileChange = async (event) => {
    const [file] = event.target.files ?? [];
    if (!file) {
      return;
    }

    await onUploadSourceFile?.(step.node_id, file);
    event.target.value = '';
  };

  return (
    <div className="dialog-overlay" role="presentation" onClick={onClose}>
      <section
        className="dialog-card dialog-card--nested"
        role="dialog"
        aria-modal="true"
        aria-label="Node step data"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="dialog-header">
          <h2>{step.label} - Datenansicht</h2>
          <button className="dialog-close" type="button" onClick={onClose}>
            Schliessen
          </button>
        </header>

        <p className="step-target">
          {step.kind.toUpperCase()} | rows: {step.row_count}
          {step.storage ? ` | storage: ${step.storage}` : ''}
          {step.persisted_rows ? ` | postgres rows: ${step.persisted_rows}` : ''}
          {step.transform_type ? ` | transform: ${step.transform_type}` : ''}
          {step.transform_engine ? ` | engine: ${step.transform_engine}` : ''}
          {step.records_truncated ? ' | gekuerzte Anzeige' : ''}
        </p>

        <section className="node-analytics">
          <div className="node-analytics__card">
            <div className="node-analytics__header">
              <h3>Spalten-Fuellgrad</h3>
              <span>erste 80 Records</span>
            </div>
            {completenessData.length ? (
              <div className="chart-canvas">
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={completenessData} margin={{ top: 8, right: 8, left: -18, bottom: 40 }}>
                    <CartesianGrid stroke="#dbe8ef" strokeDasharray="4 4" vertical={false} />
                    <XAxis dataKey="name" angle={-22} textAnchor="end" interval={0} height={62} tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                    <YAxis tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                    <Tooltip contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                    <Bar dataKey="filled" radius={[8, 8, 0, 0]}>
                      {completenessData.map((entry, index) => (
                        <Cell key={entry.name} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p className="chart-empty">Keine Spalten fuer Diagramme gefunden.</p>
            )}
          </div>

          <div className="node-analytics__card">
            <div className="node-analytics__header">
              <h3>Numerische Vorschau</h3>
              <span>{numericColumns[0] ? `Feld: ${numericColumns[0]}` : 'kein numerisches Feld'}</span>
            </div>
            {useDistributionChart ? (
              <div className="chart-canvas">
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={numericDistributionData} margin={{ top: 8, right: 8, left: -18, bottom: 16 }}>
                    <CartesianGrid stroke="#dbe8ef" strokeDasharray="4 4" vertical={false} />
                    <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                    <YAxis tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                    <Tooltip contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                    <Bar dataKey="count" fill="#2f8f9d" radius={[8, 8, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : numericTrendData.length ? (
              <div className="chart-canvas">
                <ResponsiveContainer width="100%" height={220}>
                  <LineChart data={numericTrendData} margin={{ top: 8, right: 8, left: -18, bottom: 16 }}>
                    <CartesianGrid stroke="#dbe8ef" strokeDasharray="4 4" vertical={false} />
                    <XAxis dataKey="index" tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                    <YAxis tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                    <Tooltip contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                    <Line type="monotone" dataKey="value" stroke="#2f8f9d" strokeWidth={2} dot={numericTrendData.length <= 24} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p className="chart-empty">Keine numerischen Daten fuer diese Node-Ansicht vorhanden.</p>
            )}
          </div>
        </section>

        <pre className="step-preview step-preview--large">
          {JSON.stringify(pageRows, null, 2)}
        </pre>

        <div className="dialog-footer">
          <div className="dialog-footer__group">
            {isFileSource ? (
              <>
                <label className="step-action-btn step-action-btn--label step-action-btn--primary" htmlFor={uploadInputId}>
                  CSV/JSON fuer Source laden
                </label>
                <input
                  id={uploadInputId}
                  className="upload-input"
                  type="file"
                  accept=".csv,.json,application/json,text/csv"
                  onChange={onFileChange}
                />
              </>
            ) : null}
            {isPostgresSource ? (
              <button
                className="step-action-btn step-action-btn--primary"
                type="button"
                onClick={() => onLoadPostgresSource?.(step.node_id)}
              >
                PostgreSQL Source laden
              </button>
            ) : null}
            {isLoadNode ? (
              <>
                <button className="step-action-btn step-action-btn--primary" type="button" onClick={onDownloadJson}>
                  Load JSON
                </button>
                <button className="step-action-btn step-action-btn--primary" type="button" onClick={onDownloadCsv}>
                  Load CSV
                </button>
              </>
            ) : null}
          </div>

          <div className="dialog-footer__pagination">
            <button
              className="step-action-btn"
              type="button"
              onClick={() => setPage((prev) => Math.max(1, prev - 1))}
              disabled={safePage === 1}
            >
              Zurueck
            </button>
            <span>Seite {safePage} / {totalPages}</span>
            <button
              className="step-action-btn"
              type="button"
              onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
              disabled={safePage === totalPages}
            >
              Weiter
            </button>
          </div>

          {!isLoadNode ? (
            <div className="dialog-footer__group dialog-footer__group--right">
              <button className="step-action-btn" type="button" onClick={onDownloadJson}>
                JSON herunterladen
              </button>
              <button className="step-action-btn" type="button" onClick={onDownloadCsv}>
                CSV herunterladen
              </button>
            </div>
          ) : <div className="dialog-footer__group dialog-footer__group--right" />}
        </div>
      </section>
    </div>
  );
}
