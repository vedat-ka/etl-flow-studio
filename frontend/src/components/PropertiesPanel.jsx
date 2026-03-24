import { useId, useMemo, useState } from 'react';
import { previewPostgresSource } from '../api/flowApi';
import { useFlowStore } from '../store/flowStore';
import { parseUploadedFile } from '../utils/sourceParsers';

function formatPreviewValue(value) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }

  if (typeof value === 'object') {
    return JSON.stringify(value);
  }

  return String(value);
}

export default function PropertiesPanel({ lastRunResult = null, onOpenNodeData = null }) {
  const nodes = useFlowStore((state) => state.nodes);
  const edges = useFlowStore((state) => state.edges);
  const selectedNodeId = useFlowStore((state) => state.selectedNodeId);
  const updateSelectedNode = useFlowStore((state) => state.updateSelectedNode);
  const deleteSelectedNode = useFlowStore((state) => state.deleteSelectedNode);
  const setStatusMessage = useFlowStore((state) => state.setStatusMessage);
  const sourceUploadId = useId();

  const selectedNode = useMemo(
    () => nodes.find((node) => node.id === selectedNodeId) ?? null,
    [nodes, selectedNodeId],
  );
  const selectedStep = useMemo(
    () => lastRunResult?.node_results?.find((item) => item.node_id === selectedNodeId) ?? null,
    [lastRunResult, selectedNodeId],
  );

  // Berechne (calculate) Pipeline-Label: alle upstream Source-Nodes
  const pipelineLabel = useMemo(() => {
    if (!selectedNodeId) return null;
    const parentMap = {};
    edges.forEach((edge) => {
      if (!parentMap[edge.target]) parentMap[edge.target] = [];
      parentMap[edge.target].push(edge.source);
    });
    const visited = new Set();
    const queue = [selectedNodeId];
    const sourceLabels = [];
    while (queue.length) {
      const nodeId = queue.shift();
      if (visited.has(nodeId)) continue;
      visited.add(nodeId);
      const node = nodes.find((n) => n.id === nodeId);
      if (node?.data.kind === 'source') {
        sourceLabels.push(node.data.label ?? 'Source');
      }
      (parentMap[nodeId] ?? []).forEach((parentId) => queue.push(parentId));
    }
    return sourceLabels.length ? sourceLabels.join(' + ') : null;
  }, [selectedNodeId, nodes, edges]);
  const previewRows = selectedStep?.preview ?? selectedStep?.records?.slice(0, 3) ?? [];
  const previewColumns = previewRows.length ? Object.keys(previewRows[0] ?? {}).slice(0, 4) : [];
  const currentConfig = JSON.stringify(selectedNode?.data.config ?? {}, null, 2);
  const [parseError, setParseError] = useState({ nodeId: null, message: '' });

  const resolveNumericImputeValue = (value) => {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized || normalized === 'none') {
      return 'auto';
    }
    if (normalized === 'keep' || normalized === 'keep_missing' || normalized === 'preserve') {
      return 'keep_missing';
    }
    return normalized;
  };

  const resolveCategoricalImputeValue = (value) => {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized || normalized === 'none') {
      return 'auto';
    }
    if (normalized === 'keep' || normalized === 'keep_missing' || normalized === 'preserve') {
      return 'keep_missing';
    }
    return normalized;
  };

  if (!selectedNode) {
    return (
      <aside className="properties">
        <h2>Properties</h2>
        <p>Wähle einen Node, um Parameter zu bearbeiten.</p>
      </aside>
    );
  }

  const onLabelChange = (event) => {
    updateSelectedNode({ label: event.target.value });
  };

  const onConfigChange = (event) => {
    const value = event.target.value;

    try {
      const parsed = JSON.parse(value);
      setParseError({ nodeId: selectedNodeId, message: '' });
      updateSelectedNode({ config: parsed });
    } catch {
      setParseError({ nodeId: selectedNodeId, message: 'JSON ist ungültig.' });
    }
  };

  const onSourceFileChange = async (event) => {
    const [file] = event.target.files ?? [];
    if (!file) {
      return;
    }

    try {
      const parsedFile = await parseUploadedFile(file);
      updateSelectedNode({
        label: parsedFile.label,
        config: parsedFile.config,
      });
      setStatusMessage(`Datei in Source geladen: ${file.name}`);
    } catch (error) {
      setStatusMessage(error.message);
    } finally {
      event.target.value = '';
    }
  };

  const onLoadPostgresSource = async () => {
    try {
      const result = await previewPostgresSource(selectedNode.data.config ?? {});
      updateSelectedNode({
        label: result.label ?? selectedNode.data.label,
        config: {
          ...(selectedNode.data.config ?? {}),
          ...result.config,
        },
      });
      setStatusMessage(`PostgreSQL Source geladen: ${result.row_count} rows.`);
    } catch (error) {
      setStatusMessage(error.message);
    }
  };

  const onPostgresFieldChange = (field, value) => {
    updateSelectedNode({
      config: {
        [field]: field === 'port' || field === 'limit' ? Number(value || 0) : value,
      },
    });
  };

  const onPipelineFieldChange = (field, value) => {
    let nextValue = value;

    if (['keep_columns', 'drop_columns', 'numeric_columns', 'categorical_columns'].includes(field)) {
      nextValue = String(value)
        .split(',')
        .map((entry) => entry.trim())
        .filter(Boolean);
    }

    updateSelectedNode({
      config: {
        [field]: nextValue,
      },
    });
  };

  const onTransformFieldChange = (field, value) => {
    updateSelectedNode({
      config: {
        [field]: value,
      },
    });
  };

  const isFileSource =
    selectedNode.data.kind === 'source' &&
    (selectedNode.data.label === 'CSV Source' || selectedNode.data.config?.source_type === 'file');
  const isPostgresSource =
    selectedNode.data.kind === 'source' && selectedNode.data.config?.source_type === 'postgres';
  const isPostgresLoad =
    selectedNode.data.kind === 'load' &&
    (selectedNode.data.label === 'PostgreSQL Load' || Boolean(selectedNode.data.config?.table));
  const isFilterTransform =
    selectedNode.data.kind === 'transform' &&
    (selectedNode.data.config?.transform_type === 'filter' || selectedNode.data.label === 'Filter');
  const isPipelineTransform =
    selectedNode.data.kind === 'transform' &&
    (selectedNode.data.config?.transform_type === 'transform' ||
      selectedNode.data.config?.transform_type === 'clean' ||
      selectedNode.data.label === 'Transform' ||
      selectedNode.data.label === 'Clean');
  const isJoinTransform =
    selectedNode.data.kind === 'transform' &&
    (selectedNode.data.config?.transform_type === 'join' || selectedNode.data.label === 'Join');

  return (
    <aside className="properties">
      <h2>Properties</h2>
      {pipelineLabel ? (
        <div className="pipeline-badge" title={pipelineLabel}>
          ⬡ {pipelineLabel}
        </div>
      ) : null}

      {isFileSource ? (
        <div className="source-actions">
          <label className="upload-btn upload-btn--panel" htmlFor={sourceUploadId}>
            CSV/JSON fuer diese Source auswaehlen
          </label>
          <input
            id={sourceUploadId}
            className="upload-input"
            type="file"
            accept=".csv,.json,application/json,text/csv"
            onChange={onSourceFileChange}
          />
        </div>
      ) : null}

      {isPipelineTransform ? (
        <section className="config-card">
          <div className="config-card__header">
            <h3>Pandas/Numpy Transform</h3>
          </div>

          <p className="dialog-help-text">
            Transform: Join, Filter, Spaltenauswahl, Imputation, Encoding, Skalierung — alles in einem Schritt.
          </p>

          <div className="config-grid">
            <label className="field" htmlFor="pipeline-query">
              <span>Filter-Ausdruck</span>
              <textarea
                id="pipeline-query"
                rows={3}
                value={selectedNode.data.config?.query ?? ''}
                onChange={(event) => onPipelineFieldChange('query', event.target.value)}
                placeholder="(status = active or status = pending) and amount >= 70"
              />
            </label>

            <label className="field" htmlFor="pipeline-target-column">
              <span>Target-Spalte</span>
              <input
                id="pipeline-target-column"
                value={selectedNode.data.config?.target_column ?? ''}
                onChange={(event) => onPipelineFieldChange('target_column', event.target.value)}
                placeholder="z. B. churn"
              />
            </label>

            <label className="field" htmlFor="pipeline-keep-columns">
              <span>Keep-Spalten</span>
              <input
                id="pipeline-keep-columns"
                value={(selectedNode.data.config?.keep_columns ?? []).join(', ')}
                onChange={(event) => onPipelineFieldChange('keep_columns', event.target.value)}
                placeholder="z. B. age, income, city"
              />
            </label>

            <label className="field" htmlFor="pipeline-drop-columns">
              <span>Drop-Spalten</span>
              <input
                id="pipeline-drop-columns"
                value={(selectedNode.data.config?.drop_columns ?? []).join(', ')}
                onChange={(event) => onPipelineFieldChange('drop_columns', event.target.value)}
                placeholder="z. B. id, comment"
              />
            </label>

            <label className="field" htmlFor="pipeline-join-enabled">
              <span>Join aktiv</span>
              <select
                id="pipeline-join-enabled"
                value={selectedNode.data.config?.join_enabled ? 'true' : 'false'}
                onChange={(event) => onPipelineFieldChange('join_enabled', event.target.value === 'true')}
              >
                <option value="false">Nein</option>
                <option value="true">Ja (manuell)</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-auto-join">
              <span>Auto-Join (2 Quellen)</span>
              <select
                id="pipeline-auto-join"
                value={selectedNode.data.config?.auto_join === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('auto_join', event.target.value === 'true')}
              >
                <option value="true">Ja (Schluessel auto-erkennen)</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-auto-join-type">
              <span>Auto-Join Typ</span>
              <select
                id="pipeline-auto-join-type"
                value={selectedNode.data.config?.auto_join_type ?? 'left'}
                onChange={(event) => onPipelineFieldChange('auto_join_type', event.target.value)}
              >
                <option value="left">Left (alle Zeilen linke Quelle)</option>
                <option value="inner">Inner (nur Matches)</option>
                <option value="full">Full Outer</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-left-key">
              <span>Left Key</span>
              <input
                id="pipeline-left-key"
                value={selectedNode.data.config?.left_key ?? 'id'}
                onChange={(event) => onPipelineFieldChange('left_key', event.target.value)}
                placeholder="customer_id"
              />
            </label>

            <label className="field" htmlFor="pipeline-right-key">
              <span>Right Key</span>
              <input
                id="pipeline-right-key"
                value={selectedNode.data.config?.right_key ?? 'id'}
                onChange={(event) => onPipelineFieldChange('right_key', event.target.value)}
                placeholder="id"
              />
            </label>

            <label className="field" htmlFor="pipeline-join-type">
              <span>Join-Typ</span>
              <select
                id="pipeline-join-type"
                value={selectedNode.data.config?.join_type ?? 'inner'}
                onChange={(event) => onPipelineFieldChange('join_type', event.target.value)}
              >
                <option value="inner">Inner</option>
                <option value="left">Left</option>
                <option value="right">Right</option>
                <option value="full">Full Outer</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-numeric-columns">
              <span>Numerische Features</span>
              <input
                id="pipeline-numeric-columns"
                value={(selectedNode.data.config?.numeric_columns ?? []).join(', ')}
                onChange={(event) => onPipelineFieldChange('numeric_columns', event.target.value)}
                placeholder="z. B. age, income"
              />
            </label>

            <label className="field" htmlFor="pipeline-categorical-columns">
              <span>Kategoriale Features</span>
              <input
                id="pipeline-categorical-columns"
                value={(selectedNode.data.config?.categorical_columns ?? []).join(', ')}
                onChange={(event) => onPipelineFieldChange('categorical_columns', event.target.value)}
                placeholder="z. B. city, segment"
              />
            </label>

            <label className="field" htmlFor="pipeline-impute-numeric">
              <span>Numerische Imputation</span>
              <select
                id="pipeline-impute-numeric"
                value={resolveNumericImputeValue(selectedNode.data.config?.impute_numeric)}
                onChange={(event) => onPipelineFieldChange('impute_numeric', event.target.value)}
              >
                <option value="auto">Auto (Median, Fallback 0)</option>
                <option value="keep_missing">Missing behalten</option>
                <option value="zero">0</option>
                <option value="mean">Mittelwert</option>
                <option value="median">Median</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-impute-categorical">
              <span>Kategoriale Imputation</span>
              <select
                id="pipeline-impute-categorical"
                value={resolveCategoricalImputeValue(selectedNode.data.config?.impute_categorical)}
                onChange={(event) => onPipelineFieldChange('impute_categorical', event.target.value)}
              >
                <option value="auto">Auto (Modus nur fuer echte Kategorien)</option>
                <option value="keep_missing">Missing behalten</option>
                <option value="mode">Modus</option>
                <option value="constant">Konstante</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-categorical-fill-value">
              <span>Fill-Wert Kategorie</span>
              <input
                id="pipeline-categorical-fill-value"
                value={selectedNode.data.config?.categorical_fill_value ?? 'missing'}
                onChange={(event) => onPipelineFieldChange('categorical_fill_value', event.target.value)}
                placeholder="missing"
              />
            </label>

            <label className="field" htmlFor="pipeline-encode-categorical">
              <span>Kategorien-Encoding</span>
              <select
                id="pipeline-encode-categorical"
                value={selectedNode.data.config?.encode_categorical ?? 'none'}
                onChange={(event) => onPipelineFieldChange('encode_categorical', event.target.value)}
              >
                <option value="none">Keins</option>
                <option value="onehot">One-Hot</option>
                <option value="label">Label Encoding</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-scale-numeric">
              <span>Numerik skalieren</span>
              <select
                id="pipeline-scale-numeric"
                value={selectedNode.data.config?.scale_numeric ?? 'none'}
                onChange={(event) => onPipelineFieldChange('scale_numeric', event.target.value)}
              >
                <option value="none">Keine</option>
                <option value="standard">Standardisierung</option>
                <option value="minmax">Min-Max</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-flatten-json">
              <span>JSON-Spalten aufloesen</span>
              <select
                id="pipeline-flatten-json"
                value={selectedNode.data.config?.flatten_json === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('flatten_json', event.target.value === 'true')}
              >
                <option value="true">Ja (empfohlen fuer ML)</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-normalize-col-names">
              <span>Spaltennamen normalisieren</span>
              <select
                id="pipeline-normalize-col-names"
                value={selectedNode.data.config?.normalize_column_names === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('normalize_column_names', event.target.value === 'true')}
              >
                <option value="true">Ja (snake_case)</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-parse-datetime">
              <span>Datum/Zeit-Features</span>
              <select
                id="pipeline-parse-datetime"
                value={selectedNode.data.config?.parse_datetime === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('parse_datetime', event.target.value === 'true')}
              >
                <option value="true">Ja (Jahr/Monat/Tag/Std.)</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-normalize-booleans">
              <span>Boolean zu 0/1</span>
              <select
                id="pipeline-normalize-booleans"
                value={selectedNode.data.config?.normalize_booleans === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('normalize_booleans', event.target.value === 'true')}
              >
                <option value="true">Ja</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-drop-constant">
              <span>Konstante Spalten entfernen</span>
              <select
                id="pipeline-drop-constant"
                value={selectedNode.data.config?.drop_constant_columns === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('drop_constant_columns', event.target.value === 'true')}
              >
                <option value="true">Ja</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-drop-id-columns">
              <span>ID-Spalten entfernen</span>
              <select
                id="pipeline-drop-id-columns"
                value={selectedNode.data.config?.drop_id_columns === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('drop_id_columns', event.target.value === 'true')}
              >
                <option value="true">Ja (UUID/Hash/run_id)</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-trim-strings">
              <span>Strings trimmen</span>
              <select
                id="pipeline-trim-strings"
                value={selectedNode.data.config?.trim_strings === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('trim_strings', event.target.value === 'true')}
              >
                <option value="true">Ja</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-convert-numeric">
              <span>Numerik erkennen</span>
              <select
                id="pipeline-convert-numeric"
                value={selectedNode.data.config?.convert_numeric === false ? 'false' : 'true'}
                onChange={(event) => onPipelineFieldChange('convert_numeric', event.target.value === 'true')}
              >
                <option value="true">Ja</option>
                <option value="false">Nein</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-drop-empty-rows">
              <span>Leere Rows entfernen</span>
              <select
                id="pipeline-drop-empty-rows"
                value={selectedNode.data.config?.drop_empty_rows ? 'true' : 'false'}
                onChange={(event) => onPipelineFieldChange('drop_empty_rows', event.target.value === 'true')}
              >
                <option value="false">Nein</option>
                <option value="true">Ja</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-drop-empty-columns">
              <span>Leere Spalten entfernen</span>
              <select
                id="pipeline-drop-empty-columns"
                value={selectedNode.data.config?.drop_empty_columns ? 'true' : 'false'}
                onChange={(event) => onPipelineFieldChange('drop_empty_columns', event.target.value === 'true')}
              >
                <option value="false">Nein</option>
                <option value="true">Ja</option>
              </select>
            </label>

            <label className="field" htmlFor="pipeline-drop-duplicates">
              <span>Duplikate entfernen</span>
              <select
                id="pipeline-drop-duplicates"
                value={selectedNode.data.config?.drop_duplicates ? 'true' : 'false'}
                onChange={(event) => onPipelineFieldChange('drop_duplicates', event.target.value === 'true')}
              >
                <option value="false">Nein</option>
                <option value="true">Ja</option>
              </select>
            </label>
          </div>
        </section>
      ) : null}

      {isPostgresSource ? (
        <section className="config-card">
          <div className="config-card__header">
            <h3>PostgreSQL Source</h3>
            <button className="action-secondary-btn action-secondary-btn--compact" type="button" onClick={onLoadPostgresSource}>
              Daten laden
            </button>
          </div>

          <div className="config-grid">
            <label className="field" htmlFor="pg-host">
              <span>Host</span>
              <input
                id="pg-host"
                value={selectedNode.data.config?.host ?? ''}
                onChange={(event) => onPostgresFieldChange('host', event.target.value)}
                placeholder="localhost"
              />
            </label>

            <label className="field" htmlFor="pg-port">
              <span>Port</span>
              <input
                id="pg-port"
                type="number"
                value={selectedNode.data.config?.port ?? 5432}
                onChange={(event) => onPostgresFieldChange('port', event.target.value)}
              />
            </label>

            <label className="field" htmlFor="pg-db">
              <span>Database</span>
              <input
                id="pg-db"
                value={selectedNode.data.config?.db ?? selectedNode.data.config?.database ?? ''}
                onChange={(event) => onPostgresFieldChange('db', event.target.value)}
                placeholder="mydatabase"
              />
            </label>

            <label className="field" htmlFor="pg-user">
              <span>User</span>
              <input
                id="pg-user"
                value={selectedNode.data.config?.user ?? ''}
                onChange={(event) => onPostgresFieldChange('user', event.target.value)}
                placeholder="myuser"
              />
            </label>

            <label className="field" htmlFor="pg-password">
              <span>Password</span>
              <input
                id="pg-password"
                type="password"
                value={selectedNode.data.config?.password ?? ''}
                onChange={(event) => onPostgresFieldChange('password', event.target.value)}
                placeholder="mypassword"
              />
            </label>

            <label className="field" htmlFor="pg-schema">
              <span>Schema</span>
              <input
                id="pg-schema"
                value={selectedNode.data.config?.schema ?? 'public'}
                onChange={(event) => onPostgresFieldChange('schema', event.target.value)}
                placeholder="public"
              />
            </label>

            <label className="field" htmlFor="pg-table">
              <span>Table</span>
              <input
                id="pg-table"
                value={selectedNode.data.config?.table ?? ''}
                onChange={(event) => onPostgresFieldChange('table', event.target.value)}
                placeholder="fact_orders"
              />
            </label>

            <label className="field" htmlFor="pg-limit">
              <span>Limit</span>
              <input
                id="pg-limit"
                type="number"
                value={selectedNode.data.config?.limit ?? 200}
                onChange={(event) => onPostgresFieldChange('limit', event.target.value)}
              />
            </label>
          </div>
        </section>
      ) : null}

      {isPostgresLoad ? (
        <section className="config-card">
          <div className="config-card__header">
            <h3>PostgreSQL Load</h3>
          </div>

          <p className="dialog-help-text">
            Die verarbeiteten Daten werden zur weiteren Verwendung in PostgreSQL gespeichert.
          </p>

          <div className="config-grid">
            <label className="field" htmlFor="load-database-url">
              <span>DATABASE_URL</span>
              <input
                id="load-database-url"
                value={selectedNode.data.config?.database_url ?? ''}
                onChange={(event) => onPostgresFieldChange('database_url', event.target.value)}
                placeholder="postgresql://user:password@localhost:5432/db"
              />
            </label>

            <label className="field" htmlFor="load-host">
              <span>Host</span>
              <input
                id="load-host"
                value={selectedNode.data.config?.host ?? ''}
                onChange={(event) => onPostgresFieldChange('host', event.target.value)}
                placeholder="localhost"
              />
            </label>

            <label className="field" htmlFor="load-port">
              <span>Port</span>
              <input
                id="load-port"
                type="number"
                value={selectedNode.data.config?.port ?? 5432}
                onChange={(event) => onPostgresFieldChange('port', event.target.value)}
              />
            </label>

            <label className="field" htmlFor="load-db">
              <span>Database</span>
              <input
                id="load-db"
                value={selectedNode.data.config?.db ?? selectedNode.data.config?.database ?? ''}
                onChange={(event) => onPostgresFieldChange('db', event.target.value)}
                placeholder="etl"
              />
            </label>

            <label className="field" htmlFor="load-user">
              <span>User</span>
              <input
                id="load-user"
                value={selectedNode.data.config?.user ?? ''}
                onChange={(event) => onPostgresFieldChange('user', event.target.value)}
                placeholder="postgres"
              />
            </label>

            <label className="field" htmlFor="load-password">
              <span>Password</span>
              <input
                id="load-password"
                type="password"
                value={selectedNode.data.config?.password ?? ''}
                onChange={(event) => onPostgresFieldChange('password', event.target.value)}
                placeholder="postgres"
              />
            </label>

            <label className="field" htmlFor="load-schema">
              <span>Schema</span>
              <input
                id="load-schema"
                value={selectedNode.data.config?.schema ?? 'public'}
                onChange={(event) => onPostgresFieldChange('schema', event.target.value)}
                placeholder="public"
              />
            </label>

            <label className="field" htmlFor="load-table">
              <span>Table</span>
              <input
                id="load-table"
                value={selectedNode.data.config?.table ?? ''}
                onChange={(event) => onPostgresFieldChange('table', event.target.value)}
                placeholder="fact_orders"
              />
            </label>

            <label className="field" htmlFor="load-mode">
              <span>Mode</span>
              <select
                id="load-mode"
                value={selectedNode.data.config?.mode ?? 'append'}
                onChange={(event) => onPostgresFieldChange('mode', event.target.value)}
              >
                <option value="append">Append</option>
                <option value="replace">Replace</option>
              </select>
            </label>

            <label className="field" htmlFor="load-primary-key">
              <span>Primary Key (Spalte)</span>
              <input
                id="load-primary-key"
                value={selectedNode.data.config?.primary_key ?? ''}
                onChange={(event) => onPostgresFieldChange('primary_key', event.target.value)}
                placeholder="z. B. customer_id"
              />
            </label>

            <label className="field" htmlFor="load-foreign-keys">
              <span>Foreign Keys</span>
              <input
                id="load-foreign-keys"
                value={selectedNode.data.config?.foreign_keys ?? ''}
                onChange={(event) => onPostgresFieldChange('foreign_keys', event.target.value)}
                placeholder="col->ref_table.ref_col, z.B. customer_id->customers.customer_id"
              />
            </label>
          </div>
        </section>
      ) : null}

      {isFilterTransform ? (
        <section className="config-card">
          <div className="config-card__header">
            <h3>NumPy Filter Transform</h3>
          </div>

          <p className="dialog-help-text">
            Unterstuetzt `and`, `or`, Klammern sowie Operatoren wie `=`, `!=`, `e`, `c`, `contains`.
          </p>

          <label className="field" htmlFor="filter-query">
            <span>Filter-Ausdruck</span>
            <textarea
              id="filter-query"
              rows={4}
              value={selectedNode.data.config?.query ?? ''}
              onChange={(event) => onTransformFieldChange('query', event.target.value)}
              placeholder="(status = active or status = pending) and value >= 10"
            />
          </label>
        </section>
      ) : null}

      {isJoinTransform ? (
        <section className="config-card">
          <div className="config-card__header">
            <h3>NumPy Join Transform</h3>
          </div>

          <div className="config-grid">
            <label className="field" htmlFor="join-type">
              <span>Join-Typ</span>
              <select
                id="join-type"
                value={selectedNode.data.config?.join_type ?? 'inner'}
                onChange={(event) => onTransformFieldChange('join_type', event.target.value)}
              >
                <option value="inner">Inner</option>
                <option value="left">Left</option>
                <option value="right">Right</option>
                <option value="full">Full Outer</option>
              </select>
            </label>

            <label className="field" htmlFor="join-left-key">
              <span>Left Key</span>
              <input
                id="join-left-key"
                value={selectedNode.data.config?.left_key ?? 'id'}
                onChange={(event) => onTransformFieldChange('left_key', event.target.value)}
                placeholder="customer_id"
              />
            </label>

            <label className="field" htmlFor="join-right-key">
              <span>Right Key</span>
              <input
                id="join-right-key"
                value={selectedNode.data.config?.right_key ?? selectedNode.data.config?.left_key ?? 'id'}
                onChange={(event) => onTransformFieldChange('right_key', event.target.value)}
                placeholder="id"
              />
            </label>
          </div>
        </section>
      ) : null}

      <label className="field" htmlFor="node-label">
        <span>Label</span>
        <input
          id="node-label"
          value={selectedNode.data.label}
          onChange={onLabelChange}
        />
      </label>

      {!isPostgresSource && !isPostgresLoad && !isFilterTransform && !isPipelineTransform && !isJoinTransform ? (
        <label className="field" htmlFor="node-config">
          <span>Config (JSON)</span>
          <textarea
            key={selectedNode.id}
            id="node-config"
            rows={10}
            defaultValue={currentConfig}
            onChange={onConfigChange}
          />
        </label>
      ) : (
        <details className="advanced-config">
          <summary>Erweiterte JSON-Config</summary>
          <label className="field" htmlFor="node-config">
            <span>Config (JSON)</span>
            <textarea
              key={selectedNode.id}
              id="node-config"
              rows={8}
              defaultValue={currentConfig}
              onChange={onConfigChange}
            />
          </label>
        </details>
      )}

      <section className="runtime-panel">
        <div className="runtime-panel__header">
          <div>
            <h3>Ausfuehrung</h3>
            <p>
              {selectedStep
                ? 'Letzte Run-Daten fuer den selektierten Node.'
                : 'Noch keine Run-Daten fuer diesen Node verfuegbar.'}
            </p>
          </div>
          <button
            className="action-secondary-btn action-secondary-btn--compact"
            type="button"
            onClick={() => onOpenNodeData?.(selectedNode.id)}
            disabled={!selectedStep || !onOpenNodeData}
          >
            Daten & Diagramme
          </button>
        </div>

        <div className="runtime-grid">
          <div className="runtime-item">
            <span>Node-Typ</span>
            <strong>{selectedNode.data.kind}</strong>
          </div>
          <div className="runtime-item">
            <span>Status</span>
            <strong>{selectedStep ? 'Run vorhanden' : 'Noch nicht ausgefuehrt'}</strong>
          </div>
          <div className="runtime-item">
            <span>Rows</span>
            <strong>{selectedStep?.row_count ?? 0}</strong>
          </div>
          <div className="runtime-item">
            <span>Storage</span>
            <strong>{selectedStep?.storage ?? '-'}</strong>
          </div>
          <div className="runtime-item">
            <span>Ziel</span>
            <strong>{selectedStep?.target ?? '-'}</strong>
          </div>
          <div className="runtime-item">
            <span>Run-ID</span>
            <strong>{lastRunResult?.run_id ?? '-'}</strong>
          </div>
          <div className="runtime-item">
            <span>Transform</span>
            <strong>{selectedStep?.transform_type ?? selectedNode.data.config?.transform_type ?? '-'}</strong>
          </div>
          <div className="runtime-item">
            <span>Engine</span>
            <strong>{selectedStep?.transform_engine ?? '-'}</strong>
          </div>
        </div>

        {previewRows.length ? (
          <div className="runtime-preview">
            <span className="runtime-preview__title">Preview</span>
            <div className="runtime-preview__table">
              {previewColumns.map((column) => (
                <span key={column} className="runtime-preview__head">
                  {column}
                </span>
              ))}

              {previewRows.map((row, rowIndex) =>
                previewColumns.map((column) => (
                  <span key={`${rowIndex}-${column}`} className="runtime-preview__cell">
                    {formatPreviewValue(row?.[column])}
                  </span>
                )),
              )}
            </div>
          </div>
        ) : null}
      </section>

      {parseError.nodeId === selectedNodeId && parseError.message ? (
        <p className="error-text">{parseError.message}</p>
      ) : null}

      <button className="danger-btn" type="button" onClick={deleteSelectedNode}>
        Node loeschen
      </button>
    </aside>
  );
}
