import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Download, Eye, Play, Save, Trash2 } from 'lucide-react';
import './App.css';
import FlowCanvas from './components/FlowCanvas';
import NodeDataDialog from './components/NodeDataDialog';
import PropertiesPanel from './components/PropertiesPanel';
import RunDetailsDialog from './components/RunDetailsDialog';
import Sidebar from './components/Sidebar';
import { listFlows, loadFlow, previewPostgresSource, runFlow, saveFlow } from './api/flowApi';
import { useFlowStore } from './store/flowStore';
import { parseUploadedFile } from './utils/sourceParsers';

const DEFAULT_FLOW_ID = 'default';
const LEFT_PANEL_MIN_WIDTH = 240;
const LEFT_PANEL_MAX_WIDTH = 420;
const RIGHT_PANEL_MIN_WIDTH = 280;
const RIGHT_PANEL_MAX_WIDTH = 460;

const LEGACY_POSTGRES_DEFAULTS = {
  host: 'localhost',
  port: 5432,
  db: 'etl',
  user: 'postgres',
  password: 'postgres',
};

function findRecordsInValue(value) {
  if (Array.isArray(value)) {
    const objectItems = value.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
    if (objectItems.length) {
      return objectItems;
    }

    for (const item of value) {
      const nested = findRecordsInValue(item);
      if (nested.length) {
        return nested;
      }
    }

    return [];
  }

  if (value && typeof value === 'object') {
    for (const nestedValue of Object.values(value)) {
      const nested = findRecordsInValue(nestedValue);
      if (nested.length) {
        return nested;
      }
    }
  }

  return [];
}

function buildFallbackStep(node) {
  const config = node?.data?.config ?? {};
  const records = Array.isArray(config.records)
    ? config.records
    : findRecordsInValue(config);

  return {
    node_id: node.id,
    label: node.data?.label ?? 'Node',
    kind: node.data?.kind ?? 'transform',
    row_count: records.length,
    records,
    preview: records.slice(0, 5),
    storage: 'local-node-config',
    config,
    source_type: config.source_type,
  };
}

function normalizeNodeConfig(config) {
  if (!config || typeof config !== 'object') {
    return config;
  }

  const nextConfig = { ...config };

  const hasLegacyPostgresDefaults =
    String(nextConfig.host ?? '').toLowerCase() === LEGACY_POSTGRES_DEFAULTS.host &&
    Number(nextConfig.port ?? 5432) === LEGACY_POSTGRES_DEFAULTS.port &&
    String(nextConfig.db ?? nextConfig.database ?? '') === LEGACY_POSTGRES_DEFAULTS.db &&
    String(nextConfig.user ?? '') === LEGACY_POSTGRES_DEFAULTS.user &&
    String(nextConfig.password ?? '') === LEGACY_POSTGRES_DEFAULTS.password;

  if (hasLegacyPostgresDefaults) {
    delete nextConfig.host;
    delete nextConfig.port;
    delete nextConfig.db;
    delete nextConfig.database;
    delete nextConfig.user;
    delete nextConfig.password;
  }

  if (String(nextConfig.table ?? '').trim().toLowerCase() === 'fact_orders') {
    nextConfig.table = 'arrivals_ml_ready';
    nextConfig.mode = 'replace';
    delete nextConfig.host;
    delete nextConfig.port;
    delete nextConfig.db;
    delete nextConfig.database;
    delete nextConfig.user;
    delete nextConfig.password;
    delete nextConfig.database_url;
  }

  if (Array.isArray(nextConfig.records) && nextConfig.records.length === 1) {
    const [firstRecord] = nextConfig.records;
    if (firstRecord && typeof firstRecord === 'object') {
      for (const [key, value] of Object.entries(firstRecord)) {
        if (Array.isArray(value)) {
          const objectItems = value.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
          if (objectItems.length) {
            return {
              ...nextConfig,
              records: objectItems,
              records_path: `$.${key}`,
            };
          }
        }
      }
    }
  }

  return nextConfig;
}

function migrateNodes(nodes) {
  return nodes.map((node) => {
    const nextData = { ...node.data };
    const nextConfig = normalizeNodeConfig(nextData.config ?? {});
    const loweredLabel = String(nextData.label ?? '').toLowerCase();

    if (nextData.kind === 'transform' && !nextConfig.transform_type) {
      if (loweredLabel.includes('filter')) {
        nextConfig.transform_type = 'filter';
      } else if (loweredLabel === 'transform' || loweredLabel.includes('ml')) {
        nextConfig.transform_type = 'transform';
      } else if (loweredLabel.includes('join')) {
        nextConfig.transform_type = 'join';
      } else if (loweredLabel.includes('clean')) {
        nextConfig.transform_type = 'clean';
      }
    }

    if ((nextConfig.transform_type === 'join' || loweredLabel.includes('join')) && !nextConfig.join_type) {
      nextConfig.join_type = 'inner';
    }

    if (
      nextData.kind === 'transform' &&
      loweredLabel.includes('filter') &&
      (nextConfig.query === 'age > 18' || nextConfig.query === 'status = active')
    ) {
      nextData.config = {
        ...nextConfig,
        query: '',
      };
    } else {
      nextData.config = nextConfig;
    }

    return {
      ...node,
      data: nextData,
    };
  });
}

function isLegacyDemoFlow(nodes, edges) {
  if (nodes.length !== 3 || edges.length !== 2) {
    return false;
  }

  const labels = new Set(nodes.map((node) => node.data?.label));
  return (
    labels.has('PostgreSQL Source') &&
    labels.has('Filter: active_users') &&
    labels.has('S3 Bucket')
  );
}

function clampPanelWidth(value, minWidth, maxWidth) {
  return Math.min(Math.max(value, minWidth), maxWidth);
}

function buildLastRunStorageKey(flowId) {
  return `etl:last-run:${flowId}`;
}

function sanitizeFlowId(value) {
  return String(value ?? '')
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-zA-Z0-9_-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function getSelectedPipelineLabel(selectedNodeId, nodes, edges) {
  if (!selectedNodeId) {
    return null;
  }

  const adjacency = {};
  edges.forEach((edge) => {
    if (!adjacency[edge.source]) adjacency[edge.source] = [];
    if (!adjacency[edge.target]) adjacency[edge.target] = [];
    adjacency[edge.source].push(edge.target);
    adjacency[edge.target].push(edge.source);
  });

  const visited = new Set();
  const queue = [selectedNodeId];
  const sourceLabels = [];
  while (queue.length) {
    const nodeId = queue.shift();
    if (visited.has(nodeId)) continue;
    visited.add(nodeId);
    const node = nodes.find((item) => item.id === nodeId);
    if (node?.data?.kind === 'source') {
      const cleaned = String(node.data.label ?? '')
        .replace(/^(CSV|JSON|NDJSON)\s+File:\s*/i, '')
        .trim();
      if (cleaned) {
        sourceLabels.push(cleaned);
      }
    }
    (adjacency[nodeId] ?? []).forEach((nextId) => queue.push(nextId));
  }

  return sourceLabels.length ? sourceLabels.join(' + ') : null;
}

function buildPipelineOptions(nodes, edges) {
  if (!nodes.length) {
    return [];
  }

  const adjacency = {};
  nodes.forEach((node) => {
    adjacency[node.id] = [];
  });
  edges.forEach((edge) => {
    if (!adjacency[edge.source]) adjacency[edge.source] = [];
    if (!adjacency[edge.target]) adjacency[edge.target] = [];
    adjacency[edge.source].push(edge.target);
    adjacency[edge.target].push(edge.source);
  });

  const visited = new Set();
  const components = [];
  nodes.forEach((node) => {
    if (visited.has(node.id)) {
      return;
    }
    const queue = [node.id];
    const component = [];
    while (queue.length) {
      const currentId = queue.shift();
      if (visited.has(currentId)) continue;
      visited.add(currentId);
      const currentNode = nodes.find((item) => item.id === currentId);
      if (currentNode) {
        component.push(currentNode);
      }
      (adjacency[currentId] ?? []).forEach((nextId) => queue.push(nextId));
    }
    if (component.length) {
      components.push(component);
    }
  });

  return components
    .map((component, index) => {
      const sourceLabels = component
        .filter((node) => node.data?.kind === 'source')
        .map((node) => String(node.data?.label ?? '').replace(/^(CSV|JSON|NDJSON)\s+File:\s*/i, '').trim())
        .filter(Boolean);
      const representative =
        component.find((node) => node.data?.kind === 'load') ??
        component.find((node) => node.data?.kind === 'transform') ??
        component[0];
      const minX = Math.min(...component.map((node) => node.position?.x ?? 0));
      const minY = Math.min(...component.map((node) => node.position?.y ?? 0));
      return {
        key: [...component.map((node) => node.id)].sort().join('|'),
        nodeId: representative?.id ?? component[0].id,
        nodeIds: component.map((node) => node.id),
        label: sourceLabels.length ? sourceLabels.join(' + ') : `Pipeline ${index + 1}`,
        shortLabel: sourceLabels.length ? sourceLabels.join(' + ') : `Pipeline ${index + 1}`,
        minX,
        minY,
      };
    })
    .sort((a, b) => (a.minY - b.minY) || (a.minX - b.minX))
    .map((item, index) => ({
      ...item,
      title: `Pipeline ${index + 1}`,
    }));
}

function findPipelineOptionByNodeId(nodeId, pipelineOptions) {
  if (!nodeId) {
    return null;
  }
  return pipelineOptions.find((item) => item.nodeId === nodeId || item.nodeIds.includes(nodeId)) ?? null;
}

function App() {
  const [isSaving, setIsSaving] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [runningNodeId, setRunningNodeId] = useState(null);
  const [isRunDialogOpen, setIsRunDialogOpen] = useState(false);
  const [lastRunResult, setLastRunResult] = useState(null);
  const [pipelineRunResults, setPipelineRunResults] = useState({});
  const [nodeDataStep, setNodeDataStep] = useState(null);
  const [currentFlowId, setCurrentFlowId] = useState(DEFAULT_FLOW_ID);
  const [availableFlows, setAvailableFlows] = useState([]);
  const [isLoadDialogOpen, setIsLoadDialogOpen] = useState(false);
  const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false);
  const [loadSelection, setLoadSelection] = useState(DEFAULT_FLOW_ID);
  const [saveFlowName, setSaveFlowName] = useState(DEFAULT_FLOW_ID);
  const [activePipelineNodeId, setActivePipelineNodeId] = useState(null);
  const [leftPanelWidth, setLeftPanelWidth] = useState(280);
  const [rightPanelWidth, setRightPanelWidth] = useState(320);
  const [draggingPanel, setDraggingPanel] = useState(null);
  const [isCompactLayout, setIsCompactLayout] = useState(() => window.innerWidth <= 1160);
  const isHydratedRef = useRef(false);
  const mainGridRef = useRef(null);

  const nodes = useFlowStore((state) => state.nodes);
  const edges = useFlowStore((state) => state.edges);
  const selectedNodeId = useFlowStore((state) => state.selectedNodeId);
  const setFlow = useFlowStore((state) => state.setFlow);
  const clearCanvas = useFlowStore((state) => state.clearCanvas);
  const updateNodeById = useFlowStore((state) => state.updateNodeById);
  const statusMessage = useFlowStore((state) => state.statusMessage);
  const setStatusMessage = useFlowStore((state) => state.setStatusMessage);
  const pipelineOptions = useMemo(() => buildPipelineOptions(nodes, edges), [nodes, edges]);
  const selectedPipelineLabel = useMemo(() => {
    const option = findPipelineOptionByNodeId(activePipelineNodeId, pipelineOptions);
    return option?.label ?? null;
  }, [activePipelineNodeId, pipelineOptions]);
  const activePipelineOption = useMemo(
    () => findPipelineOptionByNodeId(activePipelineNodeId, pipelineOptions),
    [activePipelineNodeId, pipelineOptions],
  );
  const visibleRunResult = useMemo(() => {
    if (selectedNodeId && pipelineRunResults[selectedNodeId]) {
      return pipelineRunResults[selectedNodeId];
    }
    if (activePipelineOption?.nodeId && pipelineRunResults[activePipelineOption.nodeId]) {
      return pipelineRunResults[activePipelineOption.nodeId];
    }
    return lastRunResult;
  }, [activePipelineOption, lastRunResult, pipelineRunResults, selectedNodeId]);

  useEffect(() => {
    if (activePipelineNodeId && !pipelineOptions.some((item) => item.nodeId === activePipelineNodeId)) {
      setActivePipelineNodeId(null);
    }
  }, [activePipelineNodeId, pipelineOptions]);

  const refreshFlowList = useCallback(async (preferredFlowId = null) => {
    const flows = await listFlows();
    const nextFlows = flows.length ? flows : [DEFAULT_FLOW_ID];
    setAvailableFlows(nextFlows);
    setLoadSelection((currentSelection) => {
      if (preferredFlowId) {
        return preferredFlowId;
      }

      return nextFlows.includes(currentSelection) ? currentSelection : nextFlows[0];
    });

    return nextFlows;
  }, []);

  useEffect(() => {
    const onResize = () => {
      setIsCompactLayout(window.innerWidth <= 1160);
    };

    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  useEffect(() => {
    if (!draggingPanel || isCompactLayout) {
      return undefined;
    }

    const onMouseMove = (event) => {
      const rect = mainGridRef.current?.getBoundingClientRect();
      if (!rect) {
        return;
      }

      if (draggingPanel === 'left') {
        const maxWidth = Math.min(LEFT_PANEL_MAX_WIDTH, rect.width - rightPanelWidth - 180);
        setLeftPanelWidth(clampPanelWidth(event.clientX - rect.left, LEFT_PANEL_MIN_WIDTH, maxWidth));
        return;
      }

      const maxWidth = Math.min(RIGHT_PANEL_MAX_WIDTH, rect.width - leftPanelWidth - 180);
      setRightPanelWidth(clampPanelWidth(rect.right - event.clientX, RIGHT_PANEL_MIN_WIDTH, maxWidth));
    };

    const onMouseUp = () => {
      setDraggingPanel(null);
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);

    return () => {
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, [draggingPanel, isCompactLayout, leftPanelWidth, rightPanelWidth]);

  const onSave = async () => {
    const targetFlowId = sanitizeFlowId(saveFlowName) || currentFlowId || DEFAULT_FLOW_ID;
    setIsSaving(true);

    try {
      await saveFlow(targetFlowId, { nodes, edges });
      setCurrentFlowId(targetFlowId);
      setSaveFlowName(targetFlowId);
      setIsSaveDialogOpen(false);
      await refreshFlowList(targetFlowId);
      setStatusMessage(`Flow erfolgreich gespeichert: ${targetFlowId}`);
    } catch (error) {
      setStatusMessage(error.message);
    } finally {
      setIsSaving(false);
    }
  };

  const onLoad = async () => {
    const targetFlowId = sanitizeFlowId(loadSelection) || currentFlowId || DEFAULT_FLOW_ID;
    setIsLoading(true);

    try {
      const payload = await loadFlow(targetFlowId);
      setFlow(payload.nodes, payload.edges);
      setCurrentFlowId(targetFlowId);
      setSaveFlowName(targetFlowId);
      setIsLoadDialogOpen(false);
      setLastRunResult(null);
      setPipelineRunResults({});
      setNodeDataStep(null);
      const storageKey = buildLastRunStorageKey(targetFlowId);
      const cachedRunText = window.localStorage.getItem(storageKey);
      if (cachedRunText) {
        const cachedRun = JSON.parse(cachedRunText);
        setLastRunResult(cachedRun);
        setPipelineRunResults(
          Object.fromEntries((cachedRun.node_results ?? []).map((step) => [step.node_id, cachedRun])),
        );
      } else {
        setLastRunResult(null);
      }
      setStatusMessage(`Flow aus dem Backend geladen: ${targetFlowId}`);
    } catch (error) {
      setStatusMessage(error.message);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    const bootstrapFlow = async () => {
      setIsLoading(true);
      try {
        await refreshFlowList(DEFAULT_FLOW_ID);
        const cachedRunText = window.localStorage.getItem(buildLastRunStorageKey(DEFAULT_FLOW_ID));
        if (cachedRunText) {
          const cachedRun = JSON.parse(cachedRunText);
          setLastRunResult(cachedRun);
          setPipelineRunResults(
            Object.fromEntries((cachedRun.node_results ?? []).map((step) => [step.node_id, cachedRun])),
          );
        }

        const payload = await loadFlow(DEFAULT_FLOW_ID, { allowNotFound: true });
        if (payload) {
          if (isLegacyDemoFlow(payload.nodes, payload.edges)) {
            setFlow([], []);
            setStatusMessage('Leeres Dashboard gestartet. Demo-Flow wurde verworfen.');
          } else {
            setFlow(migrateNodes(payload.nodes), payload.edges);
            setStatusMessage('Gespeicherte Pipeline automatisch geladen.');
          }
        }
      } catch {
        setStatusMessage('Pipeline konnte beim Start nicht geladen werden.');
      } finally {
        isHydratedRef.current = true;
        setIsLoading(false);
      }
    };

    bootstrapFlow();
  }, [refreshFlowList, setFlow, setStatusMessage]);

  useEffect(() => {
    if (!isHydratedRef.current) {
      return undefined;
    }

    const timer = window.setTimeout(async () => {
      try {
        await saveFlow(currentFlowId, { nodes, edges });
      } catch {
        // Kein UI-Fehler bei Auto-Save, manuelles Speichern bleibt moeglich.
      }
    }, 900);

    return () => window.clearTimeout(timer);
  }, [currentFlowId, nodes, edges]);

  const onNodeOpenData = (nodeId) => {
    const runResultForNode = pipelineRunResults[nodeId] ?? lastRunResult;
    const stepFromRun = runResultForNode?.node_results?.find((item) => item.node_id === nodeId);
    if (stepFromRun) {
      setIsRunDialogOpen(false);
      setNodeDataStep(stepFromRun);
      return;
    }

    const clickedNode = nodes.find((node) => node.id === nodeId);
    if (!clickedNode) {
      setStatusMessage('Node konnte nicht gefunden werden.');
      return;
    }

    const fallbackStep = buildFallbackStep(clickedNode);
    setIsRunDialogOpen(false);
    setNodeDataStep(fallbackStep);

    if (!fallbackStep.row_count) {
      setStatusMessage('Fuer diesen Node sind aktuell keine Datensaetze vorhanden. Bitte Pipeline ausfuehren.');
    }
  };

  const executePipeline = async (targetNodeId = null) => {
    setIsRunning(true);
    setRunningNodeId(targetNodeId);

    try {
      const normalizedNodes = migrateNodes(nodes);
      const effectiveTargetNodeId = targetNodeId ?? activePipelineNodeId ?? null;
      const effectivePipelineLabel = getSelectedPipelineLabel(effectiveTargetNodeId, nodes, edges);
      const result = await runFlow({
        nodes: normalizedNodes,
        edges,
        target_node_id: effectiveTargetNodeId,
      });
      setLastRunResult(result);
      setPipelineRunResults((current) => ({
        ...current,
        ...Object.fromEntries((result.node_results ?? []).map((step) => [step.node_id, result])),
      }));
      window.localStorage.setItem(buildLastRunStorageKey(currentFlowId), JSON.stringify(result));
      const loadSummary = result.loads
        .map((load) => `${load.target}: ${load.row_count} rows (${load.storage})`)
        .join(' | ');
      const warningText = result.warnings.length ? ` Warnungen: ${result.warnings.join(' ; ')}` : '';
      const pipelineText = effectivePipelineLabel ? ` Pipeline: ${effectivePipelineLabel}.` : '';
      if (loadSummary) {
        setStatusMessage(
          `Run erfolgreich:${pipelineText} ${result.rows_processed} rows verarbeitet. ${loadSummary}.${warningText}`,
        );
      } else {
        setStatusMessage(
          `Run abgeschlossen:${pipelineText} Kein Load-Node gefunden, nur Transform ausgefuehrt.${warningText}`,
        );
      }

      if (targetNodeId) {
        setIsRunDialogOpen(false);
      } else {
        setNodeDataStep(null);
        setIsRunDialogOpen(true);
      }
    } catch (error) {
      setStatusMessage(error.message);
    } finally {
      setIsRunning(false);
      setRunningNodeId(null);
    }
  };

  const onRun = async () => {
    await executePipeline();
  };

  const onRunNode = async (nodeId) => {
    await executePipeline(nodeId);
  };

  const onUploadSourceFile = async (nodeId, file) => {
    try {
      const parsedFile = await parseUploadedFile(file);
      updateNodeById(nodeId, {
        label: parsedFile.label,
        config: parsedFile.config,
      });

      const nextStep = {
        node_id: nodeId,
        label: parsedFile.label,
        kind: 'source',
        row_count: parsedFile.config.records?.length ?? 0,
        records: parsedFile.config.records ?? [],
        preview: (parsedFile.config.records ?? []).slice(0, 5),
        storage: 'local-node-config',
        config: parsedFile.config,
        source_type: 'file',
      };

      setNodeDataStep(nextStep);
      setStatusMessage(`Datei in Source geladen: ${file.name}`);
    } catch (error) {
      setStatusMessage(error.message);
    }
  };

  const onLoadPostgresSource = async (nodeId) => {
    const sourceNode = nodes.find((node) => node.id === nodeId);
    if (!sourceNode) {
      setStatusMessage('PostgreSQL Source konnte nicht gefunden werden.');
      return;
    }

    try {
      const result = await previewPostgresSource(sourceNode.data.config ?? {});
      const nextConfig = {
        ...(sourceNode.data.config ?? {}),
        ...result.config,
      };

      updateNodeById(nodeId, {
        label: result.label ?? sourceNode.data.label,
        config: nextConfig,
      });

      setNodeDataStep({
        node_id: nodeId,
        label: result.label ?? sourceNode.data.label,
        kind: 'source',
        row_count: result.row_count ?? nextConfig.records?.length ?? 0,
        records: nextConfig.records ?? [],
        preview: nextConfig.sample_rows ?? (nextConfig.records ?? []).slice(0, 5),
        storage: 'postgres-source-preview',
        config: nextConfig,
        source_type: 'postgres',
      });
      setStatusMessage(`PostgreSQL Source geladen: ${result.row_count} rows.`);
    } catch (error) {
      setStatusMessage(error.message);
    }
  };

  const onClearCanvas = () => {
    clearCanvas();
    setIsRunDialogOpen(false);
    setNodeDataStep(null);
    setLastRunResult(null);
    setPipelineRunResults({});
    window.localStorage.removeItem(buildLastRunStorageKey(currentFlowId));
  };

  return (
    <div className="layout">
      <header className="topbar">
        <div>
          <h1>ETL Flow Studio</h1>
          <p>
            Drag-and-Drop Dashboard fuer Source, Transform und Load Pipelines | Flow: {currentFlowId}
            {' | '}Auswahl: {selectedPipelineLabel ?? 'alle Pipelines'}
          </p>
        </div>

        <div className="actions">
          <label className="toolbar-select" htmlFor="pipeline-run-select" aria-label="Pipeline auswählen">
            <select
              id="pipeline-run-select"
              value={activePipelineNodeId ?? ''}
              onChange={(event) => setActivePipelineNodeId(event.target.value || null)}
            >
              <option value="">Alle Pipelines</option>
              {pipelineOptions.map((option) => (
                <option key={option.nodeId} value={option.nodeId}>
                  {option.title}: {option.shortLabel}
                </option>
              ))}
            </select>
          </label>
          <button
            className="action-btn"
            onClick={onRun}
            disabled={isRunning}
            title={selectedPipelineLabel ? `Fuehrt nur diese Pipeline aus: ${selectedPipelineLabel}` : 'Fuehrt alle getrennten Pipelines im aktuellen Flow aus'}
          >
            <Play size={16} />
            {isRunning ? 'Run...' : selectedPipelineLabel ? 'Selektierte Pipeline ausfuehren' : 'Alle Pipelines ausfuehren'}
          </button>
          <button
            className="action-btn"
            onClick={() => {
              setNodeDataStep(null);
              setIsRunDialogOpen(true);
            }}
            disabled={!visibleRunResult}
          >
            <Eye size={16} />
            Step-Daten
          </button>
          <button className="action-btn action-btn--danger" onClick={onClearCanvas}>
            <Trash2 size={16} />
            Alles loeschen
          </button>
          <button
            className="action-btn"
            onClick={() => {
              setLoadSelection(currentFlowId);
              setIsLoadDialogOpen(true);
              void refreshFlowList(currentFlowId);
            }}
            disabled={isLoading}
          >
            <Download size={16} />
            {isLoading ? 'Laden...' : 'Flow laden'}
          </button>
          <button
            className="action-btn action-btn--primary"
            onClick={() => {
              setSaveFlowName(currentFlowId);
              setIsSaveDialogOpen(true);
            }}
            disabled={isSaving}
          >
            <Save size={16} />
            {isSaving ? 'Speichern...' : 'Flow speichern'}
          </button>
        </div>
      </header>

      <div className="status-line">{statusMessage}</div>

      {isLoadDialogOpen ? (
        <div className="dialog-overlay" role="presentation" onClick={() => setIsLoadDialogOpen(false)}>
          <section
            className="dialog-card dialog-card--compact"
            role="dialog"
            aria-modal="true"
            aria-label="Flow laden"
            onClick={(event) => event.stopPropagation()}
          >
            <header className="dialog-header">
              <h2>Flow laden</h2>
              <button className="dialog-close" type="button" onClick={() => setIsLoadDialogOpen(false)}>
                Schliessen
              </button>
            </header>

            <label className="field" htmlFor="flow-select">
              <span>Gespeicherter Flow</span>
              <select
                id="flow-select"
                className="select-field"
                value={loadSelection}
                onChange={(event) => setLoadSelection(event.target.value)}
              >
                {availableFlows.map((flowId) => (
                  <option key={flowId} value={flowId}>
                    {flowId}
                  </option>
                ))}
              </select>
            </label>

            <div className="dialog-footer dialog-footer--single">
              <button className="step-action-btn" type="button" onClick={() => refreshFlowList(loadSelection)}>
                Liste aktualisieren
              </button>
              <button className="step-action-btn step-action-btn--primary" type="button" onClick={onLoad} disabled={isLoading}>
                {isLoading ? 'Laden...' : 'Flow laden'}
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {isSaveDialogOpen ? (
        <div className="dialog-overlay" role="presentation" onClick={() => setIsSaveDialogOpen(false)}>
          <section
            className="dialog-card dialog-card--compact"
            role="dialog"
            aria-modal="true"
            aria-label="Flow speichern"
            onClick={(event) => event.stopPropagation()}
          >
            <header className="dialog-header">
              <h2>Flow speichern</h2>
              <button className="dialog-close" type="button" onClick={() => setIsSaveDialogOpen(false)}>
                Schliessen
              </button>
            </header>

            <label className="field" htmlFor="flow-name">
              <span>Dateiname / Flow-ID</span>
              <input
                id="flow-name"
                value={saveFlowName}
                onChange={(event) => setSaveFlowName(event.target.value)}
                placeholder="z. B. flights-pipeline"
              />
            </label>

            <p className="dialog-help-text">Gespeichert wird unter: {sanitizeFlowId(saveFlowName) || 'ungueltiger-name'}</p>

            <div className="dialog-footer dialog-footer--single">
              <button className="step-action-btn" type="button" onClick={() => setSaveFlowName(currentFlowId)}>
                Aktuellen Namen uebernehmen
              </button>
              <button className="step-action-btn step-action-btn--primary" type="button" onClick={onSave} disabled={isSaving}>
                {isSaving ? 'Speichern...' : 'Flow speichern'}
              </button>
            </div>
          </section>
        </div>
      ) : null}

      <main
        ref={mainGridRef}
        className="main-grid"
        style={
          isCompactLayout
            ? undefined
            : { gridTemplateColumns: `${leftPanelWidth}px 10px minmax(0, 1fr) 10px ${rightPanelWidth}px` }
        }
      >
        <Sidebar />
        <div
          className="panel-resizer"
          role="separator"
          aria-orientation="vertical"
          aria-label="Sidebar Breite anpassen"
          onMouseDown={() => setDraggingPanel('left')}
        />
        <FlowCanvas
          activePipelineNodeId={activePipelineNodeId}
          onNodeOpenData={onNodeOpenData}
          onRunNode={onRunNode}
          runningNodeId={runningNodeId}
        />
        <div
          className="panel-resizer"
          role="separator"
          aria-orientation="vertical"
          aria-label="Properties Breite anpassen"
          onMouseDown={() => setDraggingPanel('right')}
        />
        <PropertiesPanel lastRunResult={visibleRunResult} onOpenNodeData={onNodeOpenData} />
      </main>

      <RunDetailsDialog
        open={isRunDialogOpen}
        onClose={() => setIsRunDialogOpen(false)}
        onOpenStep={(step) => {
          setIsRunDialogOpen(false);
          setNodeDataStep(step);
        }}
        pipelineOptions={pipelineOptions}
        runResult={visibleRunResult}
      />

      <NodeDataDialog
        key={nodeDataStep?.node_id ?? 'none'}
        step={nodeDataStep}
        onClose={() => setNodeDataStep(null)}
        onUploadSourceFile={onUploadSourceFile}
        onLoadPostgresSource={onLoadPostgresSource}
      />
    </div>
  );
}

export default App;
