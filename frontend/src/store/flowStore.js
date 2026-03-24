import { create } from 'zustand';
import { addEdge, applyEdgeChanges, applyNodeChanges } from 'reactflow';

const initialNodes = [];

const initialEdges = [];

export const paletteNodes = [
  {
    kind: 'source',
    label: 'PostgreSQL Source',
    templateConfig: {
      source_type: 'postgres',
      schema: 'public',
      table: '',
      limit: 200,
    },
  },
  {
    kind: 'source',
    label: 'CSV Source',
    templateConfig: {
      source_type: 'file',
      format: 'csv',
      file_name: '',
      records: [],
    },
  },
  {
    kind: 'transform',
    label: 'Transform',
    templateConfig: {
      transform_type: 'transform',
      query: '',
      join_enabled: false,
      auto_join: true,
      auto_join_type: 'left',
      left_key: 'id',
      right_key: 'id',
      join_type: 'inner',
      flatten_json: true,
      normalize_column_names: true,
      parse_datetime: true,
      normalize_booleans: true,
      drop_constant_columns: true,
      drop_id_columns: true,
      trim_strings: true,
      convert_numeric: true,
      drop_empty_rows: true,
      drop_empty_columns: true,
      drop_duplicates: true,
      keep_columns: [],
      drop_columns: [],
      numeric_columns: [],
      categorical_columns: [],
      target_column: '',
      impute_numeric: 'auto',
      impute_categorical: 'auto',
      categorical_fill_value: 'missing',
      encode_categorical: 'none',
      scale_numeric: 'none',
    },
  },
  {
    kind: 'load',
    label: 'S3 Bucket',
    templateConfig: { bucket: 'etl-results' },
  },
  {
    kind: 'load',
    label: 'PostgreSQL Load',
    templateConfig: {
      table: 'arrivals_ml_ready',
      schema: 'public',
      mode: 'replace',
      primary_key: '',
      foreign_keys: '',
      host: '',
      port: 5432,
      db: '',
      user: '',
      password: '',
      database_url: '',
    },
  },
];

// Leitet (derives) Tabellennamen aus Source-Label ab:
// "JSON File: flights_raw.json" -> "flights_raw"
// "CSV File: analysis_20260218.csv" -> "analysis_20260218"
function deriveTableName(label) {
  return label
    .replace(/^(CSV|JSON|NDJSON)\s+File:\s*/i, '')
    .replace(/\.[a-z0-9]+$/i, '')
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toLowerCase() || 'etl_output';
}

// Traversiert (traverses) Edges rueckwaerts und sammelt alle Source-Labels
function getUpstreamSourceLabels(nodeId, nodes, edges) {
  const parentMap = {};
  edges.forEach((e) => {
    if (!parentMap[e.target]) parentMap[e.target] = [];
    parentMap[e.target].push(e.source);
  });
  const visited = new Set();
  const queue = [nodeId];
  const labels = [];
  while (queue.length) {
    const id = queue.shift();
    if (visited.has(id)) continue;
    visited.add(id);
    const node = nodes.find((n) => n.id === id);
    if (node?.data.kind === 'source') {
      labels.push(node.data.label ?? 'source');
    }
    (parentMap[id] ?? []).forEach((pid) => queue.push(pid));
  }
  return labels;
}

// Gibt alle downstream Load-Node-IDs eines Source-Nodes zurueck
function getDownstreamLoadIds(nodeId, nodes, edges) {
  const childMap = {};
  edges.forEach((e) => {
    if (!childMap[e.source]) childMap[e.source] = [];
    childMap[e.source].push(e.target);
  });
  const visited = new Set();
  const queue = [nodeId];
  const loadIds = [];
  while (queue.length) {
    const id = queue.shift();
    if (visited.has(id)) continue;
    visited.add(id);
    const node = nodes.find((n) => n.id === id);
    if (node?.data.kind === 'load') {
      loadIds.push(id);
    }
    (childMap[id] ?? []).forEach((cid) => queue.push(cid));
  }
  return loadIds;
}

// Berechnet (computes) neuen Tabellennamen aus Source-Labels
function computeTableName(labels) {
  if (!labels.length) return '';
  return labels.map(deriveTableName).join('_');
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function getNextLabelIndex(baseLabel, nodes) {
  const pattern = new RegExp(`^${escapeRegExp(baseLabel)}(?:\\s+(\\d+))?$`);
  let maxIndex = 0;

  nodes.forEach((node) => {
    const label = String(node.data?.label ?? '');
    const match = label.match(pattern);
    if (!match) return;
    const currentIndex = match[1] ? Number.parseInt(match[1], 10) : 1;
    if (currentIndex > maxIndex) {
      maxIndex = currentIndex;
    }
  });

  return maxIndex + 1;
}

export const useFlowStore = create((set, get) => ({
  nodes: initialNodes,
  edges: initialEdges,
  selectedNodeId: null,
  pendingFocusNodeId: null,
  canvasViewport: { x: 0, y: 0, zoom: 1 },
  canvasSize: { width: 0, height: 0 },
  statusMessage: 'Bereit: Ziehe einen Node auf das Canvas.',

  onNodesChange: (changes) => {
    set((state) => ({
      nodes: applyNodeChanges(changes, state.nodes),
    }));
  },

  onEdgesChange: (changes) => {
    set((state) => ({
      edges: applyEdgeChanges(changes, state.edges),
    }));
  },

  onConnect: (connection) => {
    set((state) => {
      const newEdge = {
        ...connection,
        id: `e-${connection.source}-${connection.target}-${Date.now()}`,
        animated: true,
      };
      const nextEdges = addEdge(newEdge, state.edges);

      // Automatisch (automatically) Tabellennamen der downstream Load-Nodes aktualisieren
      const targetNode = state.nodes.find((n) => n.id === connection.target);
      let nextNodes = state.nodes;

      // Alle Load-Nodes im Pfad ab dem Zielpunkt aktualisieren
      const startId = connection.target ?? '';
      const idsToUpdate = [];
      if (targetNode?.data.kind === 'load') {
        idsToUpdate.push(startId);
      } else {
        // Transform → finde alle downstream Loads
        getDownstreamLoadIds(startId, state.nodes, nextEdges).forEach((id) => idsToUpdate.push(id));
      }

      if (idsToUpdate.length) {
        nextNodes = state.nodes.map((node) => {
          if (!idsToUpdate.includes(node.id)) return node;
          const srcLabels = getUpstreamSourceLabels(node.id, state.nodes, nextEdges);
          const tableName = computeTableName(srcLabels);
          if (!tableName) return node;
          return {
            ...node,
            data: {
              ...node.data,
              config: { ...node.data.config, table: tableName },
            },
          };
        });
      }

      return { edges: nextEdges, nodes: nextNodes };
    });
  },

  addNode: (node) => {
    set((state) => {
      // Automatisch eindeutigen (unique) Label + Tabellennamen für Load-Nodes generieren
      let nodeToAdd = node;
      const nextLabelIndex = getNextLabelIndex(node.data?.label ?? '', state.nodes);
      if (nextLabelIndex > 1) {
        const idx = nextLabelIndex;
        const newLabel = `${node.data.label} ${idx}`;
        const newConfig = { ...node.data.config };
        // Tabellenname (table name) eindeutig machen
        if (newConfig.table) {
          newConfig.table = `${newConfig.table}_${idx}`;
        }
        nodeToAdd = {
          ...node,
          data: { ...node.data, label: newLabel, config: newConfig },
        };
      }
      return {
        nodes: [...state.nodes, nodeToAdd],
        pendingFocusNodeId: nodeToAdd.id,
      };
    });
  },

  selectNode: (nodeId) => {
    set({ selectedNodeId: nodeId });
  },

  updateSelectedNode: (patch) => {
    const selectedNodeId = get().selectedNodeId;
    if (!selectedNodeId) {
      return;
    }

    set((state) => ({
      nodes: state.nodes.map((node) => {
        if (node.id !== selectedNodeId) {
          return node;
        }

        return {
          ...node,
          data: {
            ...node.data,
            ...patch,
            config: {
              ...node.data.config,
              ...(patch.config ?? {}),
            },
          },
        };
      }),
    }));
  },

  updateNodeById: (nodeId, patch) => {
    set((state) => {
      const updatedNodes = state.nodes.map((node) => {
        if (node.id !== nodeId) return node;
        return {
          ...node,
          data: {
            ...node.data,
            ...patch,
            config: {
              ...node.data.config,
              ...(patch.config ?? {}),
            },
          },
        };
      });

      // Wenn eine Source (kind=source) ein neues Label bekommt (Datei geladen),
      // dann alle downstream Load-Nodes automatisch mit neuem Tabellennamen updaten
      const updatedNode = updatedNodes.find((n) => n.id === nodeId);
      if (updatedNode?.data.kind === 'source' && patch.label) {
        const loadIds = getDownstreamLoadIds(nodeId, updatedNodes, state.edges);
        const finalNodes = updatedNodes.map((node) => {
          if (!loadIds.includes(node.id)) return node;
          const srcLabels = getUpstreamSourceLabels(node.id, updatedNodes, state.edges);
          const tableName = computeTableName(srcLabels);
          if (!tableName) return node;
          return {
            ...node,
            data: {
              ...node.data,
              config: { ...node.data.config, table: tableName },
            },
          };
        });
        return { nodes: finalNodes };
      }

      return { nodes: updatedNodes };
    });
  },

  deleteSelectedNode: () => {
    const selectedNodeId = get().selectedNodeId;
    if (!selectedNodeId) {
      return;
    }

    set((state) => ({
      nodes: state.nodes.filter((node) => node.id !== selectedNodeId),
      edges: state.edges.filter(
        (edge) => edge.source !== selectedNodeId && edge.target !== selectedNodeId,
      ),
      selectedNodeId: null,
      statusMessage: 'Node wurde geloescht.',
    }));
  },

  deleteEdgeById: (edgeId) => {
    set((state) => ({
      edges: state.edges.filter((edge) => edge.id !== edgeId),
      statusMessage: 'Edge wurde geloescht.',
    }));
  },

  clearCanvas: () => {
    set({
      nodes: [],
      edges: [],
      selectedNodeId: null,
      pendingFocusNodeId: null,
      statusMessage: 'Canvas wurde geleert.',
    });
  },

  setFlow: (nodes, edges) => {
    set({
      nodes,
      edges,
      selectedNodeId: null,
      pendingFocusNodeId: null,
    });
  },

  setStatusMessage: (statusMessage) => {
    set({ statusMessage });
  },

  setCanvasViewport: (canvasViewport) => {
    set({ canvasViewport });
  },

  setCanvasSize: (canvasSize) => {
    set({ canvasSize });
  },

  clearPendingFocusNode: () => {
    set({ pendingFocusNodeId: null });
  },
}));
