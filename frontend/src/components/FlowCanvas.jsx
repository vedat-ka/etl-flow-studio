import { useCallback, useEffect, useMemo, useRef } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  useReactFlow,
} from 'reactflow';
import 'reactflow/dist/style.css';
import ETLNode from './ETLNode';
import { useFlowStore } from '../store/flowStore';

const nodeTypes = {
  etl: ETLNode,
};

function hasCycle(nodes, edges, candidateEdge) {
  const adjacency = new Map(nodes.map((node) => [node.id, []]));

  for (const edge of [...edges, candidateEdge]) {
    adjacency.get(edge.source)?.push(edge.target);
  }

  const visited = new Set();
  const stack = new Set();

  const dfs = (nodeId) => {
    if (stack.has(nodeId)) {
      return true;
    }
    if (visited.has(nodeId)) {
      return false;
    }

    visited.add(nodeId);
    stack.add(nodeId);

    for (const nextNodeId of adjacency.get(nodeId) ?? []) {
      if (dfs(nextNodeId)) {
        return true;
      }
    }

    stack.delete(nodeId);
    return false;
  };

  for (const node of nodes) {
    if (dfs(node.id)) {
      return true;
    }
  }

  return false;
}

function FlowInner({ activePipelineNodeId, onNodeOpenData, onRunNode, runningNodeId }) {
  const nodes = useFlowStore((state) => state.nodes);
  const edges = useFlowStore((state) => state.edges);
  const canvasViewport = useFlowStore((state) => state.canvasViewport);
  const pendingFocusNodeId = useFlowStore((state) => state.pendingFocusNodeId);
  const onNodesChange = useFlowStore((state) => state.onNodesChange);
  const onEdgesChange = useFlowStore((state) => state.onEdgesChange);
  const onConnect = useFlowStore((state) => state.onConnect);
  const addNode = useFlowStore((state) => state.addNode);
  const clearPendingFocusNode = useFlowStore((state) => state.clearPendingFocusNode);
  const deleteEdgeById = useFlowStore((state) => state.deleteEdgeById);
  const selectNode = useFlowStore((state) => state.selectNode);
  const setCanvasViewport = useFlowStore((state) => state.setCanvasViewport);
  const setCanvasSize = useFlowStore((state) => state.setCanvasSize);
  const setStatusMessage = useFlowStore((state) => state.setStatusMessage);
  const reactFlow = useReactFlow();
  const canvasRef = useRef(null);

  useEffect(() => {
    setCanvasViewport(reactFlow.getViewport());
  }, [reactFlow, setCanvasViewport]);

  useEffect(() => {
    const element = canvasRef.current;
    if (!element) {
      return undefined;
    }

    const updateCanvasSize = () => {
      setCanvasSize({
        width: element.clientWidth,
        height: element.clientHeight,
      });
    };

    updateCanvasSize();

    const observer = new ResizeObserver(() => {
      updateCanvasSize();
    });

    observer.observe(element);
    return () => observer.disconnect();
  }, [setCanvasSize]);

  useEffect(() => {
    if (!pendingFocusNodeId) {
      return;
    }

    const nextNode = nodes.find((node) => node.id === pendingFocusNodeId);
    if (!nextNode) {
      return;
    }

    const viewport = reactFlow.getViewport();
    const nodeWidth = nextNode.width ?? 220;
    const nodeHeight = nextNode.height ?? 96;

    reactFlow.setCenter(nextNode.position.x + nodeWidth / 2, nextNode.position.y + nodeHeight / 2, {
      zoom: viewport.zoom,
      duration: 260,
    });

    clearPendingFocusNode();
  }, [clearPendingFocusNode, nodes, pendingFocusNodeId, reactFlow]);

  const isValidConnection = useCallback(
    (connection) => {
      if (!connection.source || !connection.target) {
        return false;
      }

      const sourceNode = nodes.find((node) => node.id === connection.source);
      const targetNode = nodes.find((node) => node.id === connection.target);

      if (!sourceNode || !targetNode) {
        return false;
      }

      if (sourceNode.data.kind === 'load' || targetNode.data.kind === 'source') {
        setStatusMessage('Ungültige Verbindung: SOURCE darf kein Ziel sein, LOAD kein Ausgang.');
        return false;
      }

      if (hasCycle(nodes, edges, connection)) {
        setStatusMessage('Ungültige Verbindung: Zyklen sind im ETL-Flow nicht erlaubt.');
        return false;
      }

      return true;
    },
    [nodes, edges, setStatusMessage],
  );

  const onConnectWithValidation = useCallback(
    (connection) => {
      if (!isValidConnection(connection)) {
        return;
      }

      onConnect(connection);
      setStatusMessage('Verbindung erstellt.');
    },
    [isValidConnection, onConnect, setStatusMessage],
  );

  const onDragOver = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event) => {
      event.preventDefault();
      const templateText = event.dataTransfer.getData('application/reactflow');

      if (!templateText) {
        return;
      }

      const template = JSON.parse(templateText);
      const position = reactFlow.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      addNode({
        id: `${template.kind}-${Date.now()}`,
        type: 'etl',
        position,
        data: {
          kind: template.kind,
          label: template.label,
          config: template.templateConfig,
        },
      });

      setStatusMessage(`Node hinzugefügt: ${template.label}`);
    },
    [addNode, reactFlow, setStatusMessage],
  );

  const onNodeClick = useCallback(
    (_event, node) => {
      selectNode(node.id);
    },
    [selectNode],
  );

  const onPaneClick = useCallback(() => {
    selectNode(null);
  }, [selectNode]);

  const onMove = useCallback(
    (_event, viewport) => {
      setCanvasViewport(viewport);
    },
    [setCanvasViewport],
  );

  const onEdgeClick = useCallback(
    (event, edge) => {
      event.preventDefault();
      deleteEdgeById(edge.id);
    },
    [deleteEdgeById],
  );

  const nodesWithActions = useMemo(() => {
    // Berechne (compute) Pipeline-Name fuer jeden Node via BFS ueber Edges
    const parentMap = {};
    edges.forEach((edge) => {
      if (!parentMap[edge.target]) parentMap[edge.target] = [];
      parentMap[edge.target].push(edge.source);
    });

    const getPipelineName = (startId) => {
      const visited = new Set();
      const queue = [startId];
      const labels = [];
      while (queue.length) {
        const id = queue.shift();
        if (visited.has(id)) continue;
        visited.add(id);
        const n = nodes.find((x) => x.id === id);
        if (n?.data.kind === 'source') {
          // Dateiname kuerzen (shorten): "JSON File: flights.json" -> "flights.json"
          const lbl = n.data.label ?? 'Source';
          labels.push(lbl.replace(/^(CSV|JSON)\s+File:\s*/i, ''));
        }
        (parentMap[id] ?? []).forEach((pid) => queue.push(pid));
      }
      return labels.length ? labels.join(' + ') : null;
    };

    return nodes.map((node) => ({
      ...node,
      data: {
        ...node.data,
        nodeId: node.id,
        onRunNode,
        onOpenNodeData: onNodeOpenData,
          onSelectNode: selectNode,
        isRunningNode: runningNodeId === node.id,
        pipelineName: node.data.kind !== 'source' ? getPipelineName(node.id) : null,
      },
    }));
  }, [nodes, edges, onNodeOpenData, onRunNode, runningNodeId, selectNode]);

  const pipelineGroups = useMemo(() => {
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
    const groups = [];
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

      if (!component.length) {
        return;
      }

      const sourceLabels = component
        .filter((item) => item.data?.kind === 'source')
        .map((item) => String(item.data?.label ?? '').replace(/^(CSV|JSON|NDJSON)\s+File:\s*/i, '').trim())
        .filter(Boolean);
      const minX = Math.min(...component.map((item) => item.position?.x ?? 0));
      const minY = Math.min(...component.map((item) => item.position?.y ?? 0));
      groups.push({
        nodeIds: component.map((item) => item.id),
        label: sourceLabels.length ? sourceLabels.join(' + ') : 'Pipeline',
        minX,
        minY,
      });
    });

    return groups
      .sort((a, b) => (a.minY - b.minY) || (a.minX - b.minX))
      .map((group, index) => ({
        ...group,
        isActive: activePipelineNodeId ? group.nodeIds.includes(activePipelineNodeId) : false,
        title: `Pipeline ${index + 1}`,
        screenLeft: group.minX * canvasViewport.zoom + canvasViewport.x,
        screenTop: group.minY * canvasViewport.zoom + canvasViewport.y - 34,
      }));
  }, [activePipelineNodeId, canvasViewport.x, canvasViewport.y, canvasViewport.zoom, edges, nodes]);

  return (
    <div ref={canvasRef} className="canvas">
      <div className="canvas__pipeline-layer" aria-hidden="true">
        {pipelineGroups.map((group) => (
          <div
            key={group.nodeIds.join('-')}
            className={`canvas__pipeline-chip${group.isActive ? ' canvas__pipeline-chip--active' : ''}`}
            style={{
              left: Math.max(12, group.screenLeft),
              top: Math.max(12, group.screenTop),
            }}
          >
            <strong>{group.title}</strong>
            <span>{group.label}</span>
          </div>
        ))}
      </div>
      <ReactFlow
        nodes={nodesWithActions}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnectWithValidation}
        isValidConnection={isValidConnection}
        onNodeClick={onNodeClick}
        onEdgeClick={onEdgeClick}
        onPaneClick={onPaneClick}
        onMove={onMove}
        onDrop={onDrop}
        onDragOver={onDragOver}
        nodeTypes={nodeTypes}
        fitView
      >
        <Background gap={24} size={1.3} />
        <MiniMap position="top-left" pannable zoomable />
        <Controls position="top-right" />
      </ReactFlow>
    </div>
  );
}

export default function FlowCanvas({ activePipelineNodeId, onNodeOpenData, onRunNode, runningNodeId }) {
  return (
    <ReactFlowProvider>
      <FlowInner
        activePipelineNodeId={activePipelineNodeId}
        onNodeOpenData={onNodeOpenData}
        onRunNode={onRunNode}
        runningNodeId={runningNodeId}
      />
    </ReactFlowProvider>
  );
}
