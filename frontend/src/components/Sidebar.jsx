import { useId } from 'react';
import { Database, FileUp, Filter, HardDriveDownload } from 'lucide-react';
import { paletteNodes } from '../store/flowStore';
import { useFlowStore } from '../store/flowStore';
import { parseUploadedFile } from '../utils/sourceParsers';

function getVisibleSpawnPosition(nodes, viewport, canvasSize) {
  const zoom = viewport?.zoom || 1;
  const visibleLeft = Math.max(40, (-viewport.x + 36) / zoom);
  const visibleBottom = Math.max(120, (-viewport.y + Math.max(canvasSize.height, 360) - 110) / zoom);
  const spawnBandTop = visibleBottom + 22;
  const spawnBandNodes = nodes.filter(
    (node) => node.position.y >= spawnBandTop - 40 && node.position.y <= spawnBandTop + 260,
  ).length;

  return {
    x: visibleLeft + (spawnBandNodes % 3) * 88,
    y: spawnBandTop + Math.floor(spawnBandNodes / 3) * 72,
  };
}

function iconForKind(kind) {
  if (kind === 'source') {
    return Database;
  }
  if (kind === 'load') {
    return HardDriveDownload;
  }
  return Filter;
}

export default function Sidebar() {
  const addNode = useFlowStore((state) => state.addNode);
  const nodes = useFlowStore((state) => state.nodes);
  const canvasViewport = useFlowStore((state) => state.canvasViewport);
  const canvasSize = useFlowStore((state) => state.canvasSize);
  const setStatusMessage = useFlowStore((state) => state.setStatusMessage);
  const fileInputId = useId();
  const groupedTemplates = {
    source: paletteNodes.filter((node) => node.kind === 'source'),
    transform: paletteNodes.filter((node) => node.kind === 'transform'),
    load: paletteNodes.filter((node) => node.kind === 'load'),
  };

  const onDragStart = (event, nodeTemplate) => {
    event.dataTransfer.setData('application/reactflow', JSON.stringify(nodeTemplate));
    event.dataTransfer.effectAllowed = 'move';
  };

  const onTemplateClick = (nodeTemplate) => {
    const position = getVisibleSpawnPosition(nodes, canvasViewport, canvasSize);

    addNode({
      id: `${nodeTemplate.kind}-${Date.now()}`,
      type: 'etl',
      position,
      data: {
        kind: nodeTemplate.kind,
        label: nodeTemplate.label,
        config: nodeTemplate.templateConfig,
      },
    });

    setStatusMessage(`Node hinzugefuegt (Klick): ${nodeTemplate.label}`);
  };

  const onFileChange = async (event) => {
    const [file] = event.target.files ?? [];

    if (!file) {
      return;
    }

    const extension = file.name.split('.').pop()?.toLowerCase();

    if (extension !== 'csv' && extension !== 'json') {
      setStatusMessage('Nur CSV oder JSON Dateien werden unterstuetzt.');
      event.target.value = '';
      return;
    }

    try {
      const parsedFile = await parseUploadedFile(file);
      const position = getVisibleSpawnPosition(nodes, canvasViewport, canvasSize);

      addNode({
        id: `source-file-${Date.now()}`,
        type: 'etl',
        position,
        data: {
          kind: 'source',
          label: parsedFile.label,
          config: parsedFile.config,
        },
      });

      setStatusMessage(`Datei geladen und als Source-Node hinzugefuegt: ${file.name}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Datei konnte nicht gelesen werden.';
      setStatusMessage(message);
    } finally {
      event.target.value = '';
    }
  };

  return (
    <aside className="sidebar">
      <h2>Node Library</h2>
      <p>Drag & Drop auf das Canvas</p>

      <div className="upload-block">
        <label className="upload-btn" htmlFor={fileInputId}>
          <FileUp size={16} />
          CSV/JSON laden
        </label>
        <input
          id={fileInputId}
          className="upload-input"
          type="file"
          accept=".csv,.json,application/json,text/csv"
          onChange={onFileChange}
        />
      </div>

      <div className="sidebar__list">
        {Object.entries(groupedTemplates).map(([kind, entries]) => (
          <div key={kind} className="palette-group">
            <p className="palette-group__title">{kind.toUpperCase()}</p>

            {entries.map((nodeTemplate) => {
              const Icon = iconForKind(nodeTemplate.kind);

              return (
                <button
                  key={`${nodeTemplate.kind}-${nodeTemplate.label}`}
                  className={`palette-card palette-card--${nodeTemplate.kind}`}
                  draggable
                  onDragStart={(event) => onDragStart(event, nodeTemplate)}
                  onClick={() => onTemplateClick(nodeTemplate)}
                  type="button"
                >
                  <Icon size={16} />
                  <span>{nodeTemplate.label}</span>
                </button>
              );
            })}
          </div>
        ))}
      </div>
    </aside>
  );
}
