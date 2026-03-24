import { Database, Filter, HardDriveDownload, Play } from 'lucide-react';
import { Handle, Position } from 'reactflow';

const kindMeta = {
  source: {
    title: 'SOURCE',
    icon: Database,
  },
  transform: {
    title: 'TRANSFORM',
    icon: Filter,
  },
  load: {
    title: 'LOAD',
    icon: HardDriveDownload,
  },
};

export default function ETLNode({ data }) {
  const meta = kindMeta[data.kind] ?? kindMeta.transform;
  const Icon = meta.icon;
  const isRunning = Boolean(data.isRunningNode);

  const onRunMouseDown = (event) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const onRunClick = (event) => {
    event.preventDefault();
    event.stopPropagation();
    data.onSelectNode?.(data.nodeId);
    data.onRunNode?.(data.nodeId);
  };

  const onLabelClick = () => {
    data.onOpenNodeData?.(data.nodeId);
  };

  return (
    <div className={`etl-node etl-node--${data.kind}`}>
      <Handle type="target" position={Position.Left} />

      <div className="etl-node__header">
        <div className="etl-node__header-left">
          <Icon size={14} />
          <span>{meta.title}</span>
        </div>
        <button
          className="etl-node__run-btn"
          type="button"
          onMouseDown={onRunMouseDown}
          onClick={onRunClick}
          disabled={isRunning}
          title="Node ausfuehren"
        >
          <Play size={12} />
        </button>
      </div>
      <button
        className="etl-node__label etl-node__label-btn"
        type="button"
        onClick={onLabelClick}
        title="Datenansicht oeffnen"
      >
        {data.label}
        {data.pipelineName ? (
          <span className="etl-node__pipeline" title={data.pipelineName}>
            &#9651; {data.pipelineName}
          </span>
        ) : null}
      </button>

      <Handle type="source" position={Position.Right} />
    </div>
  );
}
