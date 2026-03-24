import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const KIND_COLORS = {
  source: '#2f8f9d',
  transform: '#f6a609',
  load: '#4f7cff',
};

function formatDelta(value) {
  if (value > 0) {
    return `+${value}`;
  }

  return String(value);
}

function tooltipValueFormatter(value) {
  if (typeof value === 'number') {
    return value.toLocaleString('de-DE');
  }

  return value;
}

function buildAnalytics(steps, loads) {
  const warnings = [];
  const kindCounts = steps.reduce(
    (accumulator, step) => ({
      ...accumulator,
      [step.kind]: (accumulator[step.kind] ?? 0) + 1,
    }),
    {},
  );
  const stepRowsChartData = steps.map((step, index) => ({
    name: `${index + 1}. ${step.label}`,
    rows: step.row_count ?? 0,
    kind: step.kind,
  }));
  const deltaChartData = steps.map((step, index) => {
    const previousRows = index === 0 ? null : steps[index - 1]?.row_count ?? 0;
    const delta = previousRows === null ? 0 : (step.row_count ?? 0) - previousRows;

    return {
      name: `${index + 1}. ${step.label}`,
      delta,
      deltaLabel: previousRows === null ? 'Start' : formatDelta(delta),
    };
  });
  const kindChartData = Object.entries(kindCounts).map(([kind, count]) => ({
    name: kind.toUpperCase(),
    value: count,
    fill: KIND_COLORS[kind] ?? '#90a8b8',
  }));
  const loadChartData = loads.map((load) => ({
    name: load.target,
    rows: load.row_count ?? 0,
    persisted: load.persisted_rows ?? 0,
  }));

  return {
    kindCounts,
    stepRowsChartData,
    deltaChartData,
    kindChartData,
    loadChartData,
    warnings,
  };
}

function PipelineRunSection({ label, loads, runId, steps, onOpenStep, showRunId = false }) {
  const { deltaChartData, kindChartData, loadChartData, stepRowsChartData } = buildAnalytics(steps, loads);

  return (
    <section className="pipeline-run-section">
      <div className="pipeline-run-section__header">
        <div>
          <h3>{label}</h3>
          <p>{steps.length} Schritte, {loads.length} Loads</p>
        </div>
        {showRunId ? <span className="pipeline-run-section__runid">{runId}</span> : null}
      </div>

      <section className="run-analytics">
        <div className="run-analytics__summary">
          <article className="run-stat-card">
            <span>Run-ID</span>
            <strong>{runId ?? '-'}</strong>
          </article>
          <article className="run-stat-card">
            <span>Schritte</span>
            <strong>{steps.length}</strong>
          </article>
          <article className="run-stat-card">
            <span>Warnings</span>
            <strong>0</strong>
          </article>
          <article className="run-stat-card">
            <span>Loads</span>
            <strong>{loads.length}</strong>
          </article>
        </div>

        <div className="run-analytics__grid">
          <section className="chart-card">
            <div className="chart-card__header">
              <h3>Rows je Step</h3>
              <span>Input bis Output</span>
            </div>

            <div className="chart-canvas">
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={stepRowsChartData} margin={{ top: 8, right: 8, left: -18, bottom: 40 }}>
                  <CartesianGrid stroke="#dbe8ef" strokeDasharray="4 4" vertical={false} />
                  <XAxis dataKey="name" angle={-22} textAnchor="end" interval={0} height={62} tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                  <YAxis tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                  <Tooltip formatter={tooltipValueFormatter} contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                  <Bar dataKey="rows" radius={[8, 8, 0, 0]}>
                    {stepRowsChartData.map((entry) => (
                      <Cell key={entry.name} fill={KIND_COLORS[entry.kind] ?? '#90a8b8'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="chart-card">
            <div className="chart-card__header">
              <h3>Delta je Step</h3>
              <span>Rows-Veraenderung</span>
            </div>

            <div className="chart-canvas">
              <ResponsiveContainer width="100%" height={240}>
                <AreaChart data={deltaChartData} margin={{ top: 8, right: 8, left: -18, bottom: 40 }}>
                  <CartesianGrid stroke="#dbe8ef" strokeDasharray="4 4" vertical={false} />
                  <XAxis dataKey="name" angle={-22} textAnchor="end" interval={0} height={62} tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                  <YAxis tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                  <Tooltip formatter={tooltipValueFormatter} labelFormatter={(labelValue, payload) => payload?.[0]?.payload?.deltaLabel ?? labelValue} contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                  <Area type="monotone" dataKey="delta" stroke="#2b9d6f" fill="#bfe9d5" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="chart-card">
            <div className="chart-card__header">
              <h3>Step-Typen</h3>
              <span>Pipeline-Struktur</span>
            </div>

            <div className="chart-canvas chart-canvas--pie">
              <ResponsiveContainer width="100%" height={240}>
                <PieChart>
                  <Pie data={kindChartData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={48} outerRadius={82} paddingAngle={3}>
                    {kindChartData.map((entry) => (
                      <Cell key={entry.name} fill={entry.fill} />
                    ))}
                  </Pie>
                  <Tooltip formatter={tooltipValueFormatter} contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="chart-legend">
              {kindChartData.map((entry) => (
                <span key={entry.name} className="chart-legend__item">
                  <i style={{ backgroundColor: entry.fill }} />
                  {entry.name}: {entry.value}
                </span>
              ))}
            </div>
          </section>

          <section className="chart-card">
            <div className="chart-card__header">
              <h3>Load-Ziele</h3>
              <span>Zielsysteme und Storage</span>
            </div>

            {!loads.length ? (
              <p className="chart-empty">Keine Load-Ziele im letzten Run.</p>
            ) : (
              <>
                <div className="chart-canvas">
                  <ResponsiveContainer width="100%" height={240}>
                    <BarChart data={loadChartData} margin={{ top: 8, right: 8, left: -18, bottom: 24 }}>
                      <CartesianGrid stroke="#dbe8ef" strokeDasharray="4 4" vertical={false} />
                      <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                      <YAxis tick={{ fontSize: 11, fill: '#4a6a7f' }} />
                      <Tooltip formatter={tooltipValueFormatter} contentStyle={{ borderRadius: 12, borderColor: '#cbdde7' }} />
                      <Bar dataKey="rows" fill="#4f7cff" radius={[8, 8, 0, 0]} />
                      <Bar dataKey="persisted" fill="#2f8f9d" radius={[8, 8, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="load-list">
                  {loads.map((load) => (
                    <article key={load.node_id} className="load-card">
                      <strong>{load.target}</strong>
                      <span>{load.row_count} rows</span>
                      <span>Storage: {load.storage}</span>
                      <span>Persistiert: {load.persisted_rows ?? 0}</span>
                    </article>
                  ))}
                </div>
              </>
            )}
          </section>
        </div>
      </section>

      <div className="step-list">
        {steps.map((step, index) => (
          <article key={`${step.node_id}-${index}`} className="step-card">
            <div className="step-card__meta">
              <strong>{index + 1}. {step.label}</strong>
              <span>{step.kind.toUpperCase()} | rows: {step.row_count}</span>
            </div>

            {step.target ? <p className="step-target">Target: {step.target}</p> : null}

            <div className="step-actions">
              <button
                className="step-action-btn"
                type="button"
                onClick={() => onOpenStep?.(step)}
              >
                Details ansehen
              </button>
            </div>

            <pre className="step-preview">
              {JSON.stringify(step.preview ?? [], null, 2)}
            </pre>
          </article>
        ))}
      </div>
    </section>
  );
}

export default function RunDetailsDialog({ open, onClose, onOpenStep, pipelineOptions = [], runResult }) {
  const steps = runResult?.node_results ?? [];
  const loads = runResult?.loads ?? [];
  const warnings = runResult?.warnings ?? [];
  const pipelineSections = pipelineOptions
    .map((option) => ({
      label: option.title ? `${option.title}: ${option.shortLabel}` : option.shortLabel,
      steps: steps.filter((step) => option.nodeIds.includes(step.node_id)),
      loads: loads.filter((load) => option.nodeIds.includes(load.node_id)),
    }))
    .filter((section) => section.steps.length);
  const shouldGroupByPipeline = pipelineSections.length > 1;

  if (!open) {
    return null;
  }

  return (
    <div className="dialog-overlay" role="presentation" onClick={onClose}>
      <section
        className="dialog-card"
        role="dialog"
        aria-modal="true"
        aria-label="Run details"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="dialog-header">
          <h2>Verarbeitete Daten je Schritt</h2>
          <button className="dialog-close" type="button" onClick={onClose}>
            Schliessen
          </button>
        </header>

        {!steps.length ? (
          <p>Keine Step-Daten vorhanden. Fuehre zuerst die Pipeline aus.</p>
        ) : shouldGroupByPipeline ? (
          <div className="pipeline-run-stack">
            {pipelineSections.map((section) => (
              <PipelineRunSection
                key={section.label}
                label={section.label}
                loads={section.loads}
                onOpenStep={onOpenStep}
                runId={runResult?.run_id}
                showRunId={false}
                steps={section.steps}
              />
            ))}
          </div>
        ) : (
          <PipelineRunSection
            label="Ausgewählte Pipeline"
            loads={loads}
            onOpenStep={onOpenStep}
            runId={runResult?.run_id}
            showRunId={false}
            steps={steps}
          />
        )}
      </section>
    </div>
  );
}
