from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .schemas import FlowPayload, PostgresSourceConfig
from .services.etl import run_flow
from .services.postgres import fetch_records
from .services.storage import list_flows, load_flow, save_flow


settings = get_settings()
app = FastAPI(title='ETL Flow API Neu', version='2.0.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.get('/health')
def health_check() -> dict[str, str]:
    return {'status': 'ok', 'service': 'backend_neu'}


@app.get('/api/flows')
def get_flow_ids() -> dict[str, list[str]]:
    return {'flows': list_flows()}


@app.get('/api/flows/{flow_id}', response_model=FlowPayload)
def get_flow(flow_id: str) -> FlowPayload:
    flow = load_flow(flow_id)
    if flow is None:
        raise HTTPException(status_code=404, detail='Flow wurde nicht gefunden.')
    return flow


@app.post('/api/flows/{flow_id}', response_model=FlowPayload)
def post_flow(flow_id: str, payload: FlowPayload) -> FlowPayload:
    try:
        save_flow(flow_id, payload)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return payload


@app.post('/api/execute')
def execute_flow(payload: FlowPayload) -> dict:
    try:
        return run_flow(payload)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post('/api/sources/postgres/preview')
def preview_postgres_source(config: PostgresSourceConfig) -> dict:
    try:
        records = fetch_records(config.model_dump(by_alias=True))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    return {
        'label': f"PostgreSQL Source: {config.table}",
        'row_count': len(records),
        'config': {
            **config.model_dump(by_alias=True),
            'records': records,
            'sample_rows': records[:5],
        },
        'analysis': {
            'row_count': len(records),
            'column_count': len(records[0]) if records else 0,
        },
    }
