from __future__ import annotations

import json
from pathlib import Path

from ..config import FLOW_DIR
from ..schemas import FlowPayload


def _sanitize_flow_id(flow_id: str) -> str:
    safe = ''.join(char for char in flow_id if char.isalnum() or char in {'-', '_'})
    if not safe:
        raise ValueError('flow_id ist ungueltig.')
    return safe


def _flow_file(flow_id: str) -> Path:
    return FLOW_DIR / f'{_sanitize_flow_id(flow_id)}.json'


def list_flows() -> list[str]:
    if not FLOW_DIR.exists():
        return []
    return sorted(path.stem for path in FLOW_DIR.glob('*.json'))


def load_flow(flow_id: str) -> FlowPayload | None:
    target = _flow_file(flow_id)
    if not target.exists():
        return None

    with target.open('r', encoding='utf-8') as handle:
        payload = json.load(handle)

    return FlowPayload.model_validate(payload)


def save_flow(flow_id: str, payload: FlowPayload) -> None:
    FLOW_DIR.mkdir(parents=True, exist_ok=True)
    target = _flow_file(flow_id)
    with target.open('w', encoding='utf-8') as handle:
        json.dump(payload.model_dump(), handle, indent=2)
