const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';

export async function listFlows() {
  const response = await fetch(`${API_BASE_URL}/api/flows`);

  if (!response.ok) {
    throw new Error('Flow-Liste konnte nicht geladen werden.');
  }

  const payload = await response.json();
  return payload.flows ?? [];
}

export async function saveFlow(flowId, payload) {
  const response = await fetch(`${API_BASE_URL}/api/flows/${flowId}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error('Flow konnte nicht gespeichert werden.');
  }

  return response.json();
}

export async function loadFlow(flowId, options = {}) {
  const { allowNotFound = false } = options;
  const response = await fetch(`${API_BASE_URL}/api/flows/${flowId}`);

  if (response.status === 404 && allowNotFound) {
    return null;
  }

  if (!response.ok) {
    throw new Error('Flow konnte nicht geladen werden.');
  }

  return response.json();
}

export async function runFlow(payload) {
  const response = await fetch(`${API_BASE_URL}/api/execute`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorPayload = await response.json().catch(() => ({}));
    const detail = errorPayload.detail ?? 'Pipeline konnte nicht ausgefuehrt werden.';
    throw new Error(detail);
  }

  return response.json();
}

export async function previewPostgresSource(config) {
  const response = await fetch(`${API_BASE_URL}/api/sources/postgres/preview`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(config),
  });

  if (!response.ok) {
    const errorPayload = await response.json().catch(() => ({}));
    throw new Error(errorPayload.detail ?? 'PostgreSQL Source konnte nicht geladen werden.');
  }

  return response.json();
}
