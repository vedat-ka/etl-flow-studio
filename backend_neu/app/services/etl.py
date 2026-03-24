from __future__ import annotations

import ast
import json
import re
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd

from ..config import EXPORT_DIR, get_settings
from ..schemas import FlowNode, FlowPayload
from .analytics import dataframe_analysis, dataframe_preview, dataframe_records, run_analysis
from .postgres import fetch_records, write_records


def _topological_order(payload: FlowPayload) -> list[str]:
    node_ids = {node.id for node in payload.nodes}
    in_degree = {node.id: 0 for node in payload.nodes}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for edge in payload.edges:
        if edge.source in node_ids and edge.target in node_ids:
            adjacency[edge.source].append(edge.target)
            in_degree[edge.target] += 1

    queue = deque(sorted(node_id for node_id, degree in in_degree.items() if degree == 0))
    order: list[str] = []
    while queue:
        node_id = queue.popleft()
        order.append(node_id)
        for target in adjacency[node_id]:
            in_degree[target] -= 1
            if in_degree[target] == 0:
                queue.append(target)

    if len(order) != len(payload.nodes):
        raise ValueError('Der Flow enthaelt einen Zyklus oder ungueltige Edges.')

    return order


def _incoming_map(payload: FlowPayload) -> dict[str, list[str]]:
    incoming: dict[str, list[str]] = defaultdict(list)
    for edge in payload.edges:
        incoming[edge.target].append(edge.source)
    return incoming


def _connected_component_payload(payload: FlowPayload) -> FlowPayload:
    target_node_id = str(payload.target_node_id or '').strip()
    if not target_node_id:
        return payload

    node_ids = {node.id for node in payload.nodes}
    if target_node_id not in node_ids:
        raise ValueError(f'Ziel-Node {target_node_id} wurde im Flow nicht gefunden.')

    adjacency: dict[str, list[str]] = defaultdict(list)
    for edge in payload.edges:
        if edge.source in node_ids and edge.target in node_ids:
            adjacency[edge.source].append(edge.target)
            adjacency[edge.target].append(edge.source)

    connected_ids: set[str] = set()
    queue = deque([target_node_id])
    while queue:
        node_id = queue.popleft()
        if node_id in connected_ids:
            continue
        connected_ids.add(node_id)
        queue.extend(adjacency.get(node_id, []))

    return FlowPayload(
        nodes=[node for node in payload.nodes if node.id in connected_ids],
        edges=[edge for edge in payload.edges if edge.source in connected_ids and edge.target in connected_ids],
        target_node_id=target_node_id,
    )


def _collect_upstream_source_labels(
    node_id: str,
    node_map: dict[str, FlowNode],
    incoming: dict[str, list[str]],
) -> list[str]:
    visited: set[str] = set()
    queue = deque([node_id])
    labels: list[str] = []

    while queue:
        current_id = queue.popleft()
        if current_id in visited:
            continue
        visited.add(current_id)
        node = node_map.get(current_id)
        if not node:
            continue
        if str(node.data.get('kind') or '') == 'source':
            label = str(node.data.get('label') or '').strip()
            if label:
                labels.append(label)
        queue.extend(incoming.get(current_id, []))

    return labels


def _derive_table_name_from_labels(labels: list[str]) -> str:
    cleaned_parts: list[str] = []
    seen: set[str] = set()
    for label in labels:
        part = re.sub(r'^(csv|json|ndjson)\s+file:\s*', '', str(label).strip(), flags=re.IGNORECASE)
        part = re.sub(r'\.[a-z0-9]+$', '', part, flags=re.IGNORECASE)
        part = re.sub(r'[^a-zA-Z0-9_]+', '_', part)
        part = re.sub(r'_+', '_', part).strip('_').lower()
        if part and part not in seen:
            cleaned_parts.append(part)
            seen.add(part)
    return '_'.join(cleaned_parts) or 'etl_output'


def _should_auto_name_table(table_name: Any) -> bool:
    value = str(table_name or '').strip().lower()
    return (
        not value
        or value == 'table'
        or bool(re.fullmatch(r'arrivals_ml_ready(?:[_-]?\d+)?', value))
    )


def _coerce_frame(records: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame

    for column in frame.columns:
        series = frame[column]
        if series.dtype == object:
            trimmed = series.map(lambda value: value.strip() if isinstance(value, str) else value)
            frame[column] = trimmed.replace({'': None})
    return frame


def _merge_inputs(frames: list[pd.DataFrame]) -> pd.DataFrame:
    available = [frame.copy() for frame in frames if frame is not None]
    if not available:
        return pd.DataFrame()
    if len(available) == 1:
        return available[0]
    return pd.concat(available, ignore_index=True, sort=False)


def _resolve_transform_type(node: FlowNode) -> str:
    config = node.data.get('config') or {}
    explicit = str(config.get('transform_type') or '').strip().lower()
    if explicit:
        return explicit
    label = str(node.data.get('label') or '').lower()
    if 'filter' in label:
        return 'filter'
    if 'join' in label:
        return 'join'
    if 'clean' in label:
        return 'clean'
    if 'transform' in label:
        return 'transform'
    return 'passthrough'


def _split_columns(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(',') if item.strip()]
    return []


def _trim_string_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Trimmt Strings vektorisiert via pandas str-Methoden (kein Zeilen-Loop)."""
    result = frame.copy()
    for col in result.select_dtypes(include='object').columns:
        mask = result[col].notna()
        result.loc[mask, col] = result.loc[mask, col].astype(str).str.strip()
        result[col] = result[col].replace({'': None, 'nan': None, 'None': None})
    return result


def _to_snake_name(name: str) -> str:
    """Normalisiert einen Spaltennamen zu lowercase snake_case ohne Sonderzeichen."""
    s = re.sub(r'[\s\-\.\/\\]+', '_', str(name).lower())
    s = re.sub(r'[^a-z0-9_]', '', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s or 'col'


def _normalize_column_names(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalisiert alle Spaltennamen zu snake_case; dedupliziert automatisch."""
    seen: dict[str, int] = {}
    mapping = {}
    for col in frame.columns:
        name = _to_snake_name(str(col))
        if name in seen:
            seen[name] += 1
            name = f'{name}_{seen[name]}'
        else:
            seen[name] = 0
        mapping[col] = name
    return frame.rename(columns=mapping)


def _parse_datetime_columns(frame: pd.DataFrame, excluded: set[str]) -> pd.DataFrame:
    """Erkennt ISO-Datetime-String-Spalten und extrahiert Jahr/Monat/Tag/Stunde/Wochentag
    als numerische Int-Features. Vektorisiert via pd.to_datetime(utc=True).
    """
    if frame.empty:
        return frame
    drop_cols: list[str] = []
    add_frames: list[pd.DataFrame] = []
    for col in list(frame.columns):
        if col in excluded:
            continue
        series = frame[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed = series.dt.tz_localize('UTC') if series.dt.tz is None else series
        elif series.dtype == object:
            sample = series.dropna().head(20).astype(str)
            if not sample.str.match(r'\d{4}-\d{2}-\d{2}').any():
                continue
            parsed = pd.to_datetime(series, errors='coerce', utc=True)
            if parsed.notna().sum() < max(1, len(series) // 2):
                continue
        else:
            continue
        features = pd.DataFrame({
            f'{col}_year':      parsed.dt.year.astype('Int64'),
            f'{col}_month':     parsed.dt.month.astype('Int64'),
            f'{col}_day':       parsed.dt.day.astype('Int64'),
            f'{col}_hour':      parsed.dt.hour.astype('Int64'),
            f'{col}_dayofweek': parsed.dt.dayofweek.astype('Int64'),
        }, index=frame.index)
        drop_cols.append(col)
        add_frames.append(features)
    if not add_frames:
        return frame
    result = frame.drop(columns=drop_cols)
    return pd.concat([result] + add_frames, axis=1)


def _normalize_boolean_columns(frame: pd.DataFrame, excluded: set[str]) -> tuple[pd.DataFrame, int]:
    """Konvertiert bool-aehnliche Spalten vektorisiert zu Int64 (0/1/NA).
    Erkennt: Python bool, 'true'/'false', 'yes'/'no', 'ja'/'nein', '1'/'0'.
    """
    if frame.empty:
        return frame, 0
    BOOL_TRUE  = frozenset({'true', 'yes', '1', 't', 'y', 'ja', '1.0'})
    BOOL_FALSE = frozenset({'false', 'no', '0', 'f', 'n', 'nein', '0.0'})
    ALL_BOOL   = BOOL_TRUE | BOOL_FALSE
    NULL_LIKE  = frozenset({'nan', 'none', '<na>', 'nat', ''})
    result = frame.copy()
    converted = 0
    for col in result.columns:
        if col in excluded:
            continue
        series = result[col]
        if pd.api.types.is_bool_dtype(series):
            result[col] = series.astype(np.int8)
            converted += 1
            continue
        if series.dtype != object:
            continue
        non_null = series.dropna()
        if non_null.empty:
            continue
        lower = non_null.astype(str).str.lower().str.strip()
        actual_values = set(lower.unique()) - NULL_LIKE
        if not actual_values or not actual_values <= ALL_BOOL:
            continue
        result[col] = (
            series.astype(str).str.lower().str.strip()
            .map(lambda v: 1 if v in BOOL_TRUE else (0 if v in BOOL_FALSE else pd.NA))
            .astype('Int64')
        )
        converted += 1
    return result, converted


def _drop_constant_columns(frame: pd.DataFrame, excluded: set[str]) -> tuple[pd.DataFrame, list[str]]:
    """Entfernt Spalten ohne Varianz (nur ein einzigartiger Wert = kein ML-Nutzen).
    Vektorisiert via frame.nunique().
    """
    if frame.empty:
        return frame, []
    n_unique = frame.nunique(dropna=False)
    drop = [col for col in frame.columns if col not in excluded and n_unique[col] <= 1]
    if not drop:
        return frame, []
    return frame.drop(columns=drop), drop


def _drop_id_like_columns(frame: pd.DataFrame, excluded: set[str]) -> tuple[pd.DataFrame, list[str]]:
    """Erkennt und entfernt ID-artige String-Spalten: vollstaendig einzigartige Werte
    die nach UUID/Hash aussehen oder ID-bezogene Spaltennamen tragen.
    """
    if frame.empty or len(frame) < 3:
        return frame, []
    n = len(frame)
    id_re = re.compile(r'(^|_)(id|hash|uuid|key|token|ref|run_id|node_id)($|_)', re.IGNORECASE)
    drop: list[str] = []
    for col in frame.columns:
        if col in excluded or frame[col].dtype != object:
            continue
        series = frame[col].dropna()
        if len(series) == 0 or series.nunique() < n * 0.95:
            continue
        sample = series.head(5).astype(str)
        is_hash   = sample.str.match(r'^[a-f0-9\-]{8,}$').all()
        is_id_col = bool(id_re.search(col))
        if is_hash or is_id_col:
            drop.append(col)
    if not drop:
        return frame, []
    return frame.drop(columns=drop), drop


def _parse_dict_value(value):
    """Parst String-Dicts ('...') und JSON-Strings ({...}) zu Python-dict."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith('{'):
            try:
                return json.loads(stripped)
            except Exception:
                pass
            try:
                return ast.literal_eval(stripped)
            except Exception:
                pass
    return value


def _flatten_nested_columns(frame: pd.DataFrame, sep: str = '_') -> pd.DataFrame:
    """Expandiert alle Spalten mit dict-Werten rekursiv via pd.json_normalize.
    Listen-Werte werden als '|'-getrennter String gespeichert.
    Vektorisiert: kein zeilenweises Python-Loop auf dem gesamten DataFrame.
    """
    if frame.empty:
        return frame

    columns_to_drop: list[str] = []
    new_frames: list[pd.DataFrame] = []

    for column in list(frame.columns):
        series = frame[column]
        if series.dtype != object:
            continue

        # Vektorisiert: alle Werte auf einmal parsen
        parsed = series.map(_parse_dict_value)
        dict_mask = parsed.map(lambda v: isinstance(v, dict))
        if dict_mask.sum() < max(1, len(parsed) // 2):
            continue

        # Fehlende Werte als leeres dict fuellen, dann normalisieren
        safe = parsed.where(dict_mask, other=None).map(lambda v: v if isinstance(v, dict) else {})
        try:
            normalized = pd.json_normalize(safe.tolist(), sep=sep)
            normalized.index = frame.index
            normalized.columns = [f"{column}{sep}{c}" for c in normalized.columns]

            # Listen-Spalten: '|'-Join; verbleibende dicts: str()
            for col in normalized.columns:
                col_series = normalized[col]
                if col_series.map(lambda v: isinstance(v, list)).any():
                    normalized[col] = col_series.map(
                        lambda v: '|'.join(str(i) for i in v) if isinstance(v, list) else v
                    )
                if col_series.map(lambda v: isinstance(v, dict)).any():
                    normalized[col] = col_series.map(
                        lambda v: str(v) if isinstance(v, dict) else v
                    )

            columns_to_drop.append(column)
            new_frames.append(normalized)
        except Exception:
            continue

    if not new_frames:
        return frame

    result = frame.drop(columns=columns_to_drop)
    return pd.concat([result] + new_frames, axis=1).reset_index(drop=True)


def _is_mostly_numeric(series: pd.Series, threshold: float = 0.8) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False
    converted = pd.to_numeric(non_null, errors='coerce')
    ratio = converted.notna().sum() / len(non_null)
    return ratio >= threshold


def _normalize_numeric_impute_mode(value: Any, default: str = 'auto') -> str:
    mode = str(value or '').strip().lower()
    if mode in {'', 'none'}:
        return default
    if mode in {'keep', 'keep_missing', 'preserve'}:
        return 'none'
    if mode in {'auto', 'zero', 'mean', 'median'}:
        return mode
    return default


def _normalize_categorical_impute_mode(value: Any, default: str = 'auto') -> str:
    mode = str(value or '').strip().lower()
    if mode in {'', 'none'}:
        return default
    if mode in {'keep', 'keep_missing', 'preserve'}:
        return 'none'
    if mode in {'auto', 'mode', 'constant'}:
        return mode
    return default


def _fill_numeric_series(series: pd.Series, mode: str) -> pd.Series:
    resolved_mode = _normalize_numeric_impute_mode(mode)
    if resolved_mode == 'none':
        return series

    replacement: float | int | None
    if resolved_mode == 'zero':
        replacement = 0
    elif resolved_mode == 'mean':
        replacement = series.mean()
    else:
        replacement = series.median()

    if pd.isna(replacement):
        replacement = 0

    return series.fillna(replacement)


def _is_safe_auto_categorical_impute(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False

    unique_count = int(non_null.nunique(dropna=True))
    non_null_count = int(len(non_null))
    missing_ratio = 1 - (non_null_count / max(1, len(series)))
    unique_ratio = unique_count / max(1, non_null_count)

    # Auto-Modus nur fuer echte Kategorien mit wenigen Auspraegungen.
    # Sparse oder nahezu einzigartige Textspalten wie cabin/name sollen unberuehrt bleiben.
    return unique_count <= 20 and unique_ratio <= 0.2 and missing_ratio <= 0.5


def _infer_pattern_value(existing: pd.Series) -> str | None:
    """Leitet aus vorhandenen Werten einen strukturell passenden Ersatzwert ab.
    Erkennt Muster wie: 'C85', 'B96', 'A/5 21171', 'PC 17599' usw.
    """
    import re
    samples = existing.dropna().astype(str)
    if samples.empty:
        return None

    # Haeufigsten Praefix-Buchstaben + Zahlenbereich ermitteln
    prefixes = []
    numbers = []
    for val in samples:
        m = re.match(r'^([A-Za-z]+)[\s/]*(\d+)', val)
        if m:
            prefixes.append(m.group(1))
            numbers.append(int(m.group(2)))

    if prefixes and numbers:
        import random
        rng = random.Random(42)
        prefix = max(set(prefixes), key=prefixes.count)
        num = rng.randint(min(numbers), max(numbers))
        return f'{prefix}{num}'

    # Fallback: zufaelligen vorhandenen Wert verwenden
    return samples.sample(1, random_state=42).iloc[0]


def _fill_categorical_series(series: pd.Series, mode: str, fill_value: str = 'missing') -> pd.Series:
    normalized = series.astype('string')
    resolved_mode = _normalize_categorical_impute_mode(mode)
    if resolved_mode == 'none':
        return normalized

    replacement = fill_value or 'missing'
    if resolved_mode in {'auto', 'mode'}:
        if resolved_mode == 'auto' and not _is_safe_auto_categorical_impute(normalized):
            # Sparse/hochkardinal (z.B. cabin 77% fehlend):
            # Pattern-Ableitung: analysiert Struktur vorhandener Werte und generiert
            # strukturell passende Ersatzwerte (z.B. 'C85' aus Muster [A-Z]\d+).
            existing = normalized.dropna()
            if existing.empty:
                return normalized
            inferred = _infer_pattern_value(existing)
            if inferred is None:
                return normalized
            fill_series = normalized.copy()
            null_mask = fill_series.isna()
            if null_mask.any():
                # Fuer jeden fehlenden Wert individuell ableiten (mit Variation)
                derived = existing.sample(n=int(null_mask.sum()), replace=True, random_state=42)
                # Werte aus echter Verteilung verwenden, formatgerecht
                fill_series[null_mask] = derived.values
            return fill_series
        detected_mode = normalized.dropna().mode()
        if not detected_mode.empty:
            replacement = detected_mode.iloc[0]
        elif resolved_mode == 'auto':
            return normalized

    return normalized.fillna(replacement)


def _detect_numeric_columns(frame: pd.DataFrame, excluded: set[str] | None = None) -> list[str]:
    excluded = excluded or set()
    detected: list[str] = []
    for column in frame.columns:
        column_name = str(column)
        if column_name in excluded:
            continue
        if pd.api.types.is_numeric_dtype(frame[column]) or _is_mostly_numeric(frame[column]):
            detected.append(column_name)
    return detected


def _detect_categorical_columns(frame: pd.DataFrame, excluded: set[str]) -> list[str]:
    return [
        str(column)
        for column in frame.columns
        if str(column) not in excluded and not pd.api.types.is_numeric_dtype(frame[column])
    ]


def _build_transform_config(transform_type: str, config: dict) -> dict:
    # Clean-Node wird als Transform behandelt (kein separater Node mehr)
    if transform_type == 'clean':
        transform_type = 'transform'
    pipeline_config = {
        'query': '',
        'flatten_json': False,
        'normalize_column_names': False,
        'parse_datetime': False,
        'normalize_booleans': False,
        'drop_constant_columns': False,
        'drop_id_columns': False,
        'join_enabled': False,
        'auto_join': False,
        'auto_join_type': 'left',
        'join_left_key': '',
        'join_right_key': '',
        'join_type': 'inner',
        'trim_strings': False,
        'convert_numeric': False,
        'drop_empty_rows': False,
        'drop_empty_columns': False,
        'drop_duplicates': False,
        'keep_columns': [],
        'drop_columns': [],
        'numeric_columns': [],
        'categorical_columns': [],
        'target_column': '',
        'impute_numeric': 'auto',
        'impute_categorical': 'auto',
        'categorical_fill_value': 'missing',
        'encode_categorical': 'none',
        'scale_numeric': 'none',
    }

    if transform_type == 'filter':
        pipeline_config['query'] = str(config.get('query') or '')
        return pipeline_config

    if transform_type == 'join':
        pipeline_config.update(
            {
                'join_enabled': True,
                'join_left_key': str(config.get('left_key') or 'id').strip(),
                'join_right_key': str(config.get('right_key') or config.get('left_key') or 'id').strip(),
                'join_type': str(config.get('join_type') or 'inner').strip().lower(),
            }
        )
        return pipeline_config

    if transform_type == 'clean':
        pipeline_config.update(
            {
                'trim_strings': bool(config.get('trim_strings', True)),
                'convert_numeric': bool(config.get('convert_numeric', True)),
                'drop_duplicates': bool(config.get('drop_duplicates', True)),
                'drop_empty_rows': bool(config.get('drop_empty_rows', True)),
                'drop_empty_columns': bool(config.get('drop_empty_columns', True)),
                'drop_sparse_threshold': float(config.get('drop_sparse_threshold', 0.5)),
                'numeric_columns': _split_columns(config.get('numeric_fields')),
                'impute_numeric': _normalize_numeric_impute_mode(config.get('fill_numeric_nan')),
                'impute_categorical': _normalize_categorical_impute_mode(config.get('fill_categorical_missing')),
                'categorical_fill_value': str(config.get('categorical_fill_value') or 'missing'),
            }
        )
        return pipeline_config

    if transform_type == 'transform':
        pipeline_config.update(
            {
                'query': str(config.get('query') or ''),
                'flatten_json': bool(config.get('flatten_json', True)),
                'join_enabled': bool((config.get('join') or {}).get('enabled')) or bool(config.get('join_enabled')),
                'auto_join': bool(config.get('auto_join', True)),
                'auto_join_type': str(config.get('auto_join_type') or 'left').strip().lower(),
                'join_left_key': str((config.get('join') or {}).get('left_key') or config.get('left_key') or '').strip(),
                'join_right_key': str((config.get('join') or {}).get('right_key') or config.get('right_key') or '').strip(),
                'join_type': str((config.get('join') or {}).get('join_type') or config.get('join_type') or 'inner').strip().lower(),
                'normalize_column_names': bool(config.get('normalize_column_names', True)),
                'parse_datetime': bool(config.get('parse_datetime', True)),
                'normalize_booleans': bool(config.get('normalize_booleans', True)),
                'drop_constant_columns': bool(config.get('drop_constant_columns', True)),
                'drop_id_columns': bool(config.get('drop_id_columns', True)),
                'trim_strings': bool(config.get('trim_strings', True)),
                'convert_numeric': bool(config.get('convert_numeric', True)),
                'drop_empty_rows': bool(config.get('drop_empty_rows', True)),
                'drop_empty_columns': bool(config.get('drop_empty_columns', True)),
                'drop_duplicates': bool(config.get('drop_duplicates', True)),
                'keep_columns': _split_columns(config.get('keep_columns')),
                'drop_columns': _split_columns(config.get('drop_columns')),
                'numeric_columns': _split_columns(config.get('numeric_columns')),
                'categorical_columns': _split_columns(config.get('categorical_columns')),
                'target_column': str(config.get('target_column') or '').strip(),
                'impute_numeric': _normalize_numeric_impute_mode(config.get('impute_numeric')),
                'impute_categorical': _normalize_categorical_impute_mode(config.get('impute_categorical')),
                'categorical_fill_value': str(config.get('categorical_fill_value') or 'missing'),
                'encode_categorical': str(config.get('encode_categorical') or 'none').lower(),
                'scale_numeric': str(config.get('scale_numeric') or 'none').lower(),
            }
        )
        return pipeline_config

    return pipeline_config


def _normalize_filter_expression(expression: str) -> str:
    text = expression.strip()
    text = text.replace('<>', '!=')
    text = re.sub(r'\bAND\b', 'and', text, flags=re.IGNORECASE)
    text = re.sub(r'\bOR\b', 'or', text, flags=re.IGNORECASE)
    text = re.sub(r'\bNOT\b', 'not', text, flags=re.IGNORECASE)
    text = re.sub(r'(?<![<>=!])=(?!=)', '==', text)
    text = re.sub(r'\btrue\b', 'True', text, flags=re.IGNORECASE)
    text = re.sub(r'\bfalse\b', 'False', text, flags=re.IGNORECASE)
    text = re.sub(r'\bnull\b', 'None', text, flags=re.IGNORECASE)
    return text


def _literal_or_series(node: ast.AST, frame: pd.DataFrame):
    if isinstance(node, ast.Name):
        if node.id in frame.columns:
            return frame[node.id]
        if node.id in {'True', 'False', 'None'}:
            return {'True': True, 'False': False, 'None': None}[node.id]
        return node.id
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_literal_or_series(node.operand, frame)
    raise ValueError('Nicht unterstuetzter Ausdruck im Filter.')


def _evaluate_filter_ast(node: ast.AST, frame: pd.DataFrame):
    if isinstance(node, ast.BoolOp):
        values = [_evaluate_filter_ast(value, frame) for value in node.values]
        if isinstance(node.op, ast.And):
            result = values[0]
            for value in values[1:]:
                result = np.logical_and(result, value)
            return result
        result = values[0]
        for value in values[1:]:
            result = np.logical_or(result, value)
        return result

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return np.logical_not(_evaluate_filter_ast(node.operand, frame))

    if isinstance(node, ast.Compare):
        left = _literal_or_series(node.left, frame)
        result = None
        for operator, comparator in zip(node.ops, node.comparators):
            right = _literal_or_series(comparator, frame)
            if isinstance(operator, ast.Eq):
                current = left == right
            elif isinstance(operator, ast.NotEq):
                current = left != right
            elif isinstance(operator, ast.Gt):
                current = pd.to_numeric(left, errors='coerce') > pd.to_numeric(right, errors='coerce')
            elif isinstance(operator, ast.GtE):
                current = pd.to_numeric(left, errors='coerce') >= pd.to_numeric(right, errors='coerce')
            elif isinstance(operator, ast.Lt):
                current = pd.to_numeric(left, errors='coerce') < pd.to_numeric(right, errors='coerce')
            elif isinstance(operator, ast.LtE):
                current = pd.to_numeric(left, errors='coerce') <= pd.to_numeric(right, errors='coerce')
            else:
                raise ValueError('Operator im Filter wird nicht unterstuetzt.')

            result = current if result is None else np.logical_and(result, current)
            left = right
        return result

    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'contains' and len(node.args) == 2:
        series = _literal_or_series(node.args[0], frame)
        if not isinstance(series, pd.Series):
            raise ValueError('contains erwartet eine Spalte als erstes Argument.')
        search_value = _literal_or_series(node.args[1], frame)
        return series.astype('string').str.contains(str(search_value), case=False, na=False, regex=False)

    raise ValueError('Filter-Ausdruck wird nicht unterstuetzt.')


def _apply_filter(frame: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, str | None]:
    query = str(config.get('query') or '').strip()
    if not query or frame.empty:
        return frame.copy(), None

    try:
        normalized = _normalize_filter_expression(query)
        expression = ast.parse(normalized, mode='eval')
        mask = _evaluate_filter_ast(expression.body, frame)
        if not isinstance(mask, (pd.Series, np.ndarray, list)):
            raise ValueError('Filter-Ausdruck liefert keine boolesche Maske.')
        filtered = frame.loc[pd.Series(mask, index=frame.index).fillna(False)].copy()
        return filtered, None
    except Exception as error:
        return frame.copy(), f'Filter konnte nicht angewendet werden: {error}'


def _apply_clean(frame: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    cleaned = frame.copy()
    if cleaned.empty:
        return cleaned, {
            'trimmed_columns': 0,
            'numeric_columns': 0,
            'imputed_numeric_columns': 0,
            'imputed_categorical_columns': 0,
            'dropped_rows': 0,
            'dropped_columns': 0,
        }

    trimmed_columns = 0
    numeric_columns = 0
    imputed_numeric_columns = 0
    imputed_categorical_columns = 0
    dropped_rows_before = len(cleaned)
    dropped_columns_before = len(cleaned.columns)

    if config.get('trim_strings', True):
        for column in cleaned.columns:
            if cleaned[column].dtype == object:
                cleaned[column] = cleaned[column].map(lambda value: value.strip() if isinstance(value, str) else value)
                cleaned[column] = cleaned[column].replace({'': None})
                trimmed_columns += 1

    explicit_numeric_fields = bool(config.get('numeric_fields'))
    candidate_columns = config.get('numeric_fields') or list(cleaned.columns)
    if config.get('convert_numeric', True):
        for column in candidate_columns:
            if column not in cleaned.columns:
                continue
            if not explicit_numeric_fields and not pd.api.types.is_numeric_dtype(cleaned[column]) and not _is_mostly_numeric(cleaned[column]):
                continue
            converted = pd.to_numeric(cleaned[column], errors='coerce')
            if converted.notna().sum() == 0:
                continue
            cleaned[column] = converted
            numeric_columns += 1

    fill_mode = _normalize_numeric_impute_mode(config.get('fill_numeric_nan'))
    if fill_mode != 'none':
        numeric_frame = cleaned.select_dtypes(include=['number'])
        for column in numeric_frame.columns:
            missing_before = int(cleaned[column].isna().sum())
            cleaned[column] = _fill_numeric_series(cleaned[column], fill_mode)
            if missing_before and int(cleaned[column].isna().sum()) < missing_before:
                imputed_numeric_columns += 1

    categorical_fill_mode = _normalize_categorical_impute_mode(config.get('fill_categorical_missing'))
    if categorical_fill_mode != 'none':
        categorical_fill_value = str(config.get('categorical_fill_value') or 'missing')
        categorical_frame = cleaned.select_dtypes(include=['object', 'string', 'category'])
        for column in categorical_frame.columns:
            missing_before = int(cleaned[column].isna().sum())
            cleaned[column] = _fill_categorical_series(cleaned[column], categorical_fill_mode, categorical_fill_value)
            if missing_before and int(cleaned[column].isna().sum()) < missing_before:
                imputed_categorical_columns += 1

    # Schritt 1: Sparse Spalten löschen (>50% fehlend) — laut DataCamp: wenn zu viele Werte fehlen, Spalte entfernen
    sparse_threshold = float(config.get('drop_sparse_threshold', 0.5))
    if sparse_threshold < 1.0 and len(cleaned) > 0:
        missing_ratio = cleaned.isna().mean()
        sparse_cols = [c for c in cleaned.columns if missing_ratio[c] > sparse_threshold]
        if sparse_cols:
            cleaned = cleaned.drop(columns=sparse_cols)

    # Schritt 2: Zeilen mit verbleibenden NaN löschen (how='any' — laut DataCamp Standard)
    if config.get('drop_empty_rows', True):
        cleaned = cleaned.dropna(axis=0, how='any')
    # Schritt 3: Noch komplett leere Spalten entfernen
    if config.get('drop_empty_columns', True):
        cleaned = cleaned.dropna(axis=1, how='all')

    return cleaned, {
        'trimmed_columns': trimmed_columns,
        'numeric_columns': numeric_columns,
        'imputed_numeric_columns': imputed_numeric_columns,
        'imputed_categorical_columns': imputed_categorical_columns,
        'dropped_rows': max(0, dropped_rows_before - len(cleaned)),
        'dropped_columns': max(0, dropped_columns_before - len(cleaned.columns)),
    }


def _apply_join(frames: list[pd.DataFrame], config: dict) -> tuple[pd.DataFrame, str | None]:
    if len(frames) < 2:
        return pd.DataFrame(), 'Join benoetigt mindestens zwei Eingangsquellen.'

    left = frames[0].copy()
    right = frames[1].copy()
    left_key = str(config.get('left_key') or 'id').strip()
    right_key = str(config.get('right_key') or left_key).strip()
    join_type = str(config.get('join_type') or 'inner').strip().lower()
    join_map = {'inner': 'inner', 'left': 'left', 'right': 'right', 'full': 'outer', 'outer': 'outer'}
    if left_key not in left.columns or right_key not in right.columns:
        return pd.DataFrame(), f'Join-Key fehlt: {left_key} / {right_key}'

    merged = left.merge(
        right,
        how=join_map.get(join_type, 'inner'),
        left_on=left_key,
        right_on=right_key,
        suffixes=('_left', '_right'),
    )
    return merged, None


def _detect_join_key(left: pd.DataFrame, right: pd.DataFrame) -> str | None:
    """Erkennt automatisch den besten gemeinsamen Join-Key.
    Priorisiert: Spalten die in beiden DataFrames vorkommen und ID-artig benannt sind.
    Faellt zurueck auf beliebige gemeinsame Spalte mit hoher Kardinalitaet.
    """
    common = [c for c in left.columns if c in right.columns]
    if not common:
        return None

    # Priorisierung: id-artige Namen
    id_re = re.compile(r'(^|_)(id|key|code|nr|number|num)($|_)', re.IGNORECASE)
    id_candidates = [c for c in common if id_re.search(c)]
    if id_candidates:
        # Bevorzuge exakten Match ohne Praefix
        exact = [c for c in id_candidates if re.match(r'^(id|key)$', c, re.IGNORECASE)]
        return exact[0] if exact else id_candidates[0]

    # Fallback: gemeinsame Spalte mit hoechster Kardinalitaet im linken Frame
    best = max(common, key=lambda c: left[c].nunique(), default=None)
    return best


def _auto_join_frames(frames: list[pd.DataFrame], join_type: str = 'left') -> tuple[pd.DataFrame, str | None]:
    """Fuehrt automatischen Left-Join durch wenn genau 2 Frames vorhanden sind
    und ein gemeinsamer Schluessel gefunden wird.
    """
    if len(frames) != 2:
        return _merge_inputs(frames), None

    left, right = frames[0].copy(), frames[1].copy()
    key = _detect_join_key(left, right)
    if key is None:
        return _merge_inputs(frames), 'Auto-Join: kein gemeinsamer Schluessel gefunden, Daten werden zusammengefuehrt (concat).'

    join_map = {'inner': 'inner', 'left': 'left', 'right': 'right', 'full': 'outer', 'outer': 'outer'}
    merged = left.merge(
        right,
        how=join_map.get(join_type, 'left'),
        on=key,
        suffixes=('', '_r'),
    )
    # Duplikat-Suffix-Spalten (aus rechtem Frame) entfernen
    dup_cols = [c for c in merged.columns if c.endswith('_r')]
    if dup_cols:
        merged = merged.drop(columns=dup_cols)
    return merged, f'Auto-Join auf "{key}" ({join_type})'


def _apply_transform_pipeline(frames: list[pd.DataFrame], pipeline_config: dict) -> tuple[pd.DataFrame, dict, str | None]:
    warnings: list[str] = []

    if pipeline_config['join_enabled']:
        frame, join_warning = _apply_join(
            frames,
            {
                'left_key': pipeline_config['join_left_key'],
                'right_key': pipeline_config['join_right_key'],
                'join_type': pipeline_config['join_type'],
            },
        )
        if join_warning:
            warnings.append(join_warning)
    elif pipeline_config.get('auto_join', False) and len(frames) == 2:
        frame, auto_msg = _auto_join_frames(frames, pipeline_config.get('auto_join_type', 'left'))
        if auto_msg:
            warnings.append(auto_msg)
    else:
        frame = _merge_inputs(frames)

    if frame.empty:
        return frame, {
            'target_column': pipeline_config['target_column'] or None,
            'feature_columns': [],
            'numeric_columns': [],
            'categorical_columns': [],
            'encoded_features': 0,
            'scaled_features': 0,
            'operations_applied': [],
        }, '; '.join(warnings) or None

    target_column = pipeline_config['target_column']
    operations_applied: list[str] = []

    # JSON-Flatten: verschachtelte dict-Spalten vektorisiert aufloesen
    if pipeline_config['flatten_json']:
        before_cols = len(frame.columns)
        frame = _flatten_nested_columns(frame)
        if len(frame.columns) != before_cols:
            operations_applied.append('flatten_json')

    # Spaltennamen zu snake_case normalisieren; konfigurierte Spaltenrefs mitanpassen
    if pipeline_config['normalize_column_names']:
        frame = _normalize_column_names(frame)
        target_column = _to_snake_name(target_column) if target_column else target_column
        pipeline_config = {
            **pipeline_config,
            'keep_columns':       [_to_snake_name(c) for c in pipeline_config['keep_columns']],
            'drop_columns':       [_to_snake_name(c) for c in pipeline_config['drop_columns']],
            'numeric_columns':    [_to_snake_name(c) for c in pipeline_config['numeric_columns']],
            'categorical_columns':[_to_snake_name(c) for c in pipeline_config['categorical_columns']],
        }
        operations_applied.append('normalize_column_names')

    if pipeline_config['trim_strings']:
        frame = _trim_string_columns(frame)
        operations_applied.append('trim_strings')

    # Bool-aehnliche Werte (true/false, yes/no, ja/nein) zu 0/1 konvertieren
    if pipeline_config['normalize_booleans']:
        _excl_bool = {target_column} if target_column else set()
        frame, _bool_count = _normalize_boolean_columns(frame, _excl_bool)
        if _bool_count:
            operations_applied.append(f'normalize_booleans({_bool_count})')

    if pipeline_config['convert_numeric']:
        candidate_numeric = pipeline_config['numeric_columns'] or _detect_numeric_columns(frame, {target_column} if target_column else set())
        for column in candidate_numeric:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors='coerce')
        if candidate_numeric:
            operations_applied.append('convert_numeric')

    # Datetime-Strings erkennen, parsen und in numerische Features aufteilen
    if pipeline_config['parse_datetime']:
        _excl_dt = {target_column} if target_column else set()
        _before_dt = len(frame.columns)
        frame = _parse_datetime_columns(frame, _excl_dt)
        if len(frame.columns) != _before_dt:
            operations_applied.append(f'parse_datetime(+{len(frame.columns) - _before_dt}cols)')

    if pipeline_config['query']:
        frame, filter_warning = _apply_filter(frame, {'query': pipeline_config['query']})
        if filter_warning:
            warnings.append(filter_warning)
        else:
            operations_applied.append('filter_rows')

    keep_columns = [column for column in pipeline_config['keep_columns'] if column in frame.columns]
    if keep_columns:
        if target_column and target_column in frame.columns and target_column not in keep_columns:
            keep_columns.append(target_column)
        frame = frame.loc[:, keep_columns]
        operations_applied.append('keep_columns')

    drop_columns = [column for column in pipeline_config['drop_columns'] if column in frame.columns and column != target_column]
    if drop_columns:
        frame = frame.drop(columns=drop_columns)
        operations_applied.append('drop_columns')

    # Konstante Spalten entfernen (keine Varianz = kein ML-Informationsgehalt)
    if pipeline_config['drop_constant_columns']:
        _excl_const = {target_column} if target_column else set()
        frame, _const_dropped = _drop_constant_columns(frame, _excl_const)
        if _const_dropped:
            operations_applied.append(f'drop_constant({len(_const_dropped)})')

    # ID-artige Spalten entfernen (UUID/Hash/run_id, kein ML-Wert)
    if pipeline_config['drop_id_columns']:
        _excl_id = {target_column} if target_column else set()
        frame, _id_dropped = _drop_id_like_columns(frame, _excl_id)
        if _id_dropped:
            operations_applied.append(f'drop_id_cols({len(_id_dropped)})')

    numeric_columns = [column for column in pipeline_config['numeric_columns'] if column in frame.columns and column != target_column]
    if not numeric_columns:
        numeric_columns = _detect_numeric_columns(frame, {target_column} if target_column else set())

    impute_numeric = _normalize_numeric_impute_mode(pipeline_config['impute_numeric'])
    for column in numeric_columns:
        frame[column] = _fill_numeric_series(frame[column], impute_numeric)
    if impute_numeric != 'none' and numeric_columns:
        operations_applied.append('impute_numeric')

    categorical_columns = [column for column in pipeline_config['categorical_columns'] if column in frame.columns and column != target_column]
    if not categorical_columns:
        categorical_columns = _detect_categorical_columns(frame, set(numeric_columns) | ({target_column} if target_column else set()))

    impute_categorical = _normalize_categorical_impute_mode(pipeline_config['impute_categorical'])
    for column in categorical_columns:
        frame[column] = _fill_categorical_series(
            frame[column],
            impute_categorical,
            pipeline_config['categorical_fill_value'],
        )
    if impute_categorical != 'none' and categorical_columns:
        operations_applied.append('impute_categorical')

    # Nach Imputation: jetzt erst Zeilen/Spalten mit verbliebenen NaN droppen
    # how='all': nur komplett leere Zeilen entfernen.
    # Zeilen mit einzelnen null-Werten (z.B. cabin) bleiben erhalten.
    if pipeline_config['drop_empty_rows']:
        frame = frame.dropna(axis=0, how='all')
        operations_applied.append('drop_empty_rows')
    if pipeline_config['drop_empty_columns']:
        frame = frame.dropna(axis=1, how='all')
        operations_applied.append('drop_empty_columns')
    if pipeline_config['drop_duplicates']:
        frame = frame.drop_duplicates().reset_index(drop=True)
        operations_applied.append('drop_duplicates')

    encoded_features = 0
    encode_categorical = pipeline_config['encode_categorical']
    if encode_categorical == 'onehot' and categorical_columns:
        previous_columns = set(frame.columns)
        frame = pd.get_dummies(frame, columns=categorical_columns, dtype=int)
        encoded_features = len(set(frame.columns) - previous_columns)
        operations_applied.append('onehot_encode')
    elif encode_categorical == 'label' and categorical_columns:
        for column in categorical_columns:
            codes, _ = pd.factorize(frame[column], sort=True)
            frame[column] = codes
        numeric_columns = sorted(set(numeric_columns + categorical_columns))
        encoded_features = len(categorical_columns)
        operations_applied.append('label_encode')

    scaled_features = 0
    scale_numeric = pipeline_config['scale_numeric']
    for column in numeric_columns:
        if column not in frame.columns:
            continue
        series = pd.to_numeric(frame[column], errors='coerce')
        if scale_numeric == 'standard':
            std = series.std()
            frame[column] = 0 if pd.isna(std) or std == 0 else (series - series.mean()) / std
            scaled_features += 1
        elif scale_numeric == 'minmax':
            minimum = series.min()
            maximum = series.max()
            frame[column] = 0 if pd.isna(minimum) or pd.isna(maximum) or minimum == maximum else (series - minimum) / (maximum - minimum)
            scaled_features += 1
    if scale_numeric != 'none' and scaled_features:
        operations_applied.append('scale_numeric')

    feature_columns = [str(column) for column in frame.columns if str(column) != target_column]
    summary = {
        'target_column': target_column or None,
        'feature_columns': feature_columns,
        'numeric_columns': [column for column in numeric_columns if column in frame.columns],
        'categorical_columns': [column for column in categorical_columns if column in frame.columns],
        'encoded_features': encoded_features,
        'scaled_features': scaled_features,
        'operations_applied': operations_applied,
    }
    return frame, summary, '; '.join(warnings) or None


def _apply_transform(node: FlowNode, frames: list[pd.DataFrame]) -> tuple[pd.DataFrame, dict]:
    config = node.data.get('config') or {}
    transform_type = _resolve_transform_type(node)
    pipeline_config = _build_transform_config(transform_type, config)
    output, transform_summary, warning = _apply_transform_pipeline(frames, pipeline_config)
    clean_summary = None
    if transform_type == 'clean':
        clean_summary = {
            'trimmed_columns': len(transform_summary.get('numeric_columns') or []) + len(transform_summary.get('categorical_columns') or []),
            'numeric_columns': len(transform_summary.get('numeric_columns') or []),
            'dropped_rows': 0,
            'dropped_columns': 0,
        }

    return output, {
        'transform_type': transform_type,
        'transform_engine': 'pandas+numpy',
        'warning': warning,
        'clean_summary': clean_summary,
        'transform_summary': transform_summary,
    }


def _read_source(node: FlowNode) -> tuple[pd.DataFrame, dict]:
    config = node.data.get('config') or {}
    source_type = str(config.get('source_type') or 'file').strip().lower()
    if source_type == 'postgres':
        records = fetch_records(config)
        storage = 'postgres-source'
    else:
        records = list(config.get('records') or [])
        storage = 'in-memory-file'
    return _coerce_frame(records), {'source_type': source_type, 'storage': storage}


def _write_local_export(node: FlowNode, records: list[dict]) -> int:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    file_stub = ''.join(char for char in node.id if char.isalnum() or char in {'-', '_'}) or node.id
    export_file = EXPORT_DIR / f'{file_stub}.json'
    with export_file.open('w', encoding='utf-8') as handle:
        json.dump(records, handle, indent=2)
    return len(records)


def _execute_load(node: FlowNode, frame: pd.DataFrame, source_labels: list[str]) -> tuple[int, str, str, dict[str, Any]]:
    records = frame.where(pd.notna(frame), None).to_dict(orient='records') if not frame.empty else []
    config = dict(node.data.get('config') or {})
    label = str(node.data.get('label') or '')
    if _should_auto_name_table(config.get('table')):
        config['table'] = _derive_table_name_from_labels(source_labels)
    if str(config.get('table') or '').strip() or 'postgres' in label.lower():
        persisted = write_records(config, records)
        target = f"{config.get('schema') or 'public'}.{config.get('table') or 'table'}"
        return persisted, 'postgres', target, config

    persisted = _write_local_export(node, records)
    target = str(config.get('bucket') or 'local-export')
    return persisted, 'local-json', target, config


def run_flow(payload: FlowPayload) -> dict:
    payload = _connected_component_payload(payload)
    settings = get_settings()
    node_map = {node.id: node for node in payload.nodes}
    incoming = _incoming_map(payload)
    execution_order = _topological_order(payload)
    frame_cache: dict[str, pd.DataFrame] = {}
    node_results: list[dict] = []
    loads: list[dict] = []
    warnings: list[str] = []

    for node_id in execution_order:
        node = node_map[node_id]
        label = str(node.data.get('label') or node_id)
        kind = str(node.data.get('kind') or 'transform')
        input_frames = [frame_cache[source_id] for source_id in incoming.get(node_id, []) if source_id in frame_cache]

        source_type = None
        storage = 'memory-dataframe'
        target = None
        transform_type = None
        transform_engine = None
        clean_summary = None
        transform_summary = None
        persisted_rows = None

        if kind == 'source':
            frame, source_meta = _read_source(node)
            source_type = source_meta['source_type']
            storage = source_meta['storage']
        elif kind == 'transform':
            frame, transform_meta = _apply_transform(node, input_frames)
            transform_type = transform_meta['transform_type']
            transform_engine = transform_meta['transform_engine']
            clean_summary = transform_meta['clean_summary']
            transform_summary = transform_meta['transform_summary']
            if transform_meta['warning']:
                warnings.append(f'{label}: {transform_meta["warning"]}')
        elif kind == 'load':
            frame = _merge_inputs(input_frames)
            source_labels = _collect_upstream_source_labels(node_id, node_map, incoming)
            persisted_rows, storage, target, effective_config = _execute_load(node, frame, source_labels)
            loads.append(
                {
                    'node_id': node_id,
                    'label': label,
                    'target': target,
                    'row_count': int(len(frame)),
                    'persisted_rows': persisted_rows,
                    'storage': storage,
                    'pipeline_sources': source_labels,
                }
            )
        else:
            frame = _merge_inputs(input_frames)

        frame_cache[node_id] = frame
        records, records_truncated = dataframe_records(frame, settings.record_limit)
        preview = dataframe_preview(frame, settings.preview_limit)
        analysis = dataframe_analysis(frame)
        node_results.append(
            {
                'node_id': node_id,
                'label': label,
                'kind': kind,
                'row_count': int(len(frame)),
                'records': records,
                'preview': preview,
                'records_truncated': records_truncated,
                'storage': storage,
                'source_type': source_type,
                'target': target,
                'persisted_rows': persisted_rows,
                'config': effective_config if kind == 'load' else (node.data.get('config') or {}),
                'transform_type': transform_type,
                'transform_engine': transform_engine,
                'clean_summary': clean_summary,
                'transform_summary': transform_summary,
                'analysis': analysis,
            }
        )

    # Gesamtzahl verarbeiteter Zeilen: letze Non-Load-Node (transform/source)
    rows_processed = next(
        (r['row_count'] for r in reversed(node_results) if r.get('kind') != 'load'),
        node_results[-1]['row_count'] if node_results else 0,
    )

    return {
        'run_id': f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}",
        'executed_at': datetime.now(timezone.utc).isoformat(),
        'rows_processed': rows_processed,
        'node_results': node_results,
        'loads': loads,
        'warnings': warnings,
        'analysis': run_analysis(node_results, loads, warnings),
    }
