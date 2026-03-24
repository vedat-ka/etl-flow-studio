from __future__ import annotations

import pandas as pd


def _top_items(series: pd.Series, limit: int = 8) -> list[dict]:
    counts = series.fillna('UNKNOWN').astype(str).replace('', 'UNKNOWN').value_counts().head(limit)
    return [
        {
            'name': str(name),
            'count': int(count),
        }
        for name, count in counts.items()
    ]


def _build_chart_groups(frame: pd.DataFrame) -> list[dict]:
    chart_groups: list[dict] = []
    categorical_candidates = [
        str(column)
        for column in frame.columns
        if frame[column].dtype == object or str(frame[column].dtype).startswith('string') or str(frame[column].dtype) == 'category'
    ]

    for column in categorical_candidates[:4]:
        items = _top_items(frame[column])
        if items:
            chart_groups.append(
                {
                    'column': column,
                    'label': f'Top-Werte fuer {column}',
                    'items': items,
                }
            )

    return chart_groups


def _frame_to_records(frame: pd.DataFrame) -> list[dict]:
    normalized = frame.astype(object).where(pd.notna(frame), None)
    return normalized.to_dict(orient='records')


def dataframe_preview(frame: pd.DataFrame, limit: int) -> list[dict]:
    if frame.empty:
        return []
    return _frame_to_records(frame.head(limit))


def dataframe_records(frame: pd.DataFrame, limit: int) -> tuple[list[dict], bool]:
    if frame.empty:
        return [], False
    truncated = len(frame) > limit
    limited = frame.head(limit)
    return _frame_to_records(limited), truncated


def dataframe_analysis(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {
            'row_count': 0,
            'column_count': 0,
            'columns': [],
            'completeness': [],
            'numeric_summaries': [],
            'top_values': [],
            'chart_groups': [],
            'preview_rows': [],
            'target_distribution': [],
        }

    completeness = []
    top_values = []
    numeric_summaries = []

    for column in frame.columns:
      series = frame[column]
      non_null = int(series.notna().sum())
      completeness.append({
          'column': str(column),
          'filled': non_null,
          'missing': int(len(series) - non_null),
      })

      top_counts = series.astype('string').fillna('<null>').value_counts().head(5)
      top_values.append({
          'column': str(column),
          'values': [
              {'label': str(index), 'count': int(value)}
              for index, value in top_counts.items()
          ],
      })

      numeric = pd.to_numeric(series, errors='coerce')
      if numeric.notna().sum():
          numeric_summaries.append({
              'column': str(column),
              'min': float(numeric.min()),
              'max': float(numeric.max()),
              'mean': float(numeric.mean()),
              'nulls': int(numeric.isna().sum()),
          })

    target_distribution = []
    for candidate in ['target', 'label', 'class', 'y']:
        if candidate in frame.columns:
            target_distribution = _top_items(frame[candidate], limit=12)
            break

    return {
        'row_count': int(len(frame)),
        'column_count': int(len(frame.columns)),
        'columns': [str(column) for column in frame.columns],
        'completeness': completeness,
        'numeric_summaries': numeric_summaries,
        'top_values': top_values,
        'chart_groups': _build_chart_groups(frame),
        'preview_rows': _frame_to_records(frame.head(24)),
        'target_distribution': target_distribution,
    }


def run_analysis(node_results: list[dict], loads: list[dict], warnings: list[str]) -> dict:
    row_series = [int(item.get('row_count') or 0) for item in node_results]
    step_kinds: dict[str, int] = {}
    for item in node_results:
        kind = str(item.get('kind') or 'unknown')
        step_kinds[kind] = step_kinds.get(kind, 0) + 1

    deltas = []
    for index, rows in enumerate(row_series):
        previous = row_series[index - 1] if index > 0 else rows
        deltas.append(rows - previous if index > 0 else 0)

    return {
        'step_count': len(node_results),
        'warning_count': len(warnings),
        'load_count': len(loads),
        'row_series': row_series,
        'row_deltas': deltas,
        'step_kinds': step_kinds,
    }
