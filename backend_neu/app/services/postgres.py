from __future__ import annotations

import os
from urllib.parse import quote_plus

import numpy as np
import pandas as pd

from ..config import load_environment

try:
    import psycopg
    from psycopg import sql
except ImportError:  # pragma: no cover
    psycopg = None
    sql = None


def _database_name(config: dict) -> str | None:
    return str(config.get('database') or config.get('db') or '').strip() or None


def resolve_database_url(config: dict | None = None) -> str | None:
    load_environment()
    config = config or {}
    explicit = str(config.get('database_url') or os.getenv('DATABASE_URL') or '').strip()
    if explicit:
        return explicit

    host = str(config.get('host') or os.getenv('POSTGRES_HOST') or '').strip()
    database = _database_name(config) or str(os.getenv('POSTGRES_DB') or '').strip()
    user = str(config.get('user') or os.getenv('POSTGRES_USER') or '').strip()
    password = str(config.get('password') or os.getenv('POSTGRES_PASSWORD') or '').strip()
    if not (host and database and user):
        return None

    port = int(config.get('port') or os.getenv('POSTGRES_PORT') or 5432)
    return f'postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}'


def get_connection(config: dict | None = None):
    if psycopg is None:
        raise ValueError('psycopg ist nicht installiert.')

    database_url = resolve_database_url(config)
    if not database_url:
        raise ValueError('PostgreSQL Konfiguration fehlt. Bitte host/db/user oder DATABASE_URL setzen.')

    try:
        return psycopg.connect(database_url)
    except Exception as error:
        raise ValueError(f'PostgreSQL Verbindung fehlgeschlagen: {error}') from error


def fetch_records(config: dict) -> list[dict]:
    table = str(config.get('table') or '').strip()
    if not table:
        raise ValueError('PostgreSQL Source benoetigt eine Tabelle.')

    schema = str(config.get('schema') or 'public').strip()
    limit = max(1, int(config.get('limit') or 200))

    with get_connection(config) as connection:
        with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cursor.execute(
                sql.SQL('SELECT * FROM {}.{} LIMIT {}').format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Literal(limit),
                )
            )
            rows = cursor.fetchall()

    return [dict(row) for row in rows]


def _sql_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    if pd.api.types.is_integer_dtype(series):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMPTZ'
    return 'TEXT'


def _prepare_frame(records: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame

    for column in frame.columns:
        series = frame[column]
        if series.dtype == object:
            non_null = series.dropna()
            if non_null.empty:
                continue
            converted = pd.to_numeric(series, errors='coerce')
            if converted.notna().sum() >= max(1, len(non_null) // 2):
                frame[column] = converted.replace({np.nan: None})
    return frame


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(
        '''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        ''',
        (schema, table),
    )
    row = cursor.fetchone()
    return bool(row[0]) if row else False


def _existing_columns(cursor, schema: str, table: str) -> list[str]:
    cursor.execute(
        '''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        ''',
        (schema, table),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _parse_foreign_keys(raw) -> list[dict]:
    """Parst verschiedene FK-Formate zu [{'column': str, 'ref_table': str, 'ref_column': str, 'ref_schema': str}].
    Akzeptiert:
    - Liste von dicts: [{column, ref_table, ref_column}]
    - String: "col->schema.table.ref_col" oder "col->table.ref_col" (kommasepariert)
    """
    if not raw:
        return []
    if isinstance(raw, list):
        result = []
        for item in raw:
            if isinstance(item, dict) and item.get('column') and item.get('ref_table'):
                result.append({
                    'column':     str(item['column']).strip(),
                    'ref_schema': str(item.get('ref_schema') or 'public').strip(),
                    'ref_table':  str(item['ref_table']).strip(),
                    'ref_column': str(item.get('ref_column') or item['column']).strip(),
                })
        return result
    if isinstance(raw, str):
        result = []
        for part in raw.split(','):
            part = part.strip()
            if '->' not in part:
                continue
            col, ref = part.split('->', 1)
            col = col.strip()
            ref_parts = ref.strip().split('.')
            if len(ref_parts) == 3:
                ref_schema, ref_table, ref_col = ref_parts
            elif len(ref_parts) == 2:
                ref_schema, ref_table, ref_col = 'public', ref_parts[0], ref_parts[1]
            else:
                continue
            result.append({'column': col, 'ref_schema': ref_schema.strip(), 'ref_table': ref_table.strip(), 'ref_column': ref_col.strip()})
        return result
    return []


def write_records(config: dict, records: list[dict]) -> int:
    table = str(config.get('table') or '').strip()
    if not table:
        raise ValueError('PostgreSQL Load benoetigt eine Tabelle.')
    if not records:
        return 0

    schema = str(config.get('schema') or 'public').strip()
    mode = str(config.get('mode') or 'append').strip().lower()
    primary_key = str(config.get('primary_key') or '').strip()
    foreign_keys = _parse_foreign_keys(config.get('foreign_keys'))
    frame = _prepare_frame(records)
    columns = [str(column) for column in frame.columns]
    if not columns:
        return 0

    create_columns = [
        sql.SQL('{} {}').format(
            sql.Identifier(column),
            sql.SQL(
                _sql_type_for_series(frame[column])
                + (' PRIMARY KEY' if primary_key and column == primary_key else '')
            ),
        )
        for column in columns
    ]
    placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in columns)
    insert_query = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({})').format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(', ').join(sql.Identifier(column) for column in columns),
        placeholders,
    )

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
            if mode == 'replace':
                cursor.execute(sql.SQL('DROP TABLE IF EXISTS {}.{}').format(sql.Identifier(schema), sql.Identifier(table)))
                cursor.execute(
                    sql.SQL('CREATE TABLE {}.{} ({})').format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.SQL(', ').join(create_columns),
                    )
                )
            else:
                if _table_exists(cursor, schema, table):
                    existing_columns = _existing_columns(cursor, schema, table)
                    if set(existing_columns) != set(columns):
                        raise ValueError(
                            f'Bestehende Tabelle {schema}.{table} hat ein anderes Schema. '
                            f'Verwende eine neue Tabelle oder setze den Load-Mode auf replace. '
                            f'Vorhandene Spalten: {existing_columns}. Neue Spalten: {columns}.'
                        )
                else:
                    cursor.execute(
                        sql.SQL('CREATE TABLE {}.{} ({})').format(
                            sql.Identifier(schema),
                            sql.Identifier(table),
                            sql.SQL(', ').join(create_columns),
                        )
                    )
            cursor.executemany(insert_query, frame.where(pd.notna(frame), None).itertuples(index=False, name=None))

            # Foreign Key Constraints (nach INSERT, damit FK-Tabellen bereits befuellt sind)
            for fk in foreign_keys:
                fk_col = fk['column']
                if fk_col not in columns:
                    continue
                constraint_name = f'fk_{table}_{fk_col}'
                cursor.execute(
                    sql.SQL(
                        'ALTER TABLE {schema}.{tbl} '
                        'DROP CONSTRAINT IF EXISTS {cname}; '
                        'ALTER TABLE {schema}.{tbl} '
                        'ADD CONSTRAINT {cname} FOREIGN KEY ({col}) '
                        'REFERENCES {rschema}.{rtbl} ({rcol})'
                    ).format(
                        schema=sql.Identifier(schema),
                        tbl=sql.Identifier(table),
                        cname=sql.Identifier(constraint_name),
                        col=sql.Identifier(fk_col),
                        rschema=sql.Identifier(fk['ref_schema']),
                        rtbl=sql.Identifier(fk['ref_table']),
                        rcol=sql.Identifier(fk['ref_column']),
                    )
                )
        connection.commit()

    return len(frame)
