export function shortPreview(text, maxLength = 220) {
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}

function parseCsvRows(text, delimiter = ',') {
  const rows = [];
  let currentRow = [];
  let currentCell = '';
  let insideQuotes = false;

  const pushCell = () => {
    currentRow.push(currentCell.trim());
    currentCell = '';
  };

  const pushRow = () => {
    const hasContent = currentRow.some((cell) => cell !== '');
    if (hasContent) {
      rows.push(currentRow);
    }
    currentRow = [];
  };

  const normalizedText = text.replace(/^\uFEFF/, '');

  for (let index = 0; index < normalizedText.length; index += 1) {
    const char = normalizedText[index];
    const nextChar = normalizedText[index + 1];

    if (char === '"') {
      if (insideQuotes && nextChar === '"') {
        currentCell += '"';
        index += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
      continue;
    }

    if (char === delimiter && !insideQuotes) {
      pushCell();
      continue;
    }

    if ((char === '\n' || char === '\r') && !insideQuotes) {
      if (char === '\r' && nextChar === '\n') {
        index += 1;
      }
      pushCell();
      pushRow();
      continue;
    }

    currentCell += char;
  }

  if (currentCell.length > 0 || currentRow.length > 0) {
    pushCell();
    pushRow();
  }

  return rows;
}

export function parseCsv(text) {
  const rows = parseCsvRows(text);

  if (!rows.length) {
    throw new Error('CSV ist leer.');
  }

  const headers = rows[0].map((item) => item.trim());
  const records = rows
    .slice(1)
    .filter((cells) => cells.some((cell) => cell !== ''))
    .map((cells) => {
      const row = {};
      headers.forEach((header, index) => {
        row[header || `col_${index + 1}`] = cells[index] ?? '';
      });
      return row;
    });

  return {
    format: 'csv',
    delimiter: ',',
    headers,
    records,
    sample_rows: records.slice(0, 5),
    row_count: records.length,
  };
}

/**
 * Loest MongoDB Extended JSON Felder auf:
 * {"$oid": "..."} -> String, {"$date": "..."} -> String
 */
function resolveMongoDB(value) {
  if (Array.isArray(value)) return value.map(resolveMongoDB);
  if (value && typeof value === 'object') {
    if ('$oid' in value) return String(value.$oid);
    if ('$date' in value) return String(value.$date);
    const result = {};
    for (const [k, v] of Object.entries(value)) result[k] = resolveMongoDB(v);
    return result;
  }
  return value;
}

/**
 * Erkennt NDJSON (Newline-Delimited JSON): jede Zeile ist ein eigenes JSON-Objekt.
 * Gibt null zurueck wenn kein NDJSON.
 */
function tryParseNdjson(text) {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (lines.length < 2) return null;
  // Pruefen: erste Zeile muss { sein, zweite Zeile auch (kein Array)
  if (!lines[0].startsWith('{') || !lines[1].startsWith('{')) return null;
  const records = [];
  for (const line of lines) {
    try {
      records.push(JSON.parse(line));
    } catch {
      // teilweise ungueltiges NDJSON: ignoriere fehlerhafte Zeilen
    }
  }
  if (records.length < 1) return null;
  return records;
}

export function parseJson(text) {
  // NDJSON-Erkennung vor Standard-JSON.parse
  const ndjsonRecords = tryParseNdjson(text);
  if (ndjsonRecords) {
    const resolved = ndjsonRecords.map(resolveMongoDB);
    const keys = resolved[0] ? Object.keys(resolved[0]) : [];
    return {
      format: 'ndjson',
      structure: 'ndjson',
      keys,
      records: resolved.slice(0, 5000),
      item_count: ndjsonRecords.length,
      sample: resolved.slice(0, 3),
    };
  }

  const parsed = JSON.parse(text);

  const findFirstObjectArray = (value, path = '$') => {
    if (Array.isArray(value)) {
      const objectItems = value.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
      if (objectItems.length) {
        return { records: objectItems, path };
      }

      for (let index = 0; index < value.length; index += 1) {
        const result = findFirstObjectArray(value[index], `${path}[${index}]`);
        if (result) {
          return result;
        }
      }

      return null;
    }

    if (value && typeof value === 'object') {
      for (const [key, nested] of Object.entries(value)) {
        const result = findFirstObjectArray(nested, `${path}.${key}`);
        if (result) {
          return result;
        }
      }
    }

    return null;
  };

  if (Array.isArray(parsed)) {
    const records = parsed.filter((item) => item && typeof item === 'object').slice(0, 5000);
    const resolved = records.map(resolveMongoDB);
    const firstRow = resolved[0];
    const keys = firstRow && typeof firstRow === 'object' ? Object.keys(firstRow) : [];

    return {
      format: 'json',
      structure: 'array',
      keys,
      records: resolved,
      item_count: parsed.length,
      sample: resolved.slice(0, 3),
    };
  }

  if (parsed && typeof parsed === 'object') {
    const nestedResult = findFirstObjectArray(parsed);
    const nestedRecords = (nestedResult?.records ?? []).map(resolveMongoDB);

    return {
      format: 'json',
      structure: 'object',
      keys: Object.keys(parsed),
      records: nestedRecords.length ? nestedRecords : [parsed],
      records_path: nestedResult?.path,
      sample: parsed,
    };
  }

  return {
    format: 'json',
    structure: typeof parsed,
    records: [{ value: parsed }],
    value: parsed,
  };
}

export async function parseUploadedFile(file) {
  const extension = file.name.split('.').pop()?.toLowerCase();
  const isJson = ['json', 'ndjson', 'jsonl'].includes(extension);

  if (extension !== 'csv' && !isJson) {
    throw new Error('Nur CSV, JSON, NDJSON oder JSONL Dateien werden unterstuetzt.');
  }

  const content = await file.text();
  const parsedConfig = extension === 'csv' ? parseCsv(content) : parseJson(content);

  return {
    extension,
    content,
    parsedConfig,
    config: {
      source_type: 'file',
      file_name: file.name,
      file_size: file.size,
      ...parsedConfig,
      content_preview: shortPreview(content),
    },
    label: `${extension.toUpperCase()} File: ${file.name}`,
  };
}
