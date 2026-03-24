# React Flow ETL Dashboard (Python + React)

Drag-and-Drop ETL Dashboard mit React Flow im Frontend und FastAPI im Backend.

## Dashboard Screenshot

![Dashboard Screenshot](Screenshot-Dashboard.png)

## Architektur

- Sidebar: Bibliothek mit ETL-Nodes (`source`, `transform`, `load`)
- Canvas: Node-basierter Graph mit Verbindungen (Edges)
- Properties Panel: Parameter-Bearbeitung pro selektierter Node
- Backend: Persistenz des Flows als JSON (`nodes`, `edges`)

## Features

- Drag-and-Drop von Node-Templates aus der Sidebar auf das Canvas
- CSV/JSON Import aus der Sidebar (Datei wird als Source-Node angelegt)
- Expliziter `PostgreSQL Load`-Node fuer lokales Docker-Postgres
- Custom ETL-Nodes mit visuellen Typen (`source`, `transform`, `load`)
- Validierung von Verbindungen:
  - `load` darf keinen Ausgang haben
  - `source` darf kein Ziel sein
  - Zyklen (cycles) werden verhindert
- Speichern/Laden des Graphen via FastAPI (`/api/flows/{flow_id}`)
- Pipeline-Ausfuehrung ueber Backend (`POST /api/execute`)
- Edge-Loeschen per Klick im Canvas
- Step-Detaildialog mit Pagination und Download (JSON/CSV)
- Klick auf Node oeffnet Daten-Popup aus letztem Run
- Flow-Auto-Save und Auto-Load beim Browser-Reload
- Load-Output kann in PostgreSQL gespeichert werden
- Zustandverwaltung mit Zustand Store

## Projektstruktur

```text
etl-flow-studio/
  backend/
    app/
      main.py
      schemas.py
      storage.py
    data/flows/
    requirements.txt
  frontend/
    src/
      api/flowApi.js
      components/
      store/flowStore.js
```

## PostgreSQL lokal in Docker starten

```powershell
docker compose up -d postgres
```

Danach ist PostgreSQL lokal verfuegbar unter `localhost:5432` mit:

- DB: `etl`
- User: `postgres`
- Passwort: `postgres`

Die Daten bleiben im Docker-Volume `postgres_data` erhalten.

Optional pruefen:

```powershell
docker compose ps
```

## Backend starten (FastAPI)

```powershell
cd backend
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
$env:DATABASE_URL = "postgresql://<user>:<password>@localhost:5432/<database>"
python -m uvicorn app.main:app --reload --port 8000
```

Beispiele fuer `DATABASE_URL`:
- Mit der Projekt-Compose-DB aus [compose.yaml](compose.yaml): `postgresql://postgres:postgres@localhost:5432/etl`
- Mit einer bereits laufenden externen DB: `postgresql://<user>:<password>@localhost:5432/<database>`

Hinweis: Falls bei der Installation Fehler mit `pydantic-core` auftreten, wird sehr wahrscheinlich eine nicht unterstuetzte Python-Version (z. B. 3.15 alpha) verwendet. Nutze in dem Fall explizit `py -3.12` oder `py -3.11`.

Wichtig:
- Wenn `.venv` beschaedigt ist und beim Start `No pyvenv.cfg file` meldet, nutze die funktionierende Umgebung oder erstelle die venv neu.
- In diesem Fall funktioniert zum Beispiel:

```powershell
cd backend
.\.venv312\Scripts\Activate.ps1
$env:DATABASE_URL = "postgresql://<user>:<password>@localhost:5432/<database>"
python -m uvicorn app.main:app --reload --port 8000
```

- Wenn auf `localhost:5432` bereits ein anderer PostgreSQL-Container laeuft, muss `DATABASE_URL` zu dessen echten Credentials passen. Ein falscher User oder ein falsches Passwort fuehrt dazu, dass `PostgreSQL Load` nur im Vorschau-Modus bleibt oder mit Verbindungsfehler abbricht.
- Wenn pgAdmin meldet `database "postgres-db" does not exist`, wurde der Container-Name als Datenbankname eingetragen. Fuer eine externe DB ist korrekt:
  - Host: `localhost`
  - Port: `5432`
  - Maintenance database: `<database>`
  - Username: `<user>`
  - Password: `<password>`
- Das aktive Backend ist `backend_neu` auf Port `8000`: `python -m uvicorn app.main:app --reload --port 8000`.

## Frontend starten (Vite)

```powershell
cd frontend
npm install
npm run dev
```

Frontend laeuft standardmaessig unter `http://localhost:5173`.
Backend laeuft unter `http://localhost:8000`.

## API

- `GET /health`
- `GET /api/flows/{flow_id}`
- `POST /api/flows/{flow_id}`
- `POST /api/execute`

## Pipeline ausfuehren

1. Source mit CSV/JSON laden
2. Source mit Transform und Load verbinden
3. Oben auf `Pipeline ausfuehren` klicken
4. `Step-Daten` oeffnen und je Step Details/Pagination pruefen
5. Optional je Step als JSON oder CSV herunterladen

Hinweis zu PostgreSQL-Load:
- Wenn eine Load-Node in der Config ein Feld `table` hat (z. B. `fact_orders`), werden Run-Daten in diese Tabelle geschrieben.
- Tabelle wird automatisch erstellt mit den Spalten `run_id`, `node_id`, `payload`, `created_at`.
- Ohne `DATABASE_URL` bleibt Load auf Vorschau-Modus (`preview-only`).
- Dieselbe PostgreSQL-DB speichert auch die Pipeline selbst in `etl_flows`, dadurch bleiben Flows nach Browser-Reload erhalten.
- Alternativ kann der `PostgreSQL Load`-Node eigene Felder wie `host`, `port`, `db`, `user`, `password` und `schema` verwenden. Diese Werte ueberschreiben die globale `DATABASE_URL` fuer diesen Load-Schritt.

Beispiel fuer eine Load-Node-Config mit Docker-Postgres:

```json
{
  "schema": "public",
  "table": "fact_orders"
}
```

## PostgreSQL Handling

- Die ETL-Daten werden aktuell in `public.fact_orders` gespeichert.
- Die Pipeline-Konfiguration selbst wird in `public.etl_flows` gespeichert.
- Die aktive Verbindung sollte als Umgebungsvariable gesetzt werden, zum Beispiel:
  - `DATABASE_URL=postgresql://<user>:<password>@localhost:5432/<database>`
- Wenn du pgAdmin verwendest, ist korrekt:
  - Host: `localhost`
  - Port: `5432`
  - Maintenance database: `<database>`
  - Username: `<user>`
  - Password: `<password>`
- `postgres-db` ist nur ein Container-Name und darf nicht als Datenbankname eingetragen werden.
- Wenn der Run im UI `preview-only` meldet, verwendet der `PostgreSQL Load`-Node meist noch alte oder falsche Zugangsdaten.
- Falls eine alte gespeicherte Load-Node noch `postgres/postgres/etl` verwendet, muss sie auf die echten Ziel-Credentials umgestellt werden oder auf die globale `DATABASE_URL` zurueckfallen.
- Datenpruefung direkt in PostgreSQL:

```sql
SELECT count(*) FROM public.fact_orders;
SELECT * FROM public.fact_orders ORDER BY id DESC LIMIT 20;
```

- Der eigentliche Nutzinhalt liegt in der Spalte `payload` vom Typ `JSONB`.

Beispiel-Payload:

```json
{
  "nodes": [
    {
      "id": "source-1",
      "type": "etl",
      "position": { "x": 120, "y": 140 },
      "data": { "kind": "source", "label": "PostgreSQL Source", "config": {} }
    }
  ],
  "edges": [
    {
      "id": "e-source-transform",
      "source": "source-1",
      "target": "transform-1",
      "animated": true
    }
  ]
}
```
