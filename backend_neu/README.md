# backend_neu

Sauberes zweites Backend fuer das bestehende React-Frontend.

Ziele:
- wenig Redundanz
- kleine, klare Module statt einer grossen Executor-Datei
- ETL-Transforms mit pandas und numpy
- Diagramm-Auswertungen direkt im API-Resultat
- bestehendes Frontend kann weiterverwendet werden

## Start

```powershell
cd backend_neu
pip install -r requirements.txt
$env:DATABASE_URL = "postgresql://<user>:<password>@localhost:5432/<database>"
python -m uvicorn app.main:app --reload --port 8000
```

Das Frontend verwendet standardmaessig `http://localhost:8000`.

Hinweis:
- `python -m uvicorn` verwendet sicher den Interpreter, in dem auch `requirements.txt` installiert wurde.
- Das ist auf Windows wichtig, wenn mehrere Python-Versionen parallel installiert sind.

## Endpunkte

- `GET /health`
- `GET /api/flows`
- `GET /api/flows/{flow_id}`
- `POST /api/flows/{flow_id}`
- `POST /api/execute`
- `POST /api/sources/postgres/preview`

## Struktur

- `app/main.py`: FastAPI-Endpunkte
- `app/config.py`: Konfiguration und Pfade
- `app/schemas.py`: Pydantic-Modelle
- `app/services/storage.py`: Flow-Speicherung
- `app/services/postgres.py`: PostgreSQL lesen/schreiben
- `app/services/analytics.py`: Kennzahlen fuer Diagramme
- `app/services/etl.py`: schlanke ETL-Pipeline
