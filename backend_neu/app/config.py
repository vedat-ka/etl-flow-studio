from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = ROOT_DIR.parent
DATA_DIR = ROOT_DIR / 'data'
FLOW_DIR = DATA_DIR / 'flows'
EXPORT_DIR = DATA_DIR / 'exports'


def load_environment() -> None:
    env_candidates = [
        ROOT_DIR / '.env',
        WORKSPACE_DIR / '.env',
    ]
    for env_path in env_candidates:
        if env_path.exists():
            load_dotenv(env_path, override=False)

    if not os.getenv('DATABASE_URL'):
        example_path = WORKSPACE_DIR / '.env.example'
        if example_path.exists():
            load_dotenv(example_path, override=False)


@dataclass(frozen=True)
class Settings:
    cors_origins: list[str]
    preview_limit: int = 5
    record_limit: int = 200


def get_settings() -> Settings:
    load_environment()
    origin_text = os.getenv('CORS_ORIGINS', 'http://localhost:5173')
    origins = [item.strip() for item in origin_text.split(',') if item.strip()]
    return Settings(cors_origins=origins)
