import tomllib
from pathlib import Path
from dataclasses import dataclass, field
from loguru import logger as log

def _resolve_config_path(name='config.toml') -> Path:
    return Path(__file__).parent.parent / 'config' / name

def _resolve_data_config_path(name='inbound') -> Path:
    return Path(__file__).parent.parent / 'ingest' / name

@dataclass
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

@dataclass
class IngestConfig:
    watch_dir: str | Path = field(default_factory=lambda: _resolve_data_config_path())
    config_path: str | Path = field(default_factory=lambda: _resolve_config_path('data_config.toml'))
    file_suffix: str = ".jsonl"
    flush_period: int = 10
    flush_max_batch: int = 1000

@dataclass
class RootConfig:
    app: AppConfig = field(default_factory=AppConfig)
    ingest: IngestConfig = field(default_factory=IngestConfig)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "RootConfig":

        if path is None:
            path = _resolve_config_path()

        path = Path(path)
        if not path.exists():
            log.warning(f"Config file not found: {path}")
            return cls()

        with open(path, "rb") as f:
            raw = tomllib.load(f)
            app = raw.get("app", {})
            ingest = raw.get("ingest", {})

        return cls(
            app=AppConfig(**app),
            ingest=IngestConfig(**ingest)
        )
