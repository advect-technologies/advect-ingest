import string
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from random import choices
from typing import Any

from loguru import logger as log


def generate_random_suffix() -> str:
    return "".join(choices(string.ascii_lowercase + string.digits, k=8))


def _resolve_config_path(name: str = "config.toml") -> Path:
    return Path(__file__).parent.parent / "config" / name


def _resolve_ingest_path(name: str = "inbound") -> Path:
    return Path(__file__).parent.parent / "ingest" / name


@dataclass
class AppConfig:
    log_level: str = "INFO"
    ha_config_path: str | Path = field(
        default_factory=lambda: _resolve_config_path("ha_config.toml")
    )


@dataclass
class IngestConfig:
    watch_dir: str | Path = field(default_factory=lambda: _resolve_ingest_path())
    config_path: str | Path = field(
        default_factory=lambda: _resolve_config_path("data_config.toml")
    )
    file_suffix: str = ".jsonl"
    flush_period: int = 10
    flush_max_batch: int = 1000


@dataclass
class HAMqttConfig:
    broker: str = "homeassistant"
    port: int = 1883
    username: str = ""
    password: str = ""
    client_id: str = "advect-ingest-ha-" + generate_random_suffix()
    discovery_prefix: str = "homeassistant"
    state_prefix: str = "advect"


@dataclass
class HASensor:
    """One sensor/entity that belongs to a device."""

    field: str  # key that appears in DataPoint.fields
    unique_id: str
    name: str
    device_class: str | None = None
    unit_of_measurement: str | None = None
    state_class: str | None = None
    icon: str | None = None

    def build_component(self, state_topic: str) -> dict:
        """Build the component entry used inside a Device Discovery payload."""
        comp = {k: v for k, v in asdict(self).items() if v is not None}
        comp.pop("field")
        comp["p"] = "sensor"
        comp["state_topic"] = state_topic

        return comp


@dataclass
class HADevice:
    """A physical device (e.g. one Pico)."""

    id: str  # hardware serial – matches tags["id"]
    name: str
    model: str | None = None
    manufacturer: str | None = "Advect"
    sensors: list[HASensor] = field(default_factory=list)

    def find_sensor(self, field_name: str) -> HASensor | None:
        for sensor in self.sensors:
            if sensor.field == field_name:
                return sensor
        return None

    def build_discovery_payload(self, state_prefix: str) -> dict[str, Any]:
        """Build the full Device Discovery payload."""
        components = {}
        for sensor in self.sensors:
            state_topic = f"{state_prefix}/{self.id}/{sensor.field}/state"
            components[sensor.field] = sensor.build_component(state_topic)

        payload = {
            "device": {
                "identifiers": [self.id],
                "name": self.name,
            },
            "origin": {"name": "advect-ingest"},
            "cmps": components,
        }

        if self.model:
            payload["device"]["model"] = self.model
        if self.manufacturer:
            payload["device"]["manufacturer"] = self.manufacturer

        return payload


@dataclass
class HAConfig:
    mqtt: HAMqttConfig = field(default_factory=HAMqttConfig)
    devices: list[HADevice] = field(default_factory=list)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "HAConfig":
        if path is None:
            path = _resolve_config_path("ha_config.toml")
        path = Path(path)

        if not path.exists():
            log.warning(f"HA config not found: {path}")
            return cls()

        with open(path, "rb") as f:
            raw = tomllib.load(f)

        mqtt = HAMqttConfig(**raw.get("mqtt", {}))

        devices = []
        for d in raw.get("devices", []):
            sensors = [HASensor(**s) for s in d.pop("sensors", [])]
            devices.append(HADevice(**d, sensors=sensors))

        return cls(mqtt=mqtt, devices=devices)

    def find_device(self, device_id: str) -> HADevice | None:
        for device in self.devices:
            if device.id == device_id:
                return device
        return None

    @property
    def active(self) -> bool:
        """Return True only if there is at least one device defined."""
        return len(self.devices) > 0


@dataclass
class RootConfig:
    app: AppConfig = field(default_factory=AppConfig)
    ingest: IngestConfig = field(default_factory=IngestConfig)
    ha: HAConfig = field(default_factory=HAConfig)

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
            ingest=IngestConfig(**ingest),
            ha=HAConfig.load(app.get("ha_config_path")),
        )
