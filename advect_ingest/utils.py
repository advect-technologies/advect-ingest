import asyncio
import datetime as dt
import json
import uuid
from pathlib import Path
from typing import Any

from aiomqtt import Client
from loguru import logger as log

from .config import HAConfig, HADevice, HASensor, IngestConfig
from .models import DataPoint


class IngestQueue:
    """Async queue + periodic flush to JSONL for daq-tools watcher."""

    def __init__(self, config: IngestConfig):
        self.config = config
        self.queue: asyncio.Queue[DataPoint] = asyncio.Queue()
        self.flush_task: asyncio.Task | None = None
        self._running = False
        self._lock = asyncio.Lock()
        self._flush_now = asyncio.Event()

    async def start(self):
        """Start background flush task."""
        self._running = True
        self.flush_task = asyncio.create_task(self._flush_loop())
        log.info("IngestQueue background flush started")

    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()  # final flush
        log.info("IngestQueue stopped")

    async def add(self, datapoint: DataPoint):
        """Add a single point to the queue."""
        await self.queue.put(datapoint)
        if self.queue.qsize() >= self.config.flush_max_batch:
            self._flush_now.set()

    async def add_batch(self, datapoints: list[DataPoint]):
        """Add multiple points."""
        for dp in datapoints:
            await self.queue.put(dp)
        if self.queue.qsize() >= self.config.flush_max_batch:
            self._flush_now.set()

    async def _flush_loop(self):
        """Periodic flush."""
        while self._running:
            try:
                async with asyncio.timeout(self.config.flush_period):
                    await self._flush_now.wait()
                    self._flush_now.clear()
                    log.debug("Flush triggered")
            except TimeoutError:
                log.debug("Flush timeout")
            await self.flush()

    async def flush(self):
        """Write current queue to a single JSONL file."""

        points = []
        while len(points) < self.config.flush_max_batch and not self.queue.empty():
            points.append(self.queue.get_nowait())

        if not points:
            return

        watch_dir = Path(self.config.watch_dir)
        watch_dir.mkdir(parents=True, exist_ok=True)

        timestamp = dt.datetime.now(tz=dt.UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{uuid.uuid4().hex[:8]}{self.config.file_suffix}"
        file_path = watch_dir / filename

        points = [dp.model_dump_json() for dp in points]

        try:
            file_path.write_text("\n".join(points), encoding="utf-8")
            log.info(f"Flushed {len(points)} datapoint(s) → {file_path.name}")
        except Exception as e:
            log.error(f"Flush failed: {e}")
            # Could re-queue or handle errors here


class HAMqttPublisher:
    def __init__(self, config: HAConfig):
        self.config = config
        self.client: Client | None = None
        self._discovered: set[str] = (
            set()
        )  # track device ids we've already sent discovery for

    async def start(self):
        if not self.config.active:
            return

        mqtt = self.config.mqtt
        self.client = Client(
            hostname=mqtt.broker,
            port=mqtt.port,
            username=mqtt.username or None,
            password=mqtt.password or None,
            identifier=mqtt.client_id,
        )
        await self.client.__aenter__()
        log.info(f"HA MQTT publisher connected to {mqtt.broker}:{mqtt.port}")

    async def stop(self):
        if self.client:
            await self.client.__aexit__(None, None, None)
            self.client = None
            log.info("HA MQTT publisher stopped")

    async def publish_point(self, device_id: str, fields: dict[str, Any]):
        """Main entry point – call this from the /ha/state endpoint."""
        device = self.config.find_device(device_id)
        if not device:
            return

        # Publish device discovery once
        if device.id not in self._discovered:
            await self._publish_device_discovery(device)
            self._discovered.add(device.id)

        # Publish state for every field that has a matching sensor
        for field_name, value in fields.items():
            sensor = device.find_sensor(field_name)
            if sensor:
                await self._publish_state(device, sensor, value)

    async def _publish_device_discovery(self, device: HADevice):
        if not self.client:
            return
        mqtt = self.config.mqtt
        topic = f"{mqtt.discovery_prefix}/device/{device.id}/config"
        # topic = "testtopic"
        payload = device.build_discovery_payload(mqtt.state_prefix)

        await self.client.publish(topic, json.dumps(payload), qos=0, retain=False)
        log.info(f"HA device discovery published for {device.id}")
        log.debug(f"HA discovery → {topic} = {payload}")

    async def _publish_state(self, device: HADevice, sensor: HASensor, value: Any):
        if not self.client:
            return
        topic = f"{self.config.mqtt.state_prefix}/{device.id}/{sensor.field}/state"
        await self.client.publish(topic, str(value), qos=1, retain=False)
        log.debug(f"HA state → {topic} = {value}")
