import asyncio
import datetime as dt
import uuid
from pathlib import Path

from loguru import logger as log

from .config import IngestConfig
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
