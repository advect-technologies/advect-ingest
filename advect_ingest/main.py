import asyncio
from contextlib import asynccontextmanager
from sys import platform, stderr

from daq_tools import DAQIngestor
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import RedirectResponse
from loguru import logger as log

from .config import RootConfig
from .models import DataPoint
from .utils import HAMqttPublisher, IngestQueue

if platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Load config
config = RootConfig.load()
ingest_queue = IngestQueue(config.ingest)
ha_publisher: HAMqttPublisher | None = None

# Setup logging
log.remove()
log.add(stderr, level=config.app.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ingest_queue.start()
    if config.ha.active:
        global ha_publisher
        ha_publisher = HAMqttPublisher(config.ha)
        await ha_publisher.start()
    else:
        log.info("HA MQTT publisher is not active")
    async with DAQIngestor.from_config_file(config_path=config.ingest.config_path):
        yield
    await ingest_queue.stop()
    if ha_publisher:
        await ha_publisher.stop()


app = FastAPI(
    title="Advect Ingest",
    description="Lightweight HTTP ingestion for Advect IoT pipelines",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse("/docs")


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest(point: DataPoint):
    log.debug(f"Ingesting point: {point}")
    await ingest_queue.add(point)
    return {"status": "ingested"}


@app.post("/ingest/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(datapoints: list[DataPoint]):
    if not datapoints:
        raise HTTPException(status_code=400, detail="At least one datapoint required")
    await ingest_queue.add_batch(datapoints)
    return {"status": "accepted", "count": len(datapoints)}


@app.post("/ha/state")
async def ha_state(point: DataPoint):
    """Publish a point to Home Assistant if it has a matching registry entry."""
    if not ha_publisher or not config.ha.active:
        log.warning("HA MQTT publisher is not active")
        return {"status": "ha_disabled"}

    device_id = point.tags.get("id", "")
    await ha_publisher.publish_point(device_id, point.fields)

    return {"status": "ok"}
