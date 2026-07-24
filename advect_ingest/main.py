from contextlib import asynccontextmanager
from sys import stderr

from daq_tools import DAQIngestor
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import RedirectResponse
from loguru import logger as log

from .config import RootConfig
from .models import DataPoint
from .utils import IngestQueue

# Load config
config = RootConfig.load()
ingest_queue = IngestQueue(config.ingest)

# Setup logging
log.remove()
log.add(stderr, level=config.app.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ingest_queue.start()
    async with DAQIngestor.from_config_file(config_path=config.ingest.config_path):
        yield
    await ingest_queue.stop()


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
