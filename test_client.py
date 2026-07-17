import asyncio
import typer
import httpx
import time
from typing import Annotated

app = typer.Typer()

async def send_single(client: httpx.AsyncClient, i: int, base_url: str):
    payload = {
        "measurement": "advect-ingest-test",
        "tags": {"id": f"test_pico_{i % 5 + 1}", "location": "single-endpoint"},
        "fields": {"value": 20.0 + (i % 15), "unit": "C", "battery": 98 - (i % 20)}
    }
    r = await client.post(f"{base_url}/ingest", json=payload)
    return r.status_code == 202

async def send_batch(client: httpx.AsyncClient, batch_size: int, batch_id: int, base_url: str):
    batch = []
    for j in range(batch_size):
        i = batch_id * batch_size + j
        batch.append({
            "time": time.time(),
            "measurement": "advect-ingest-test",
            "tags": {"id": f"test_pico_{i % 5 + 1}", "location": "batch-endpoint"},
            "fields": {"value": 20.0 + (i % 15), "unit": "C","battery": 98 - (i % 20)}
        })
    r = await client.post(f"{base_url}/ingest/batch", json=batch)
    return r.status_code == 202

@app.command()
def run(
    points: Annotated[int, typer.Option(help="Total points to send")] = 100,
    batch: Annotated[int, typer.Option(help="Batch size (1 for singles)")] = 10,
    url: Annotated[str, typer.Option(help="Base URL")] = "http://localhost:8000",
):
    """Run ingest load test.
    uv run python test_client.py --help
    uv run python test_client.py --points 500 --batch 25 --url http://localhost:8000
    """
    async def _run():
        async with httpx.AsyncClient() as client:
            success = 0
            if batch > 1:
                num_batches = (points + batch - 1) // batch
                for b in range(num_batches):
                    ok = await send_batch(client, batch, b, url)
                    if ok:
                        success += batch
                    print(f"Batch {b+1}/{num_batches} → {'OK' if ok else 'FAIL'}")
            else:
                for i in range(points):
                    ok = await send_single(client, i, url)
                    if ok:
                        success += 1
                    if (i + 1) % 50 == 0 or i == points - 1:
                        print(f"Sent {i+1}/{points} points")
        print(f"\nTest complete: {success}/{points} points accepted")

    print(f"Testing {points} points with batch size {batch} against {url}")
    asyncio.run(_run())

if __name__ == "__main__":
    app()
