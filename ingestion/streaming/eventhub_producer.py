import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import yaml
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("stream.producer")

SENSOR_IDS = [int(s) for s in os.environ.get("SENSOR_IDS", "").split(",") if s.strip()]

API_BASE = "https://data.sensor.community/airrohr/v1/sensor"


def load_config(env: str) -> dict[str, Any]:
    config_path = Path(__file__).resolve().parents[2] / "config" / "aircheck.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        full_cfg = yaml.safe_load(f)
    if env not in full_cfg:
        raise KeyError(f"Environment '{env}' not found in {config_path}")
    return full_cfg[env]


def get_secrets(cfg: dict[str, Any]) -> dict[str, str]:
    vault_name = os.environ.get("KEY_VAULT_NAME")
    if not vault_name:
        raise RuntimeError(
            "KEY_VAULT_NAME environment variable is not set. "
            "Set it to the Key Vault name backing the "
            f"'{cfg.get('secret_scope')}' secret scope (e.g. kvua5816bd)."
        )
    vault_url = f"https://{vault_name}.vault.azure.net/"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)

    secret_keys = cfg["secrets"]
    contact_email = client.get_secret(secret_keys["contact_email"]).value
    eventhub_conn_str = client.get_secret(secret_keys["eventhub_conn_str"]).value

    return {"contact_email": contact_email, "eventhub_conn_str": eventhub_conn_str}


RETRYABLE_ERRORS = (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError)


async def fetch_sensor(
    client: httpx.AsyncClient, sensor_id: int, headers: dict, streaming_cfg: dict
) -> list[dict] | None:
    max_retries = streaming_cfg["max_retries"]
    backoff_base = streaming_cfg["retry_backoff_base"]
    timeout = streaming_cfg["request_timeout_sec"]

    url = f"{API_BASE}/{sensor_id}/"

    retrying = AsyncRetrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=backoff_base, min=1),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        reraise=True,
    )

    try:
        async for attempt in retrying:
            with attempt:
                resp = await client.get(url, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
    except RETRYABLE_ERRORS as e:
        log.error("sensor=%s giving up after %d attempts: %s", sensor_id, max_retries, e)
        return None


def pivot_record(rec: dict) -> dict:
    event = {
        "id": rec.get("id"),
        "timestamp": rec.get("timestamp"),
        "sensor_id": rec.get("sensor", {}).get("id"),
        "sensor_type": rec.get("sensor", {}).get("sensor_type", {}).get("name"),
        "location_id": rec.get("location", {}).get("id"),
        "latitude": rec.get("location", {}).get("latitude"),
        "longitude": rec.get("location", {}).get("longitude"),
        "country": rec.get("location", {}).get("country"),
    }
    for v in rec.get("sensordatavalues", []):
        value_type = v.get("value_type")
        if value_type:
            event[value_type] = v.get("value")
    return event


async def send_events(producer: EventHubProducerClient, events: list[dict]) -> int:
    if not events:
        return 0

    batch = await producer.create_batch()
    sent = 0
    pending_sends = []

    for event in events:
        payload = json.dumps(event)
        try:
            batch.add(EventData(payload))
        except ValueError:
            pending_sends.append(asyncio.create_task(producer.send_batch(batch)))
            batch = await producer.create_batch()
            batch.add(EventData(payload))
        sent += 1

    if len(batch) > 0:
        pending_sends.append(asyncio.create_task(producer.send_batch(batch)))

    if pending_sends:
        results = await asyncio.gather(*pending_sends, return_exceptions=True)
        failed = [r for r in results if isinstance(r, Exception)]
        if failed:
            log.error("failed to send %d batch(es): %s", len(failed), failed)

    return sent


async def poll_cycle(
    http_client: httpx.AsyncClient,
    producer: EventHubProducerClient,
    headers: dict,
    streaming_cfg: dict,
) -> None:
    events = []
    ok, failed = 0, 0

    for sensor_id in SENSOR_IDS:
        records = await fetch_sensor(http_client, sensor_id, headers, streaming_cfg)
        if records is None:
            failed += 1
            continue
        ok += 1
        for rec in records:
            events.append(pivot_record(rec))

    sent = await send_events(producer, events)
    log.info(
        "poll cycle done: sensors_ok=%d sensors_failed=%d events_sent=%d",
        ok, failed, sent,
    )


async def run(env: str) -> None:
    if not SENSOR_IDS:
        raise RuntimeError(
            "SENSOR_IDS environment variable is not set (comma-separated sensor ids)."
        )

    cfg = load_config(env)
    secrets = get_secrets(cfg)
    streaming_cfg = cfg["streaming"]

    headers = {"User-Agent": f"aircheck-producer (contact: {secrets['contact_email']})"}

    log.info(
        "starting producer env=%s eventhub=%s sensors=%s streaming_cfg=%s",
        env, cfg["eventhub_name"], SENSOR_IDS, streaming_cfg,
    )

    producer = EventHubProducerClient.from_connection_string(
        conn_str=secrets["eventhub_conn_str"],
        eventhub_name=cfg["eventhub_name"],
    )

    async with producer, httpx.AsyncClient() as http_client:
        while True:
            try:
                await poll_cycle(http_client, producer, headers, streaming_cfg)
            except Exception as e:
                log.error("poll cycle raised an unexpected error: %s", e)
            await asyncio.sleep(streaming_cfg["poll_interval_sec"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.env))
    except KeyboardInterrupt:
        log.info("producer stopped by user")