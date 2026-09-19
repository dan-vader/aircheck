import argparse
import asyncio
import itertools
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any

import httpx
import yaml
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_random_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("stream.producer")

API_FILTER_BASE = "https://data.sensor.community/airrohr/v1/filter"

RETRYABLE_TRANSPORT_ERRORS = (httpx.TimeoutException, httpx.ConnectError)
MAX_BACKOFF_SEC = 60


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, RETRYABLE_TRANSPORT_ERRORS):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


def load_config(env: str) -> dict[str, Any]:
    config_path = Path(__file__).resolve().parents[2] / "config" / "aircheck.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        full_cfg = yaml.safe_load(f)
    if env not in full_cfg:
        raise KeyError(f"Environment '{env}' not found in {config_path}")
    return full_cfg[env]


async def get_secrets(cfg: dict[str, Any]) -> dict[str, str]:
    vault_name = cfg["key_vault_name"]
    vault_url = f"https://{vault_name}.vault.azure.net/"

    secret_keys = cfg["secrets"]

    async with DefaultAzureCredential() as credential, \
            SecretClient(vault_url=vault_url, credential=credential) as client:
        contact_email = (await client.get_secret(secret_keys["contact_email"])).value
        eventhub_conn_str = (await client.get_secret(secret_keys["eventhub_conn_str"])).value

    return {"contact_email": contact_email, "eventhub_conn_str": eventhub_conn_str}


def build_filter_url(sensor_type_filter: str, country_filter: str) -> str:
    return f"{API_FILTER_BASE}/type={sensor_type_filter}/country={country_filter}"


async def fetch_filtered(
    client: httpx.AsyncClient,
    headers: dict,
    streaming_cfg: dict,
    sensor_type_filter: str,
    country_filter: str,
) -> list[dict]:
    max_retries = streaming_cfg["max_retries"]
    backoff_base = streaming_cfg["retry_backoff_base"]
    timeout = streaming_cfg["request_timeout_sec"]

    url = build_filter_url(sensor_type_filter, country_filter)

    retrying = AsyncRetrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_random_exponential(multiplier=backoff_base, max=MAX_BACKOFF_SEC),
        retry=retry_if_exception(is_retryable_error),
        reraise=True,
    )

    try:
        async for attempt in retrying:
            with attempt:
                resp = await client.get(url, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if is_retryable_error(e):
            log.error(
                "filter request giving up after %d attempts, last status %s: %s",
                max_retries, status, e,
            )
        else:
            log.error("filter request failed with non-retryable status %s: %s", status, e)
        return []
    except RETRYABLE_TRANSPORT_ERRORS as e:
        log.error("filter request giving up after %d attempts: %s", max_retries, e)
        return []


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


async def send_partition_group(
    producer: EventHubProducerClient, partition_key: str, events: list[dict]
) -> int:
    sent = 0
    batch = await producer.create_batch(partition_key=partition_key)

    for event in events:
        payload = json.dumps(event)
        try:
            batch.add(EventData(payload))
        except ValueError:
            await producer.send_batch(batch)
            sent += len(batch)
            batch = await producer.create_batch(partition_key=partition_key)
            batch.add(EventData(payload))

    if len(batch) > 0:
        await producer.send_batch(batch)
        sent += len(batch)

    return sent


async def send_events(producer: EventHubProducerClient, events: list[dict]) -> int:
    if not events:
        return 0

    events_sorted = sorted(events, key=lambda e: str(e.get("sensor_id")))
    groups = itertools.groupby(events_sorted, key=lambda e: str(e.get("sensor_id")))

    tasks = [
        asyncio.create_task(send_partition_group(producer, partition_key, list(group_events)))
        for partition_key, group_events in groups
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    sent = 0
    for r in results:
        if isinstance(r, Exception):
            log.error("failed to send a partition batch: %s", r)
        else:
            sent += r

    return sent


async def poll_cycle(
    http_client: httpx.AsyncClient,
    producer: EventHubProducerClient,
    headers: dict,
    streaming_cfg: dict,
    sensor_type_filter: str,
    country_filter: str,
) -> None:
    records = await fetch_filtered(
        http_client, headers, streaming_cfg, sensor_type_filter, country_filter
    )
    events = [pivot_record(rec) for rec in records]

    sent = await send_events(producer, events)
    log.info(
        "poll cycle done: sensors_in_response=%d events_sent=%d filter=type=%s/country=%s",
        len(records), sent, sensor_type_filter, country_filter,
    )


async def run(env: str, sensor_filter_override: str | None, shutdown_event: asyncio.Event) -> None:
    cfg = load_config(env)
    secrets = await get_secrets(cfg)
    streaming_cfg = cfg["streaming"]

    sensor_type_filter = sensor_filter_override or streaming_cfg["sensor_type_filter"]
    country_filter = streaming_cfg["country_filter"]

    headers = {"User-Agent": f"aircheck-producer (contact: {secrets['contact_email']})"}

    log.info(
        "starting producer env=%s eventhub=%s filter=type=%s/country=%s streaming_cfg=%s",
        env, cfg["eventhub_name"], sensor_type_filter, country_filter, streaming_cfg,
    )

    producer = EventHubProducerClient.from_connection_string(
        conn_str=secrets["eventhub_conn_str"],
        eventhub_name=cfg["eventhub_name"],
    )

    async with producer, httpx.AsyncClient() as http_client:
        while not shutdown_event.is_set():
            try:
                await poll_cycle(
                    http_client, producer, headers, streaming_cfg,
                    sensor_type_filter, country_filter,
                )
            except Exception as e:
                log.error("poll cycle raised an unexpected error: %s", e)

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=streaming_cfg["poll_interval_sec"]
                )
            except asyncio.TimeoutError:
                pass

    log.info("shutdown signal received, producer stopped cleanly")


def install_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event) -> None:
    def _handle_signal(sig_name: str) -> None:
        log.info("received %s, shutting down after current poll cycle", sig_name)
        shutdown_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _handle_signal, sig_name)
        except NotImplementedError:
            pass


async def main(env: str, sensor_filter_override: str | None) -> None:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, shutdown_event)
    await run(env, sensor_filter_override, shutdown_event)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument(
        "--sensor-filter",
        default=None,
        help='Overrides streaming.sensor_type_filter from config, e.g. "SDS011,BME280" for Phase B',
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.env, args.sensor_filter))
    except KeyboardInterrupt:
        log.info("producer stopped by user (KeyboardInterrupt)")