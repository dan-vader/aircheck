import argparse
import asyncio
import logging
import sys

import httpx
from azure.eventhub.aio import EventHubProducerClient

from producer._01_config import load_config
from producer._05_eventhub_sender import send_events
from producer._02_secrets import get_secrets
from producer._03_sensor_api import fetch_filtered
from producer._06_shutdown import install_signal_handlers
from producer._04_transform import pivot_record

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("stream.producer")


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
        "poll cycle done: sensors_in_response=%d events_sent=%d filter=type=%s&country=%s",
        len(records), sent, sensor_type_filter, country_filter,
    )


async def run(
    env: str, config_path: str, sensor_filter_override: str | None, shutdown_event: asyncio.Event
) -> None:
    cfg = load_config(env, config_path)
    secrets = await get_secrets(cfg)
    streaming_cfg = cfg["streaming"]

    sensor_type_filter = sensor_filter_override or streaming_cfg["sensor_type_filter"]
    country_filter = streaming_cfg["country_filter"]

    headers = {"User-Agent": f"aircheck-producer (contact: {secrets['contact_email']})"}

    log.info(
        "starting producer env=%s eventhub=%s filter=type=%s&country=%s streaming_cfg=%s",
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


async def main(env: str, config_path: str, sensor_filter_override: str | None) -> None:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, shutdown_event)
    await run(env, config_path, sensor_filter_override, shutdown_event)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument(
        "--config-path",
        default="../../config/aircheck.yaml",
        help="Path to aircheck.yaml, relative to the current working directory by default",
    )
    parser.add_argument(
        "--sensor-filter",
        default=None,
        help='Overrides streaming.sensor_type_filter from config, e.g. "SDS011,BME280" for Phase B',
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.env, args.config_path, args.sensor_filter))
    except KeyboardInterrupt:
        log.info("producer stopped by user (KeyboardInterrupt)")