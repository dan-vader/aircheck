import asyncio
import itertools
import json
import logging

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

log = logging.getLogger("stream.producer.eventhub_sender")


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