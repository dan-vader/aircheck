import json
import logging

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

log = logging.getLogger("stream.producer.eventhub_sender")


async def send_events(producer: EventHubProducerClient, events: list[dict]) -> int:
    if not events:
        return 0

    sent = 0
    batch = await producer.create_batch()

    for event in events:
        payload = json.dumps(event)
        try:
            batch.add(EventData(payload))
        except ValueError:
            await producer.send_batch(batch)
            sent += len(batch)
            batch = await producer.create_batch()
            batch.add(EventData(payload))

    if len(batch) > 0:
        await producer.send_batch(batch)
        sent += len(batch)

    return sent

