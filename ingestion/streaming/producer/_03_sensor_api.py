import logging

import httpx
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_random_exponential

log = logging.getLogger("stream.producer.sensor_api")

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


def build_filter_url(sensor_type_filter: str, country_filter: str) -> str:
    return f"{API_FILTER_BASE}/type={sensor_type_filter}&country={country_filter}"


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