import asyncio
import logging
import signal

log = logging.getLogger("stream.producer.shutdown")


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