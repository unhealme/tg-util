import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as wait_futures
from pathlib import Path
from threading import Event, Lock, Thread

from .types import ABC

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from typing import Concatenate, Self

    from _typeshed import Unused

logger = logging.getLogger(__name__)


class SheetGenerator(ABC):
    _closed: Event
    _finalizing: Event
    _running: Event
    _stopped: Event
    lock: Lock
    pool: ThreadPoolExecutor
    queue: list[Path]
    thread: Thread

    @staticmethod
    def __ensure_open[T, **PT](func: "Callable[Concatenate[SheetGenerator, PT], T]"):
        def wrapper(self: "Self", *args: PT.args, **kwargs: PT.kwargs) -> T:
            if self._closed.is_set():
                raise RuntimeError
            return func(self, *args, **kwargs)

        return wrapper

    def __init__(self):
        self._closed = Event()
        self._finalizing = Event()
        self._running = Event()
        self._stopped = Event()
        self.lock = Lock()
        self.queue = []

    def __enter__(self):
        return self

    def __exit__(self, *_: "Unused"):
        return self.close()

    @__ensure_open
    def submit(self, val: Path):
        logger.debug("submitting %s for sheet generation", val.name)
        with self.lock:
            self.queue.append(val)
        if not self._running.is_set():
            logger.debug("thread is not started, starting thread")
            self.thread = Thread(target=self._run, daemon=True)
            self.thread.start()

    @__ensure_open
    def submits(self, vals: "Iterable[Path]"):
        with self.lock:
            self.queue.extend(vals)
        if not self._running.is_set():
            self.thread = Thread(target=self._run, daemon=True)
            self.thread.start()

    def get(self):
        with self.lock:
            return self.queue.pop(0)

    @__ensure_open
    def close(self):
        self._closed.set()
        logger.debug("queue closed")
        if self._running.is_set():
            logger.debug("waiting thread")
            self.thread.join()
        if not self._stopped.is_set() and self.queue:
            self._finalize()
        self.queue.clear()

    def stop(self):
        self._stopped.set()
        if self._finalizing.is_set():
            return self.pool.shutdown()

    def _finalize(self):
        try:
            logger.debug("finalizing sheet generation")
            self._finalizing.set()
            with ThreadPoolExecutor(4) as self.pool:
                wait_futures(self.pool.submit(generate_sheet, p) for p in self.queue)
        finally:
            self._finalizing.clear()

    def _run(self):
        try:
            self._running.set()
            logger.debug("running flag set")
            logger.debug(
                "is closed: %s, is stopped: %s",
                self._closed.is_set(),
                self._stopped.is_set(),
            )
            while not self._closed.is_set() and not self._stopped.is_set():
                try:
                    generate_sheet(self.get())
                except IndexError:
                    break
        finally:
            self._running.clear()
            logger.debug("running flag cleared")


def generate_sheet(file: Path):
    out_path = file.parent / "sheets"
    out_path.mkdir(parents=True, exist_ok=True)
    logger.debug("%s: generating sheet", file.name)
    p = subprocess.run(
        (
            "vcsi.py",
            "-t",
            *("-o", out_path / (file.name + ".webp")),
            *("-w", "4000"),
            *("-g", "6x6"),
            *("-f", "webp"),
            *("--timestamp-font-size", "20"),
            *("--metadata-font-size", "30"),
            *("--delay-percent", "0"),
            "--",
            file,
        ),
        shell=sys.platform == "win32",
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    if p.returncode == 0:
        logger.info("%s: generating sheet success", file.name)
    else:
        logger.warning("%s: generating sheet failed", file.name)
