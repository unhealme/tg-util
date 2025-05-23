from asyncio import Lock
from contextlib import asynccontextmanager
from pathlib import Path

from .types import ABC
from .utils import wrap_async


class InputFile(ABC):
    _fpath: Path
    _write_lock: Lock
    content: dict[int, tuple[Lock, str]]

    def __init__(self, fp: str):
        self._fpath = Path(fp)
        self.content = {}
        with open(self._fpath, "r", encoding="utf-8") as f:
            self.content = {n: (Lock(), line.strip()) for n, line in enumerate(f, 1)}
        self._write_lock = Lock()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._fpath})"

    def __len__(self) -> int:
        return self.content.__len__()

    async def __aiter__(self):
        for x in sorted(self.content):
            yield x, await self.get(x)

    @asynccontextmanager
    async def ensure_write(self):
        try:
            yield self
        finally:
            await self.write()

    async def set(self, k: int, v: str):
        try:
            lock = self.content[k][0]
        except KeyError:
            lock = Lock()
        async with lock:
            self.content[k] = (lock, v)

    async def get(self, k: int):
        async with self.content[k][0]:
            return self.content[k][1]

    async def set_status(self, at: int, fmt: str):
        await self.set(at, fmt % await self.get(at))

    async def write(self):
        async with self._write_lock:
            await wrap_async(
                self._fpath.write_text,
                "".join(["%s\n" % await self.get(n) for n in sorted(self.content)]),
                encoding="utf-8",
            )
