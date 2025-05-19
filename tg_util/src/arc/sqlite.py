from asyncio import Lock
from contextlib import asynccontextmanager
from datetime import datetime
from sqlite3 import Connection, connect

from tg_util.src.utils import wrap_async

from .base import ArchiveBase

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any
    from urllib.parse import ParseResult

    from tg_util.src.tg.messages.export import MessageExport


class SQLiteArchive(ArchiveBase):
    _conn: Connection

    def __init__(self, params: "ParseResult"):
        self._params = params
        self._lock = Lock()

    async def __aenter__(self):
        db = self._params.path
        conn = await wrap_async(
            connect,
            db,
            check_same_thread=False,
            autocommit=True,
        )
        self._conn = await wrap_async(conn.__enter__)
        if db != ":memory:":
            async with self.get_cursor() as cursor:
                await wrap_async(cursor.execute, "PRAGMA optimize")
        return self

    async def __aexit__(self, *_exc: "Any"):
        await wrap_async(self._conn.commit)
        await wrap_async(self._conn.__exit__, *_exc)

    @asynccontextmanager
    async def get_cursor(self):
        cursor = await wrap_async(self._conn.cursor)
        try:
            yield cursor
        finally:
            await wrap_async(cursor.close)

    async def prepare(self):
        async with self.get_cursor() as cursor:
            await wrap_async(
                cursor.executescript,
                "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;",
            )
            await wrap_async(
                cursor.execute,
                "CREATE TABLE IF NOT EXISTS _archive_"
                "(file_id INTEGER PRIMARY KEY NOT NULL, msg TEXT NOT NULL, msg_id INTEGER NOT NULL, "
                "chat_id INTEGER NOT NULL, chat_username TEXT, hash BLOB NOT NULL, width INTEGER, "
                "height INTEGER, size INTEGER, duration REAL, downloaded TEXT DEFAULT NULL, "
                "type TEXT NOT NULL) STRICT",
            )

    async def check_attr(
        self,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
    ) -> "tuple[Any, Any, Any] | None":
        async with self._lock, self.get_cursor() as cursor:
            await wrap_async(
                cursor.execute,
                "select msg, hash, downloaded from _archive_ where "
                "downloaded is not null and (hash = ? or "
                "(width = ? and height = ? and size = ? and duration = ?))",
                (hash, width, height, size, duration),
            )
            return await wrap_async(cursor.fetchone)

    async def check_id(self, file_id: int):
        async with self._lock, self.get_cursor() as cursor:
            await wrap_async(
                cursor.execute,
                "select msg from _archive_ where file_id = ? and "
                "downloaded is not null",
                (file_id,),
            )
            if row := await wrap_async(cursor.fetchone):
                return row[0]

    async def set_complete(self, file_id: int):
        async with self._lock, self.get_cursor() as cursor:
            await wrap_async(
                cursor.execute,
                "update _archive_ set downloaded = ? where file_id = ?",
                (datetime.now().isoformat(" ", "seconds"), file_id),
            )

    async def update(
        self,
        file_id: int,
        msg: str,
        msg_id: int,
        chat_id: int,
        chat_username: str | None,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
        type: str,
    ):
        async with self._lock, self.get_cursor() as cursor:
            q = (
                "replace into _archive_ (file_id, msg, msg_id, chat_id, "
                "chat_username, hash, width, height, size, duration, type) "
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            await wrap_async(
                cursor.execute,
                q,
                (
                    file_id,
                    msg,
                    msg_id,
                    chat_id,
                    chat_username,
                    hash,
                    width,
                    height,
                    size,
                    duration,
                    type,
                ),
            )

    async def export(self, message: "MessageExport"):
        raise NotImplementedError
