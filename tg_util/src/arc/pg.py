from asyncio import Lock
from datetime import datetime

from psycopg import AsyncConnection, IntegrityError

from .base import ArchiveBase

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any
    from urllib.parse import ParseResult

    from psycopg.rows import TupleRow

    from tg_util.src.tg.messages.export import MessageExport


class PSQLArchive(ArchiveBase):
    _conn: AsyncConnection["TupleRow"]

    def __init__(self, params: "ParseResult"):
        self._params = params
        self._lock = Lock()

    async def __aenter__(self):
        conn = await AsyncConnection.connect(self._params.geturl(), autocommit=True)
        self._conn = await conn.__aenter__()
        return self

    async def __aexit__(self, *_exc: "Any"):
        await self._conn.__aexit__(*_exc)

    async def prepare(self):
        async with self._conn.cursor() as cur:
            q = (
                """CREATE TABLE IF NOT EXISTS _archive_ (
                    file_id int8 NOT NULL,
                    msg text NOT NULL,
                    msg_id int4 NOT NULL,
                    chat_id int8 NOT NULL,
                    username text NULL,
                    hash bytea NOT NULL,
                    width int4 NULL,
                    height int4 NULL,
                    "size" int8 NULL,
                    duration float8 NULL,
                    downloaded timestamp NULL,
                    "type" varchar(20) NOT NULL,
                    CONSTRAINT _archive__pk PRIMARY KEY (file_id),
                    CONSTRAINT _archive__unique UNIQUE (hash)
                );"""
                'CREATE INDEX IF NOT EXISTS "_archive__type_IDX" ON "_archive_" ("type");'
            )
            await cur.execute(q)

    async def check_attr(
        self,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
    ):
        async with self._lock, self._conn.cursor() as cur:
            await cur.execute(
                "select msg, hash, downloaded from _archive_ where "
                "downloaded is not null and (hash = %s or "
                "(width = %s and height = %s and size = %s and duration = %s))",
                (
                    hash,
                    width,
                    height,
                    size,
                    duration,
                ),
            )
            if r := await cur.fetchone():
                return r[0], r[1], r[2]

    async def check_id(self, file_id: int):
        async with self._lock, self._conn.cursor() as cur:
            await cur.execute(
                "select msg from _archive_ where file_id = %s and downloaded is not null",
                (file_id,),
            )
            if r := await cur.fetchone():
                return r[0]

    async def set_complete(self, file_id: int):
        async with self._lock, self._conn.cursor() as cur:
            await cur.execute(
                "update _archive_ set downloaded = %s where file_id = %s",
                (
                    datetime.now(),
                    file_id,
                ),
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
        insert = (
            "insert into _archive_ (file_id, msg, msg_id, chat_id, "
            "username, hash, width, height, size, duration, type) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
        async with self._lock, self._conn.cursor() as cur:
            try:
                await cur.execute(*insert)
            except IntegrityError:
                async with self._conn.transaction():
                    await cur.execute(
                        "delete from _archive_ where file_id = %s or hash = %s",
                        (
                            file_id,
                            hash,
                        ),
                    )
                    await cur.execute(*insert)

    async def export(self, message: "MessageExport"):
        insert = (
            "insert into _all_chats_ values (%s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            *message.as_tuple(),
        )
        async with self._conn.cursor() as cur:
            try:
                await cur.execute(*insert)
            except IntegrityError:
                async with self._conn.transaction():
                    await cur.execute(
                        "delete from _all_chats_ where chat_id = %s and message_id = %s",
                        (
                            message.chat_id,
                            message.message_id,
                        ),
                    )
                    await cur.execute(*insert)
