from asyncio import Lock
from datetime import datetime

from asyncpg import Connection, Record, UniqueViolationError, connect

from .base import ArchiveBase

TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib.parse import ParseResult

    from _typeshed import Unused

    from tg_util.src.tg.messages.export import MessageExport


class PSQLArchive(ArchiveBase):
    _conn: "Connection[Record]"

    def __init__(self, params: "ParseResult"):
        self._params = params
        self._lock = Lock()

    async def __aenter__(self):
        self._conn = await connect(self._params.geturl())
        return self

    async def __aexit__(self, *_exc: "Unused"):
        await self._conn.close()

    async def prepare(self):
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
        await self._conn.execute(q)

    async def check_attr(
        self,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
    ):
        async with self._lock:
            r = await self._conn.fetchrow(
                "select msg, hash, downloaded from _archive_ where "
                "downloaded is not null and (hash = $1 or "
                "(width = $2 and height = $3 and size = $4 and duration = $5))",
                hash,
                width,
                height,
                size,
                duration,
            )
        if r:
            return r[0], r[1], r[2]

    async def check_id(self, file_id: int):
        async with self._lock:
            return await self._conn.fetchval(
                "select msg from _archive_ where file_id = $1 and downloaded is not null",
                file_id,
            )

    async def set_complete(self, file_id: int):
        async with self._lock:
            await self._conn.execute(
                "update _archive_ set downloaded = $1 where file_id = $2",
                datetime.now(),
                file_id,
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
            "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
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
        )
        async with self._lock:
            try:
                await self._conn.execute(*insert)
            except UniqueViolationError:
                async with self._conn.transaction():
                    await self._conn.execute(
                        "delete from _archive_ where file_id = $1 or hash = $2",
                        file_id,
                        hash,
                    )
                    await self._conn.execute(*insert)

    async def export(self, message: "MessageExport"):
        insert = (
            "insert into _all_chats_ values ($1, $2, $3, $4, $5, $6, $7, "
            "$8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, "
            "$20, $21, $22, $23, $24, $25, $26, $27, $28)",
            *message.as_tuple(),
        )
        try:
            await self._conn.execute(*insert)
        except UniqueViolationError:
            async with self._conn.transaction():
                await self._conn.execute(
                    "delete from _all_chats_ where chat_id = $1 and message_id = $2",
                    message.chat_id,
                    message.message_id,
                )
                await self._conn.execute(*insert)
