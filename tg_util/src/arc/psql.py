from asyncio import Lock
from datetime import datetime

from asyncpg import Pool, Record, UniqueViolationError, create_pool

from .base import ArchiveBase

TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib.parse import ParseResult

    from _typeshed import Unused

    from tg_util.src.tg.messages.export import MessageExport


class PSQLArchive(ArchiveBase):
    _pool: "Pool[Record]"

    def __init__(self, params: "ParseResult"):
        self._params = params
        self._pool = create_pool(self._params.geturl())
        self._lock = Lock()

    async def __aenter__(self):
        self._pool = await self._pool.__aenter__()
        return self

    async def __aexit__(self, *_exc: "Unused"):
        await self._pool.__aexit__(*_exc)

    async def prepare(self):
        q = (
            """CREATE TABLE IF NOT EXISTS "_archive_" (
            file_id bigint NOT NULL,
            msg text NOT NULL,
            msg_id int NOT NULL,
            chat_id bigint NOT NULL,
            username text NULL,
            hash char(128) NOT NULL,
            width int NULL,
            height int NULL,
            "size" bigint NULL,
            duration double precision NULL,
            downloaded timestamp NULL,
            "type" varchar(20) NOT NULL,
            CONSTRAINT "_archive__pk" PRIMARY KEY (file_id),
            CONSTRAINT hash_unique UNIQUE (hash)
            );"""
            'CREATE INDEX IF NOT EXISTS "_archive__type_IDX" ON "_archive_" ("type");'
        )
        await self._pool.execute(q)

    async def check_attr(
        self,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
    ):
        async with self._lock:
            r = await self._pool.fetchrow(
                "select msg, hash, downloaded from _archive_ where "
                "downloaded is not null and (hash = $1 or "
                "(width = $2 and height = $3 and size = $4 and duration = $5))",
                hash.hex(),
                width,
                height,
                size,
                duration,
            )
        if r:
            return r[0], r[1], r[2]

    async def check_id(self, file_id: int):
        async with self._lock:
            return await self._pool.fetchval(
                "select msg from _archive_ where file_id = $1 and downloaded is not null",
                file_id,
            )

    async def set_complete(self, file_id: int):
        async with self._lock:
            await self._pool.execute(
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
            hash.hex(),
            width,
            height,
            size,
            duration,
            type,
        )
        async with self._lock, self._pool.acquire() as con:
            try:
                await con.execute(*insert)
            except UniqueViolationError:
                async with con.transaction():
                    await con.execute(
                        "delete from _archive_ where file_id = $1 or hash = $2",
                        file_id,
                        hash.hex(),
                    )
                    await con.execute(*insert)

    async def export(self, message: "MessageExport"):
        insert = (
            "insert into _all_chats_ values ($1, $2, $3, $4, $5, $6, $7, "
            "$8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, "
            "$20, $21, $22, $23, $24, $25, $26, $27, $28)",
            *message.as_tuple(),
        )
        async with self._pool.acquire() as con:
            try:
                await con.execute(*insert)
            except UniqueViolationError:
                async with con.transaction():
                    await con.execute(
                        "delete from _all_chats_ where chat_id = $1 and message_id = $2",
                        message.chat_id,
                        message.message_id,
                    )
                    await con.execute(*insert)
