from datetime import datetime

from asyncpg import Pool, Record, create_pool

from .base import ArchiveBase

TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib.parse import ParseResult

    from _typeshed import Unused


class PSQLArchive(ArchiveBase):
    _pool: "Pool[Record]"

    def __init__(self, params: "ParseResult"):
        self._params = params
        self._pool = create_pool(self._params.geturl())

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
        r = await self._pool.fetchrow(
            "select msg, hash, downloaded from _archive_ where "
            "hash = $1 or "
            "(width = $2 and height = $3 and size = $4 and duration = $5)",
            hash.hex(),
            width,
            height,
            size,
            duration,
        )
        if r:
            return r[0], r[1], r[2]

    async def check_id(self, file_id: int):
        return await self._pool.fetchval(
            "select msg from _archive_ where file_id = $1 and downloaded is not null",
            file_id,
        )

    async def set_complete(self, file_id: int):
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
        async with self._pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "delete from _archive_ where file_id = $1 or hash = $2",
                    file_id,
                    hash.hex(),
                )
                await con.execute(
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
