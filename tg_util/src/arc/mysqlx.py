from asyncio import Lock
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from mysqlx import Row, RowResult, Session, Table, get_session
from mysqlx.errors import OperationalError

from tg_util.src.utils import wrap_async

from .base import ArchiveBase

if TYPE_CHECKING:
    from urllib.parse import ParseResult

    from tg_util.src.tg.messages.export import MessageExport


class MySQLXArchive(ArchiveBase):
    _session: Session
    _table: Table

    def __init__(self, params: "ParseResult"):
        self._params = params
        self._lock = Lock()

    async def __aenter__(self):
        schema_name = self._params.path.strip("/")
        session = await wrap_async(
            get_session,
            user=self._params.username,
            password=self._params.password,
            host=self._params.hostname,
            port=self._params.port,
            schema=schema_name,
            use_pure=True,
        )
        self._session = await wrap_async(session.__enter__)
        schema = await wrap_async(session.get_schema, schema_name)
        self._table = await wrap_async(schema.get_table, "_archive_")
        return self

    async def __aexit__(self, *_exc: Any):
        await wrap_async(self._session.commit)
        return await wrap_async(self._session.__exit__, *_exc)

    async def prepare(self):
        return

    async def check_attr(
        self,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
    ):
        select = (
            self._table.select("msg", "hash", "downloaded")
            .where(
                "downloaded is not null and ("
                "hash = :hash or (width = :width and "
                "height = :height and size = :size and "
                "duration = :duration))"
            )
            .bind("hash", hash)
            .bind("width", width)
            .bind("height", height)
            .bind("size", size)
            .bind("duration", duration)
        )
        async with self._lock:
            result = cast("RowResult", await wrap_async(select.execute))
            if row := cast("Row | None", await wrap_async(result.fetch_one)):
                return row[0], row[1], row[2]

    async def check_id(self, file_id: int):
        async with self._lock:
            result = cast(
                "RowResult",
                await wrap_async(
                    self._table.select("msg")
                    .where("file_id = :fid and downloaded is not null")
                    .bind("fid", file_id)
                    .execute,
                ),
            )
            if row := cast("Row | None", await wrap_async(result.fetch_one)):
                return row[0]

    async def set_complete(self, file_id: int):
        async with self._lock:
            await wrap_async(
                self._table.update()
                .set("downloaded", datetime.now().isoformat(timespec="seconds"))
                .where("file_id = :fid")
                .bind("fid", file_id)
                .execute,
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
        insert = self._table.insert(
            "file_id",
            "msg",
            "msg_id",
            "chat_id",
            "chat_username",
            "hash",
            "width",
            "height",
            "size",
            "duration",
            "type",
        ).values(
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
                await wrap_async(insert.execute)
            except OperationalError:
                await wrap_async(
                    self._table.delete()
                    .where("file_id = :fid or hash = :hash")
                    .bind("fid", file_id)
                    .bind("hash", hash)
                    .execute,
                )
                await wrap_async(insert.execute)

    async def export(self, message: "MessageExport"):
        raise NotImplementedError
