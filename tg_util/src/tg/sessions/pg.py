"""psql session for telethon

changes:
- tables are not auto create
- no version table
"""
# pyright: reportIncompatibleMethodOverride=false

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Thread

from asyncpg import Connection, Record, connect
from telethon.crypto import AuthKey
from telethon.sessions.memory import MemorySession, _SentFileType
from telethon.sessions.sqlite import CURRENT_VERSION as UPSTREAM_VERSION
from telethon.tl.types import (
    InputDocument,
    InputPhoto,
    PeerChannel,
    PeerChat,
    PeerUser,
    TLObject,
)
from telethon.tl.types.updates import State
from telethon.utils import get_peer_id

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any, Coroutine

CURRENT_VERSION = 7


class PSQLSession(MemorySession):
    _conn: "Connection[Record]"
    _loop: asyncio.AbstractEventLoop
    _schema: str
    _thread: Thread

    def __init__(
        self,
        user: str,
        password: str,
        host: str = "127.0.0.1",
        port: int = 5432,
        schema: str = "telethon",
    ):
        if CURRENT_VERSION < UPSTREAM_VERSION:
            err = (
                f"schema version {CURRENT_VERSION} is lower "
                f"than upstream version {UPSTREAM_VERSION}"
            )
            raise RuntimeError(err)
        super().__init__()
        self.save_entities = True
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(target=self._start_loop)
        self._thread.start()
        self._schema = schema
        self._conn = self._wrap_sync(
            connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=schema,
            )
        )

        # These values will be saved
        result = self._wrap_sync(self._conn.fetchrow("select * from sessions"))
        if result:
            (
                self._dc_id,
                self._server_address,
                self._port,
                key,
                self._takeout_id,
            ) = result
            self._auth_key = AuthKey(data=key)

    def __repr__(self) -> str:
        return "<%s: %s>" % (self.__class__.__name__, self._schema)

    def clone(self, to_instance=None):
        cloned = super().clone(to_instance)
        cloned.save_entities = self.save_entities
        return cloned

    def set_dc(self, dc_id, server_address, port):
        super().set_dc(dc_id, server_address, port)
        self._update_session_table()

        # Fetch the auth_key corresponding to this data center
        row = self._wrap_sync(self._conn.fetchrow("select auth_key from sessions"))
        if row and row[0]:
            self._auth_key = AuthKey(data=row[0])
        else:
            self._auth_key = None

    @MemorySession.auth_key.setter
    def auth_key(self, value):
        self._auth_key = value
        self._update_session_table()

    @MemorySession.takeout_id.setter
    def takeout_id(self, value):
        self._takeout_id = value
        self._update_session_table()

    def _update_session_table(self):
        with self._transactions():
            self._wrap_sync(self._conn.execute("truncate sessions"))
            self._wrap_sync(
                self._conn.execute(
                    "insert into sessions values($1, $2, $3, $4, $5)",
                    self._dc_id,
                    self._server_address,
                    self._port,
                    self._auth_key.key if self._auth_key else b"",
                    self._takeout_id,
                )
            )

    def get_update_state(self, entity_id: int):
        row = self._wrap_sync(
            self._conn.fetchrow(
                'select pts, qts, "date", seq from update_state where id = $1',
                entity_id,
            )
        )
        if row:
            pts, qts, date, seq = row
            date = datetime.fromtimestamp(date, tz=timezone.utc)
            return State(pts, qts, date, seq, unread_count=0)

    def set_update_state(self, entity_id: int, state: State):
        assert state.date
        self._wrap_sync(
            self._conn.execute(
                "insert into update_state values ($1, $2, $3, $4, $5) "
                "on conflict (id) do update set pts = $2, qts = $3, "
                '"date" = $4, seq = $5',
                entity_id,
                state.pts,
                state.qts,
                state.date.timestamp(),
                state.seq,
            )
        )

    def get_update_states(self):
        rows = self._wrap_sync(self._conn.fetch("select * from update_state"))
        for row in rows:
            yield (
                row[0],
                State(
                    pts=row[1],
                    qts=row[2],
                    date=datetime.fromtimestamp(row[3], tz=timezone.utc),
                    seq=row[4],
                    unread_count=0,
                ),
            )

    def close(self):
        """Closes the connection unless we're working in-memory"""
        self._wrap_sync(self._conn.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def process_entities(self, tlo: TLObject):
        """
        Processes all the found entities on the given TLObject,
        unless .save_entities is False.
        """
        if not self.save_entities:
            return

        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        now = int(datetime.now().timestamp())
        self._wrap_sync(
            self._conn.executemany(
                "insert into entities values ($1, $2, $3, $4, $5, $6) "
                "on conflict (id) do update set hash = $2, username = $3, "
                'phone = $4, name = $5, "date" = $6',
                ((*row, now) for row in rows),
            )
        )

    def get_entity_rows_by_phone(self, phone: int):  # type: ignore
        return self._wrap_sync(
            self._conn.fetchrow(
                "select id, hash from entities where phone = $1",
                phone,
            )
        )

    def get_entity_rows_by_username(self, username: str):
        rows = self._wrap_sync(
            self._conn.fetch(
                'select id, hash, "date" from entities where username = $1',
                username,
            )
        )

        if not rows:
            return None

        # If there is more than one result for the same username, evict the oldest one
        if len(rows) > 1:
            rows.sort(key=lambda t: t[2] or 0)
            self._wrap_sync(
                self._conn.executemany(
                    "update entities set username = null where id = $1",
                    ((r[0],) for r in rows[:-1]),
                )
            )
        row = rows[-1]
        return row[0], row[1]

    def get_entity_rows_by_name(self, name: str):  # type: ignore
        return self._wrap_sync(
            self._conn.fetchrow("select id, hash from entities where name = $1", name)
        )

    def get_entity_rows_by_id(self, id: int, exact: bool = True):  # type: ignore
        q: tuple[Any, ...] = ()
        if exact:
            q = ("select id, hash from entities where id = $1", id)
        else:
            q = (
                "select id, hash from entities where id in ($1, $2, $3)",
                get_peer_id(PeerUser(id)),
                get_peer_id(PeerChat(id)),
                get_peer_id(PeerChannel(id)),
            )
        return self._wrap_sync(self._conn.fetchrow(*q))

    def get_file(self, md5_digest: bytes, file_size: int, cls: "Any"):
        if row := self._wrap_sync(
            self._conn.fetchrow(
                "select id, hash from sent_files where "
                "md5_digest = $1 and file_size = $2 and type = $3",
                md5_digest,
                file_size,
                _SentFileType.from_type(cls).value,
            )
        ):
            # Both allowed classes have (id, access_hash) as parameters
            return cls(row[0], row[1])

    def cache_file(self, md5_digest: bytes, file_size: int, instance: "Any"):
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache %s instance" % type(instance))
        self._wrap_sync(
            self._conn.execute(
                "insert into sent_files values ($1, $2, $3, $4, $5) "
                "on conflict (md5_digest, file_size, type) do update set "
                "id = $4, hash = $5",
                md5_digest,
                file_size,
                _SentFileType.from_type(type(instance)).value,
                instance.id,
                instance.access_hash,
            )
        )

    def _wrap_sync[T](self, coro: "Coroutine[Any, Any, T]"):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    @contextmanager
    def _transactions(self):
        t = self._conn.transaction()
        try:
            self._wrap_sync(t.start())
            yield
            self._wrap_sync(t.commit())
        except BaseException:
            self._wrap_sync(t.rollback())
            raise

    def _start_loop(self):
        self._loop.run_forever()
        self._loop.close()
