"""psql session for telethon

changes:
- tables must be created manually
- no version table
"""
# pyright: reportIncompatibleMethodOverride=false

from datetime import datetime, timezone

from psycopg import Connection, connect
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
    from collections.abc import Iterable
    from typing import Any

    from psycopg.abc import Params, Query
    from psycopg.rows import TupleRow

CURRENT_VERSION = 7


class PSQLSession(MemorySession):
    _conn: Connection["TupleRow"]

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
        self._conn = connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=schema,
            autocommit=True,
        )

        # These values will be saved
        result = self._fetchone("select * from sessions")
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
        return "<%s: %s>" % (self.__class__.__name__, self._conn.info.dbname)

    def clone(self, to_instance=None):
        cloned = super().clone(to_instance)
        cloned.save_entities = self.save_entities
        return cloned

    def set_dc(self, dc_id, server_address, port):
        super().set_dc(dc_id, server_address, port)
        self._update_session_table()

        # Fetch the auth_key corresponding to this data center
        row = self._fetchone("select auth_key from sessions")
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
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute("truncate sessions")
            cur.execute(
                "insert into sessions values(%s, %s, %s, %s, %s)",
                (
                    self._dc_id,
                    self._server_address,
                    self._port,
                    self._auth_key.key if self._auth_key else b"",
                    self._takeout_id,
                ),
            )

    def get_update_state(self, entity_id: int):
        row = self._fetchone(
            'select pts, qts, "date", seq from update_state where id = %s',
            (entity_id,),
        )
        if row:
            pts, qts, date, seq = row
            date = datetime.fromtimestamp(date, tz=timezone.utc)
            return State(pts, qts, date, seq, unread_count=0)

    def set_update_state(self, entity_id: int, state: State):
        assert state.date
        self._executeonly(
            "insert into update_state values (%(0)s, %(1)s, %(2)s, %(3)s, %(4)s) "
            "on conflict (id) do update set "
            'pts = %(1)s, qts = %(2)s, "date" = %(3)s, seq = %(4)s',
            {
                "0": entity_id,
                "1": state.pts,
                "2": state.qts,
                "3": state.date.timestamp(),
                "4": state.seq,
            },
        )

    def get_update_states(self):
        with self._conn.cursor() as cur:
            cur.execute("select * from update_state")
            for row in cur:
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
        self._conn.close()

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
        self._executemany(
            "insert into entities values (%(0)s, %(1)s, %(2)s, %(3)s, %(4)s, %(5)s) "
            "on conflict (id) do update set hash = %(1)s, username = %(2)s, "
            'phone = %(3)s, name = %(4)s, "date" = %(5)s',
            (
                {
                    "0": row[0],
                    "1": row[1],
                    "2": row[2],
                    "3": row[3],
                    "4": row[4],
                    "5": now,
                }
                for row in rows
            ),
        )

    def get_entity_rows_by_phone(self, phone: int):
        return self._fetchone(
            "select id, hash from entities where phone = %s",
            (phone,),
        )

    def get_entity_rows_by_username(self, username: str):
        with self._conn.cursor() as cur:
            rows = cur.execute(
                'select id, hash, "date" from entities where username = %s',
                (username),
            ).fetchall()

            if not rows:
                return None

            # If there is more than one result for the same username, evict the oldest one
            if len(rows) > 1:
                rows.sort(key=lambda t: t[2] or 0)
                cur.executemany(
                    "update entities set username = null where id = %s",
                    ((r[0],) for r in rows[:-1]),
                )
            row = rows[-1]
            return row[0], row[1]

    def get_entity_rows_by_name(self, name: str):
        return self._fetchone(
            "select id, hash from entities where name = %s",
            (name,),
        )

    def get_entity_rows_by_id(self, id: int, exact: bool = True):
        q: tuple[Any, ...] = ()
        if exact:
            q = ("select id, hash from entities where id = %s", (id,))
        else:
            q = (
                "select id, hash from entities where id in (%s, %s, %s)",
                (
                    get_peer_id(PeerUser(id)),
                    get_peer_id(PeerChat(id)),
                    get_peer_id(PeerChannel(id)),
                ),
            )
        return self._fetchone(*q)

    def get_file(self, md5_digest: bytes, file_size: int, cls: "Any"):
        if row := self._fetchone(
            "select id, hash from sent_files where "
            "md5_digest = %s and file_size = %s and type = %s",
            (
                md5_digest,
                file_size,
                _SentFileType.from_type(cls).value,
            ),
        ):
            # Both allowed classes have (id, access_hash) as parameters
            return cls(row[0], row[1])

    def cache_file(self, md5_digest: bytes, file_size: int, instance: "Any"):
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache %s instance" % type(instance))
        self._executeonly(
            "insert into sent_files values (%(0)s, %(1)s, %(2)s, %(3)s, %(4)s) "
            "on conflict (md5_digest, file_size, type) do update set "
            "id = %(3)s, hash = %(4)s",
            {
                "0": md5_digest,
                "1": file_size,
                "2": _SentFileType.from_type(type(instance)).value,
                "3": instance.id,
                "4": instance.access_hash,
            },
        )

    def _fetchone(self, query: "Query", params: "Params | None" = None):
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def _executeonly(self, query: "Query", params: "Params | None" = None):
        with self._conn.cursor() as cur:
            cur.execute(query, params)

    def _executemany(self, query: "Query", params: "Iterable[Params]"):
        with self._conn.cursor() as cur:
            cur.executemany(query, params)
