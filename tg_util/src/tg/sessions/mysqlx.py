"""mysql session for telethon

changes:
- tables must be created manually
- no version table
"""

import warnings
from datetime import datetime, timezone
from typing import Any, cast

from mysqlx import Result, Row, RowResult, Schema, Session, Table, expr, get_session
from mysqlx.errors import OperationalError
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

warnings.filterwarnings("ignore", module="mysqlx.protobuf")

CURRENT_VERSION = 7


class MySQLXSession(MemorySession):
    __session: Session
    __schema: Schema
    __tbl_entities: Table
    __tbl_sent_files: Table
    __tbl_sessions: Table
    __tbl_update_state: Table

    def __init__(
        self,
        user: str,
        password: str,
        host: str = "127.0.0.1",
        port: int = 33060,
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
        self.__session = get_session(
            user=user,
            password=password,
            host=host,
            port=port,
            schema=schema,
            use_pure=True,
        )

        self.__schema = self.__session.get_schema(schema)
        self.__tbl_entities = self.__schema.get_table("entities")
        self.__tbl_sent_files = self.__schema.get_table("sent_files")
        self.__tbl_sessions = self.__schema.get_table("sessions")
        self.__tbl_update_state = self.__schema.get_table("update_state")

        # These values will be saved
        result = cast("Row | None", self.__tbl_sessions.select().execute().fetch_one())
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
        return "<%s: %s>" % (self.__class__.__name__, self.__schema.name)

    def clone(self, to_instance=None):
        cloned = super().clone(to_instance)
        cloned.save_entities = self.save_entities
        return cloned

    def set_dc(self, dc_id, server_address, port):
        super().set_dc(dc_id, server_address, port)
        self._update_session_table()

        # Fetch the auth_key corresponding to this data center
        row = cast(
            "Row | None", self.__tbl_sessions.select("auth_key").execute().fetch_one()
        )
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
        self.__session.start_transaction()
        try:
            self.__session.sql("truncate sessions").execute()
            self.__tbl_sessions.insert(
                "dc_id",
                "server_address",
                "port",
                "auth_key",
                "takeout_id",
            ).values(
                self._dc_id,
                self._server_address,
                self._port,
                self._auth_key.key if self._auth_key else b"",
                self._takeout_id,
            ).execute()
            self.__session.commit()
        except Exception:
            self.__session.rollback()
            raise

    def get_update_state(self, entity_id: int):
        result = (
            self.__tbl_update_state.select("pts", "qts", "date", "seq")
            .where("id = :id")
            .bind("id", entity_id)
            .execute()
        )
        row = cast("Row | None", cast("RowResult", result).fetch_one())
        if row:
            pts, qts, date, seq = row
            date = datetime.fromtimestamp(date, tz=timezone.utc)
            return State(pts, qts, date, seq, unread_count=0)

    def set_update_state(self, entity_id: int, state: State):
        assert state.date
        _insert_or_update(
            self.__tbl_update_state,
            "id",
            id=entity_id,
            pts=state.pts,
            qts=state.qts,
            date=state.date.timestamp(),
            seq=state.seq,
        )

    def get_update_states(self):  # type: ignore
        rows = cast(
            "list[Row]",
            self.__tbl_update_state.select("id", "pts", "qts", "date", "seq")
            .execute()
            .fetch_all(),
        )
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

    def save(self):
        """Saves the current session object as session_user_id.session"""
        # This is a no-op if there are no changes to commit, so there's
        # no need for us to keep track of an "unsaved changes" variable.
        return self.__session.commit()

    def close(self):
        """Closes the connection unless we're working in-memory"""
        self.save()
        return self.__session.close()

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
        for row in rows:
            _insert_or_update(
                self.__tbl_entities,
                "id",
                id=row[0],
                hash=row[1],
                username=row[2],
                phone=row[3],
                name=row[4],
                date=now,
            )

    def get_entity_rows_by_phone(self, phone: int):  # type: ignore
        result = cast(
            "RowResult",
            self.__tbl_entities.select("id", "hash")
            .where("phone = :phone")
            .bind("phone", phone)
            .execute(),
        )
        return result.fetch_one()

    def get_entity_rows_by_username(self, username: str):
        result = cast(
            "RowResult",
            self.__tbl_entities.select("id", "hash", "date")
            .where("username = :username")
            .bind("username", username)
            .execute(),
        )
        rows = cast("list[Row]", result.fetch_all())

        if not rows:
            return None

        # If there is more than one result for the same username, evict the oldest one
        if len(rows) > 1:
            rows.sort(key=lambda t: t[2] or 0)
            update = (
                self.__tbl_entities.update().set("username", None).where("id = :id")
            )
            for row in rows[:-1]:
                update.bind("id", row[0]).execute()
        row = rows[-1]
        return row[0], row[1]

    def get_entity_rows_by_name(self, name: str):  # type: ignore
        result = cast(
            "RowResult",
            self.__tbl_entities.select("id", "hash")
            .where("name = :name")
            .bind("name", name)
            .execute(),
        )
        return result.fetch_one()

    def get_entity_rows_by_id(self, id: int, exact: bool = True):  # type: ignore
        if exact:
            select = (
                self.__tbl_entities.select("id", "hash")
                .where("id = :id")
                .bind("id", id)
            )
        else:
            select = (
                self.__tbl_entities.select("id", "hash")
                .where("id = :user or id = :chat or id = :channel")
                .bind("user", get_peer_id(PeerUser(id)))
                .bind("chat", get_peer_id(PeerChat(id)))
                .bind("channel", get_peer_id(PeerChannel(id)))
            )
        result = cast("RowResult", select.execute())
        return result.fetch_one()

    def get_file(self, md5_digest: bytes, file_size: int, cls: Any):
        result = cast(
            "RowResult",
            self.__tbl_sent_files.select("id", "hash")
            .where("md5_digest = :md5 and file_size = :size and type = :type")
            .bind("md5", md5_digest)
            .bind("size", file_size)
            .bind("type", _SentFileType.from_type(cls).value)
            .execute(),
        )
        if row := cast("Row", result.fetch_one()):
            # Both allowed classes have (id, access_hash) as parameters
            return cls(row[0], row[1])

    def cache_file(self, md5_digest: bytes, file_size: int, instance: Any):
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache %s instance" % type(instance))
        _insert_or_update(
            self.__tbl_sent_files,
            "md5_digest",
            "file_size",
            "type",
            md5_digest=md5_digest,
            file_size=file_size,
            type=_SentFileType.from_type(type(instance)).value,
            id=instance.id,
            hash=instance.access_hash,
        )


def _insert_or_update(tbl: Table, /, *pks: str, **data: Any) -> Result:
    qpks = [f"`{pk}`" for pk in pks]
    qdata = {f"`{k}`": v for k, v in data.items()}
    try:
        return tbl.insert(*qdata.keys()).values(*qdata.values()).execute()
    except OperationalError:
        update = tbl.update()
        for k, v in qdata.items():
            if k not in qpks:
                if v is None:
                    v = expr("null")
                update = update.set(k, v)
        where: list[str] = []
        binds: dict[str, Any] = {}
        for n, pk in enumerate(qpks):
            where.append(f"{pk} = :val{n}")
            binds[f"val{n}"] = qdata[pk]
        return update.where(" and ".join(where)).bind(binds).execute()
