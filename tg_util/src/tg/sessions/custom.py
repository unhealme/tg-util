import os
import sys
from pathlib import Path

from telethon.sessions import SQLiteSession

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class CustomSession(SQLiteSession):
    _closed: bool

    def __init__(self, name: str) -> None:
        self.__orig_session = Path(sys.argv[0]).with_name(name).with_suffix(".session")
        if not self.__orig_session.exists():
            SQLiteSession(str(self.__orig_session)).close()

        if sys.platform == "win32":
            root = Path("R:\\")
        else:
            root = Path("/tmp")
        self.__temp_session = root / f"{name}_{os.getpid()}.session"
        with (
            open(self.__temp_session, "wb", 0) as fo,
            open(self.__orig_session, "rb", 0) as fi,
        ):
            fo.writelines(fi)
        super().__init__(str(self.__temp_session))
        self._closed = False

    def __repr__(self) -> str:
        return "<%s: %s>" % (self.__class__.__name__, self.filename)

    def __enter__(self):
        return self

    def __exit__(self, *exc: "Any"):
        self.close()

    def close(self):
        if not self._closed:
            super().close()
            self.__orig_session.write_bytes(self.__temp_session.read_bytes())
            with (
                open(self.__orig_session, "wb", 0) as fo,
                open(self.__temp_session, "rb", 0) as fi,
            ):
                fo.writelines(fi)
            self.delete()
            self._closed = True
