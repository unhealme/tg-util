__all__ = (
    "ArchiveBase",
    "open",
)

from .base import ArchiveBase

TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib.parse import ParseResult


def open(url: "ParseResult"):
    match url.scheme.lower():
        case "mysqlx":
            from .mysqlx import MySQLXArchive as arc

        case "sqlite" | "sqlite3":
            from .sqlite import SQLiteArchive as arc

        case "postgresql":
            from .psql import PSQLArchive as arc

        case Never:
            err = f"unknown database scheme: {Never}"
            raise ValueError(err)
    return arc(url)
