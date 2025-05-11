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
        case "mysql":
            from .mysql import MySQLArchive

            arc = MySQLArchive(url)
        case "sqlite" | "sqlite3":
            from .sqlite import SQLiteArchive

            arc = SQLiteArchive(url)
        case Never:
            err = f"unknown database scheme: {Never}"
            raise ValueError(err)
    return arc
