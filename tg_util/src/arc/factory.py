TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib.parse import ParseResult


def create(url: "ParseResult"):
    match url.scheme.lower():
        case "mysqlx":
            from .mysqlx import MySQLXArchive as arc

        case "sqlite" | "sqlite3":
            from .sqlite import SQLiteArchive as arc

        case "postgresql":
            from .pg import PSQLArchive as arc

        case Never:
            err = f"unknown database scheme: {Never}"
            raise ValueError(err)
    return arc(url)
