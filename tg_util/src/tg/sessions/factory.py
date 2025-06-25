TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib.parse import ParseResult


def create(url: "ParseResult"):
    match url.scheme.lower():
        case "mysqlx":
            from .mysqlx import MySQLXSession as arc

        case "sqlite" | "sqlite3":
            from .custom import CustomSession

            return CustomSession(url.path.strip("/"))

        case "postgresql":
            from .pg import PSQLSession as arc

        case Never:
            err = f"unknown database scheme: {Never}"
            raise ValueError(err)
    return arc(
        user=url.username or "",
        password=url.password or "",
        host=url.hostname or "127.0.0.1",
        port=url.port or 0,
        schema=url.path.strip("/"),
    )
