TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any
    from urllib.parse import ParseResult


def create(url: "ParseResult", ipv6: bool):
    params: dict[str, Any] = {}
    match url.scheme.lower():
        case "mysqlx":
            from .mysqlx import MySQLXSession

            params.update(
                user=url.username or "telethon",
                password=url.password or "telethon",
            )
            if url.hostname:
                params["host"] = url.hostname
            if url.port:
                params["port"] = url.port
            if schema := url.path.strip("/"):
                params["schema"] = schema
            return MySQLXSession(**params)
        case "sqlite" | "sqlite3":
            from .custom import CustomSession

            return CustomSession(url.path.strip("/"))
        case "postgresql":
            from .pg import PSQLSession

            params.update(
                user=url.username or "telethon",
                password=url.password or "telethon",
                ipv6=ipv6,
            )
            if url.hostname:
                params["host"] = url.hostname
            if url.port:
                params["port"] = url.port
            if schema := url.path.strip("/"):
                params["schema"] = schema
            return PSQLSession(**params)
        case Never:
            err = f"unknown database scheme: {Never}"
            raise ValueError(err)
