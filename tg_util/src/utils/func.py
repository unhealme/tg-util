from datetime import timedelta

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any
    from urllib.parse import ParseResult


def round_size(n: float | int) -> str:
    for unit in ("Bytes", "KB", "MB"):
        if n < 1024.0:
            if unit == "Bytes":
                return f"{n} {unit}"
            else:
                return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} GB"


def format_duration(secs: float) -> str:
    return str(timedelta(seconds=secs))


def parse_proxy(url: "ParseResult"):
    from python_socks import ProxyType

    proxy: dict[str, Any] = {}
    match url.scheme.lower():
        case "socks" | "socks5":
            proxy["proxy_type"] = ProxyType.SOCKS5
        case "socks4":
            proxy["proxy_type"] = ProxyType.SOCKS4
        case "http" | "https":
            proxy["proxy_type"] = ProxyType.HTTP
    proxy["addr"] = url.hostname
    proxy["port"] = url.port
    if url.username:
        proxy["username"] = url.username
    if url.password:
        proxy["password"] = url.password
    return proxy
