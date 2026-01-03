from argparse import BooleanOptionalAction
from datetime import timedelta

from msgspec import json

from tg_util.src.config import Takeout
from tg_util.src.types import ARGDefault

TYPE_CHECKING = False
if TYPE_CHECKING:
    from argparse import ArgumentParser
    from typing import Any
    from urllib.parse import ParseResult

JSON_ENC = json.Encoder()


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


def encode_json_str(data: "Any"):
    return JSON_ENC.encode(data).decode()


def add_misc_args(parser: "ArgumentParser", version: str):
    misc = parser.add_argument_group("misc")
    misc.add_argument("-h", "--help", action="help", help="print this help and exit")
    misc.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=ARGDefault(value=False),
        help="enable debug log",
        dest="debug",
    )
    misc.add_argument(
        "-V",
        "--version",
        action="version",
        help="print version",
        version=f"%(prog)s {version}",
    )
    return misc


def add_opts_args(parser: "ArgumentParser"):
    options = parser.add_argument_group("options")
    options.add_argument(
        "-a",
        "--archive",
        dest="archive",
        default=ARGDefault("sqlite::memory:"),
        metavar="scheme:[//user:pass@host:port/]schema",
    )
    options.add_argument(
        "-s",
        "--session",
        dest="session",
        default=ARGDefault("sqlite:telethon.session"),
        metavar="scheme:[//user:pass@host:port/]schema[?api_id=id&api_hash=hash]",
    )
    options.add_argument(
        "-c",
        "--config",
        help="load config from FILE",
        dest="config",
        metavar="FILE",
    )
    options.add_argument(
        "--ipv6",
        action=BooleanOptionalAction,
        default=ARGDefault(value=False),
        dest="ipv6",
        help="use ipv6 (default: %(default)s)",
    )
    options.add_argument(
        "--proxy",
        default=ARGDefault(None),
        dest="proxy",
        metavar="{http,socks4,socks5}://user:pass@host:port",
    )
    options.add_argument(
        "--takeout",
        dest="takeout",
        nargs="?",
        const=Takeout.TRUE,
        default=ARGDefault(Takeout.FALSE),
        choices=list(Takeout),
        type=Takeout,
    )
    return options


def unpack_default[RT, DT](arg: ARGDefault[DT] | RT) -> DT | RT:
    if isinstance(arg, ARGDefault):
        return arg.value
    return arg
