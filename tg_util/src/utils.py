from asyncio.events import get_running_loop
from datetime import timedelta
from functools import partial
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, cast, overload

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import Awaitable, Callable
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


_loop: "AbstractEventLoop"


@overload
async def wrap_async[**P, T](
    func: "Callable[P, T]",
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T: ...
@overload
async def wrap_async[**P, T](
    func: "Callable[P, Awaitable[T]]",
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T: ...
async def wrap_async[**P, T](
    func: "Callable[P, T | Awaitable[T]]",
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    if iscoroutinefunction(func):
        return await func(*args, **kwargs)
    if callable(func):
        func = cast("Callable[P, T]", func)
        global _loop
        try:
            loop = _loop
        except NameError:
            loop = _loop = get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))
    err = f"{func} is neither a callable or awaitable"
    raise TypeError(err)
