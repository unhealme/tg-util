import asyncio
from functools import partial

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Callable

loop: asyncio.AbstractEventLoop


async def wrap_async[**P, T](
    func: "Callable[P, T]",
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    global loop
    if asyncio.iscoroutinefunction(func) or not callable(func):
        err = f"{func} is neither a callable or awaitable"
        raise TypeError(err)
    try:
        _loop = loop
    except NameError:
        _loop = loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))
