from asyncio.coroutines import iscoroutinefunction
from asyncio.events import AbstractEventLoop, get_running_loop
from concurrent.futures import ThreadPoolExecutor
from functools import partial

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Callable

loop: AbstractEventLoop


async def wrap_async[**P, T](
    func: "Callable[P, T]",
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    if iscoroutinefunction(func) or not callable(func):
        err = f"{func} is either a coroutine or not callable"
        raise TypeError(err)
    global loop
    try:
        _ = loop
    except NameError:
        loop = get_running_loop()
    with ThreadPoolExecutor(1) as e:
        return await loop.run_in_executor(e, partial(func, *args, **kwargs))
