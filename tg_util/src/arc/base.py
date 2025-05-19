from tg_util.src import ABC, abstractmethod

TYPE_CHECKING = False
if TYPE_CHECKING:
    from asyncio import Lock
    from typing import Any, Self
    from urllib.parse import ParseResult

    from tg_util.src.tg.messages.export import MessageExport


class ArchiveBase(ABC):
    _params: "ParseResult"
    _lock: "Lock"

    @abstractmethod
    def __init__(self, params: "ParseResult") -> None: ...

    @abstractmethod
    async def __aenter__(self) -> "Self": ...
    @abstractmethod
    async def __aexit__(self, *_exc: "Any") -> None: ...

    @abstractmethod
    async def prepare(self) -> None: ...

    @abstractmethod
    async def check_attr(
        self,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
    ) -> "tuple[Any, Any, Any] | None": ...
    @abstractmethod
    async def check_id(self, file_id: int) -> "Any | None": ...
    @abstractmethod
    async def set_complete(self, file_id: int) -> None: ...

    @abstractmethod
    async def update(
        self,
        file_id: int,
        msg: str,
        msg_id: int,
        chat_id: int,
        chat_username: str | None,
        hash: bytes,
        width: int | None,
        height: int | None,
        size: int | None,
        duration: float | None,
        type: str,
    ) -> None: ...
    @abstractmethod
    async def export(self, message: "MessageExport") -> None: ...
