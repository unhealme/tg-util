from collections.abc import AsyncIterator
from typing import Any, Literal, overload

from telethon import TelegramClient
from telethon.hints import Entity
from telethon.network import MTProtoSender
from telethon.tl.custom import Message
from telethon.tl.custom.file import File
from telethon.tl.types import TypeInputFileLocation

from tg_util.src.types import EntityStats, FileAttribute

ENTITIES: dict[str, Entity]
SENDERS: dict[int, MTProtoSender]

@overload
async def resolve_entity(
    client: TelegramClient,
    entity: Any,
    with_stats: Literal[True],
) -> tuple[Entity, EntityStats]: ...
@overload
async def resolve_entity(
    client: TelegramClient,
    entity: Any,
    with_stats: bool = False,
) -> Entity: ...
async def get_file_hash(
    client: TelegramClient,
    dc_id: int | None,
    location: TypeInputFileLocation,
) -> bytes:
    """known raises: LocationInvalidError"""

def parse_hashtags(message: Message) -> list[str]: ...
def parse_entity(entity: Any) -> tuple[str, str, str, int]:
    """return: class, title, username, id"""

def get_file_attr(file: File) -> FileAttribute: ...
async def get_entity_stats(client: TelegramClient, entity: Entity) -> EntityStats: ...
def iter_messages(
    client: TelegramClient,
    entity: Entity,
    ids: int | None = None,
    max_id: int = 0,
    min_id: int = 0,
    wait_time: float | None = None,
    reverse: bool = False,
    with_reply: bool = True,
) -> AsyncIterator[tuple[Message, int | None]]: ...
