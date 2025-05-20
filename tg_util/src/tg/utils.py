from hashlib import blake2b
from typing import TYPE_CHECKING, Any, Literal, overload

from telethon.tl.functions.upload import GetFileHashesRequest
from telethon.tl.types import (
    Channel,
    ChannelForbidden,
    Chat,
    ChatEmpty,
    ChatForbidden,
    Document,
    DocumentAttributeVideo,
    FileHash,
    InputMessagesFilterDocument,
    InputMessagesFilterPhotoVideo,
    MessageEntityHashtag,
    MessageReplies,
    PeerChannel,
    PeerChat,
    PeerUser,
    Photo,
    PhotoSizeProgressive,
    TypeInputFileLocation,
    User,
    UserEmpty,
)
from telethon.utils import get_inner_text

from tg_util.src.types import EntityStats, FileAttribute, FileType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from telethon import TelegramClient
    from telethon.hints import Entity
    from telethon.network import MTProtoSender
    from telethon.tl.custom import Message
    from telethon.tl.custom.file import File


ENTITIES: dict[str, "Entity"] = {}
SENDERS: dict[int, "MTProtoSender"] = {}


@overload
async def resolve_entity(
    c: "TelegramClient",
    e: "Any",
    with_stats: Literal[True],
) -> tuple["Entity", EntityStats]: ...
@overload
async def resolve_entity(
    c: "TelegramClient",
    e: "Any",
    with_stats: bool = False,
) -> "Entity": ...
async def resolve_entity(c: "TelegramClient", e: "Any", with_stats: bool = False):
    if isinstance(e, str) and e.isdigit():
        e = int(e, 10)
    try:
        entity: Entity = ENTITIES[str(e)]
    except KeyError:
        if isinstance(e, int):
            try:
                entity = await c.get_entity(PeerChannel(e))  # type: ignore
            except Exception:
                try:
                    entity = await c.get_entity(PeerChat(e))  # type: ignore
                except Exception:
                    entity = await c.get_entity(PeerUser(e))  # type: ignore
        else:
            entity = await c.get_entity(e)  # type: ignore
        ENTITIES[str(e)] = entity  # type: ignore
    return (entity, await get_entity_stats(c, entity)) if with_stats else entity


async def get_file_hash(
    client: "TelegramClient",
    dc_id: int | None,
    location: TypeInputFileLocation,
):
    """raises: LocationInvalidError"""
    if dc_id and dc_id != client.session.dc_id:  # type: ignore
        try:
            sender = SENDERS[dc_id]
        except KeyError:
            sender = SENDERS[dc_id] = await client._borrow_exported_sender(dc_id)
        hashes = await client._call(sender, GetFileHashesRequest(location, 0))  # type: ignore
    else:
        hashes: list[FileHash] = await client(GetFileHashesRequest(location, 0))
    file_hash = blake2b()
    for fh in hashes:
        file_hash.update(fh.hash)
    return file_hash.digest()


def parse_hashtags(msg: "Message") -> list[str]:
    if not msg.entities:
        return []
    s = set(
        get_inner_text(
            msg.message,
            (e for e in msg.entities if isinstance(e, MessageEntityHashtag)),
        )
    )
    return sorted(s, key=str.casefold)


def parse_entity(entity: "Any") -> tuple[str, str, str, int]:
    """return: class, title, username, id"""
    entity_username = entity_name = ""
    match entity:
        case User(
            id=id,
            username=username,
            usernames=usernames,
            first_name=fname,
            last_name=lname,
        ):
            if username:
                entity_username = username
            elif usernames:
                entity_username = usernames[0].username
            entity_id = id
            entity_name = " ".join([x for x in (fname, lname) if x is not None])
        case Channel(id=id, username=username, usernames=usernames, title=title):
            if username:
                entity_username = username
            elif usernames:
                entity_username = usernames[0].username
            entity_id = id
            entity_name = title
        case UserEmpty(id=id) | ChatEmpty(id=id):
            entity_id = id
        case (
            Chat(id=id, title=title)
            | ChatForbidden(id=id, title=title)
            | ChannelForbidden(id=id, title=title)
        ):
            entity_id = id
            entity_name = title
        case _:
            raise TypeError
    return entity.__class__.__name__, entity_name, entity_username, entity_id


def get_file_attr(file: "File"):
    width = height = duration = size = None
    ftype = FileType.Other
    match file.media:
        case Document(size=sz, attributes=attributes):
            size = sz
            for attr in attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    ftype = FileType.Video
                    duration = attr.duration
                    width = attr.w
                    height = attr.h
        case Photo(sizes=sizes):
            ftype = FileType.Image
            for s in sizes:
                if isinstance(s, PhotoSizeProgressive):
                    width = s.w
                    height = s.h
                    size = s.sizes[-1]
    return FileAttribute(width, height, duration, size, ftype, file.media.id)


async def get_entity_stats(c: "TelegramClient", e: "Entity"):
    type, title, username, id = parse_entity(e)
    media_count = getattr(
        await c.get_messages(e, 0, filter=InputMessagesFilterPhotoVideo),
        "total",
        0,
    )
    file_count = getattr(
        await c.get_messages(e, 0, filter=InputMessagesFilterDocument),
        "total",
        0,
    )
    message_count = getattr(await c.get_messages(e, 0), "total", -1)
    return EntityStats(
        type,
        title,
        username,
        id,
        media_count,
        file_count,
        message_count,
    )


async def iter_messages(
    client: "TelegramClient",
    entity: "Entity",
    ids: int | None = None,
    max_id: int = 0,
    min_id: int = 0,
    wait_time: float | None = None,
    reverse: bool = False,
    with_reply: bool = True,
) -> "AsyncIterator[tuple[Message, int | None]]":
    message: Message
    async for message in client.iter_messages(
        entity,
        ids=ids,  # type: ignore
        max_id=max_id,
        min_id=min_id,
        wait_time=wait_time,  # type: ignore
        reverse=reverse,
    ):
        if not message:
            continue
        yield message, None
        match with_reply, message.replies:
            case True, MessageReplies(replies=replies) if replies > 0:
                reply_id = 0
                async for reply in client.iter_messages(
                    entity,
                    wait_time=wait_time,  # type: ignore
                    reply_to=message.id,
                ):
                    if not reply:
                        continue
                    yield reply, reply_id
                    reply_id += 1
