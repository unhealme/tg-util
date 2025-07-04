from hashlib import blake2b

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

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any

    from telethon import TelegramClient
    from telethon.hints import Entity
    from telethon.network import MTProtoSender
    from telethon.tl.custom import Message
    from telethon.tl.custom.file import File


ENTITIES: dict[str, "Entity"] = {}
SENDERS: dict[int, "MTProtoSender"] = {}


async def resolve_entity(
    client: "TelegramClient",
    entity: "Any",
    with_stats: bool = False,
):
    if isinstance(entity, str) and entity.isdigit():
        entity = int(entity, 10)
    try:
        e: Entity = ENTITIES[str(entity)]
    except KeyError:
        if isinstance(entity, int):
            try:
                e = await client.get_entity(PeerChannel(entity))  # type: ignore
            except Exception:
                try:
                    e = await client.get_entity(PeerChat(entity))  # type: ignore
                except Exception:
                    e = await client.get_entity(PeerUser(entity))  # type: ignore
        else:
            e = await client.get_entity(entity)  # type: ignore
        ENTITIES[str(entity)] = e  # type: ignore
    return (e, await get_entity_stats(client, e)) if with_stats else e


async def get_file_hash(
    client: "TelegramClient",
    dc_id: int | None,
    location: TypeInputFileLocation,
):
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


def parse_hashtags(msg: "Message"):
    if not msg.entities:
        return []
    s = set(
        get_inner_text(
            msg.message,
            (e for e in msg.entities if isinstance(e, MessageEntityHashtag)),
        )
    )
    return sorted(s, key=str.casefold)


def parse_entity(entity: "Any"):
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
            err = f"unable to parse {entity}"
            raise TypeError(err)
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


async def get_entity_stats(client: "TelegramClient", entity: "Entity"):
    type, title, username, id = parse_entity(entity)
    media_count = getattr(
        await client.get_messages(entity, 0, filter=InputMessagesFilterPhotoVideo),
        "total",
        0,
    )
    file_count = getattr(
        await client.get_messages(entity, 0, filter=InputMessagesFilterDocument),
        "total",
        0,
    )
    message_count = getattr(await client.get_messages(entity, 0), "total", -1)
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
):
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
