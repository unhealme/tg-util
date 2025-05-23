import logging
from hashlib import blake2b
from typing import TYPE_CHECKING, Any, cast

from msgspec import Struct, json
from telethon.errors import FileReferenceExpiredError
from telethon.utils import get_input_location

from tg_util.src.tg.utils import get_file_hash, parse_hashtags
from tg_util.src.types import FileAttribute, FileType, MessageHasNoFile
from tg_util.src.utils import wrap_async

if TYPE_CHECKING:
    from pathlib import Path

    from telethon import TelegramClient
    from telethon.hints import Entity
    from telethon.tl.custom import Message

logger = logging.getLogger(__name__)


class InputMessageWrapper(Struct):
    client: "TelegramClient"
    dl_path: "Path"
    categorize: bool
    create_sheet: bool
    overwrite: bool
    thumbs_only: bool

    async def wrap(
        self,
        message: "Message",
        entity: "Entity",
        file_attr: FileAttribute,
        target_path: "Path",
        meta_path: "Path",
        message_repr: str,
        reply_id: int | None,
    ):
        message, hash = await self.get_file_hash(message, message_repr)
        return MessageWrapped(
            message,
            entity,
            reply_id,
            file_attr,
            hash,
            target_path,
            meta_path,
            (
                self.create_sheet
                if file_attr.type is FileType.Video and not self.thumbs_only
                else False
            ),
            self.overwrite,
            self.thumbs_only,
            message_repr,
        )

    def resolve_path(
        self,
        chat_id: int,
        chat_username: str,
        message_id: int,
        file_name: Any,
        file_ext: Any,
        reply_id: int | None,
        file_attr: FileAttribute,
    ):
        """return target_path, meta_path"""
        base_name = "%s_%s%s%s" % (
            chat_id,
            message_id,
            f"_{reply_id}" if reply_id is not None else "",
            file_ext or "",
        )
        if file_attr.type is FileType.Other:
            base_name = file_name or base_name
        if not self.categorize:
            dl_path = self.dl_path
        elif chat_username:
            dl_path = self.dl_path / f"@{chat_username}"
        else:
            dl_path = self.dl_path / str(chat_id)
        if self.thumbs_only:
            dl_path = dl_path.with_name(dl_path.name + " - thumbs")
        meta_path = (dl_path / "Meta" / base_name).with_suffix(".json")
        target_path = dl_path / file_attr.type.path / base_name
        if self.thumbs_only and file_attr.type is FileType.Video:
            target_path = target_path.with_suffix(".webp")
        return target_path, meta_path

    async def get_file_hash(self, message: "Message", message_repr: str):
        try:
            dc, loc = get_input_location(message.media)
            hash = await get_file_hash(self.client, dc, loc)
        except FileReferenceExpiredError:
            message = await self.refetch(message)
            if not (file := message.file):
                raise MessageHasNoFile
            try:
                dc, loc = get_input_location(file.media)
                hash = await get_file_hash(self.client, dc, loc)
            except Exception:
                logger.warning(
                    "unable to get file hash for %s", message_repr, exc_info=True
                )
                hash = blake2b(file.media.id.to_bytes(8)).digest()
        except TypeError:
            logger.warning(
                "unable to get file hash for %s", message_repr, exc_info=True
            )
            assert message.file
            hash = blake2b(message.file.media.id.to_bytes(8)).digest()
        return message, hash

    def get_repr(
        self,
        message_id: int,
        entity_class: str,
        entity_id: int | None,
        username: str | None,
        reply_id: int | None,
    ):
        return "Message(id=%r, reply_id=%r, from=%s" % (
            message_id,
            reply_id,
            "%s(%s)"
            % (
                entity_class,
                ", ".join(
                    [
                        f"{x}={y!r}"
                        for x, y in (
                            ("id", entity_id),
                            ("username", username),
                        )
                        if y is not None
                    ]
                ),
            ),
        )

    async def refetch(self, message: "Message"):
        return cast(
            "Message",
            await self.client.get_messages(message.input_chat, ids=message.id),
        )

    async def write_meta(self, message: "Message", entity: "Entity", fp: "Path"):
        meta = message.to_dict()
        meta["_hashtags"] = parse_hashtags(message)
        meta["peer_id"]["_entity"] = entity.to_dict()
        await wrap_async(fp.parent.mkdir, parents=True, exist_ok=True)
        await wrap_async(
            fp.write_bytes,
            await wrap_async(
                json.format, await wrap_async(json.encode, meta, enc_hook=str)
            ),
        )


class MessageWrapped(Struct):
    message: "Message"
    entity: "Entity"
    reply_id: int | None

    file_attr: FileAttribute
    file_hash: bytes
    target_path: "Path"
    meta_path: "Path"
    create_sheet: bool
    overwrite: bool
    thumbs_only: bool

    _str_: str

    def __str__(self):
        return self._str_
