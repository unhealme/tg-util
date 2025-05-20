import logging
from datetime import datetime

from msgspec import Struct, structs
from telethon.tl.types import (
    Document,
    DocumentAttributeFilename,
    DocumentAttributeImageSize,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
    Photo,
    PhotoSizeProgressive,
)
from telethon.utils import get_peer_id

from tg_util.src.tg.utils import parse_entity, parse_hashtags
from tg_util.src.utils import format_duration, round_size

TYPE_CHECKING = False
if TYPE_CHECKING:
    from telethon.tl.custom import Message

logger = logging.getLogger(__name__)


class MessageExport(Struct, array_like=True):
    date: datetime
    chat_id: int
    chat_name: str | None
    chat_username: str | None
    message_id: int
    text: str | None
    clean_text: str | None
    has_photo: bool
    has_video: bool
    size: int | None
    human_readable_size: str | None
    width: int | None
    height: int | None
    resolution: str | None
    duration: str | None
    duration_sec: float | None
    views: int | None
    forwards: int | None
    reactions: int | None
    file_name: str | None
    file_id: int | None
    file_uid: int | None
    grouped_id: int | None
    sender_id: int | None
    sender_name: str | None
    sender_username: str | None
    hashtags: str | None
    fetch_date: datetime

    @classmethod
    def from_message(cls, msg: "Message"):
        assert msg.date
        sender_name = sender_username = sender_id = None
        if msg.sender is not None:
            try:
                _, sender_name, sender_username, sender_id = parse_entity(msg.sender)
            except TypeError:
                logger.warning(
                    "unable to parse sender for entity %s: %r",
                    type(msg.sender),
                    msg.sender,
                )

        _, chat_name, chat_username, _ = parse_entity(msg.chat)
        clean_text = None
        if msg.message is not None:
            clean_text = " ".join(
                [ss for s in msg.message.splitlines() if (ss := s.strip())]
            )

        has_photo = False
        has_video = False
        size = None
        human_readable_size = None
        width = None
        height = None
        resolution = None
        duration = None
        duration_sec = None
        file_name = None
        file_id = None
        file_uid = None
        match msg.media:
            case MessageMediaDocument(document=document):
                match document:
                    case Document(
                        id=id,
                        access_hash=access_hash,
                        size=_size,
                        attributes=attributes,
                    ):
                        size = _size
                        human_readable_size = round_size(_size)
                        file_id = id
                        file_uid = access_hash
                        for a in attributes:
                            match a:
                                case DocumentAttributeVideo(
                                    w=w, h=h, duration=_duration
                                ):
                                    has_video = True
                                    duration_sec = _duration
                                    duration = format_duration(_duration)
                                    resolution = f"{w}x{h}"
                                    width = w
                                    height = h
                                case DocumentAttributeFilename(file_name=_file_name):
                                    file_name = _file_name
                                case DocumentAttributeImageSize(w=w, h=h):
                                    if not resolution:
                                        resolution = f"{w}x{h}"
                                        width = w
                                        height = h
            case MessageMediaPhoto(photo=photo):
                match photo:
                    case Photo(id=id, access_hash=access_hash, sizes=sizes):
                        has_photo = True
                        file_id = id
                        file_uid = access_hash
                        for s in sizes:
                            match s:
                                case PhotoSizeProgressive(w=w, h=h, sizes=sizes):
                                    resolution = f"{w}x{h}"
                                    width = w
                                    height = h
                                    size = sorted(sizes)[-1]
                                    human_readable_size = round_size(sorted(sizes)[-1])
        reactions = None
        if msg.reactions:
            reactions = sum(x.count for x in msg.reactions.results)

        return cls(
            msg.date.replace(tzinfo=None),
            get_peer_id(msg.chat, add_mark=False),
            chat_name,
            chat_username,
            msg.id,
            msg.message,
            clean_text,
            has_photo,
            has_video,
            size,
            human_readable_size,
            width,
            height,
            resolution,
            duration,
            duration_sec,
            msg.views,
            msg.forwards,
            reactions,
            file_name,
            file_id,
            file_uid,
            msg.grouped_id,
            sender_id,
            sender_name,
            sender_username,
            ", ".join(parse_hashtags(msg)) or None,
            datetime.now(),
        )

    def as_tuple(self):
        return structs.astuple(self)
