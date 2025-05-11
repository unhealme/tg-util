from datetime import datetime
from typing import Any, Iterator, Self

from tg_util.src.types import Decodable, TLSchemaBase
from tg_util.src.utils import format_duration, round_size


class PeerChat(TLSchemaBase):
    chat_id: int

    @property
    def id(self):
        return self.chat_id


class PeerChannel(TLSchemaBase):
    channel_id: int

    @property
    def id(self):
        return self.channel_id


class PeerUser(TLSchemaBase):
    user_id: int

    @property
    def id(self):
        return self.user_id


class DocumentAttributeImageSize(TLSchemaBase):
    w: int
    h: int


class DocumentAttributeVideo(TLSchemaBase):
    w: int
    h: int
    duration: float


class DocumentAttributeFilename(TLSchemaBase):
    file_name: str


class Document(TLSchemaBase):
    id: int
    access_hash: int
    size: int
    attributes: list[
        DocumentAttributeImageSize
        | DocumentAttributeVideo
        | DocumentAttributeFilename
        | Any
    ]


class PhotoSizeProgressive(TLSchemaBase):
    w: int
    h: int
    sizes: list[int]


class Photo(TLSchemaBase):
    id: int
    access_hash: int
    sizes: list[PhotoSizeProgressive | Any]


class MessageMediaDocument(TLSchemaBase):
    document: Document | Any


class MessageMediaPhoto(TLSchemaBase):
    photo: Photo | Any


class ReactionCount(TLSchemaBase):
    count: int


class MessageReactions(TLSchemaBase):
    results: list[ReactionCount]

    def total_reactions(self):
        return sum(r.count for r in self.results)


class MessageElement(Decodable):
    id: int
    date: datetime
    peer_id: PeerChat | PeerChannel | PeerUser
    _hashtags: list[str]
    grouped_id: int | None = None
    media: MessageMediaDocument | MessageMediaPhoto | Any = None
    views: int | None = None
    forwards: int | None = None
    reactions: MessageReactions | None = None
    message: str = ""

    _has_photo_: bool = False
    _has_video_: bool = False
    _rounded_size_: str = ""
    _res_: str = ""
    _duration_string_: str = ""
    _duration_secs_: str = ""
    _size_: str = ""
    _file_name_: str = ""
    _file_id_: str = ""
    _file_uid_: str = ""

    def __post_init__(self) -> None:
        match self.media:
            case MessageMediaDocument(document):
                match document:
                    case Document(id, access_hash, size, attributes):
                        self._size_ = str(size)
                        self._rounded_size_ = round_size(size)
                        self._file_id_ = str(id)
                        self._file_uid_ = str(access_hash)
                        for a in attributes:
                            match a:
                                case DocumentAttributeVideo(w, h, duration):
                                    self._has_video_ = True
                                    self._duration_secs_ = str(duration)
                                    self._duration_string_ = format_duration(duration)
                                    self._res_ = f"{w}x{h}"
                                case DocumentAttributeFilename(file_name):
                                    self._file_name_ = file_name
                                case DocumentAttributeImageSize(w, h):
                                    if not self._res_:
                                        self._res_ = f"{w}x{h}"
            case MessageMediaPhoto(photo):
                match photo:
                    case Photo(id, access_hash, sizes):
                        self._has_photo_ = True
                        self._file_id_ = str(id)
                        self._file_uid_ = str(access_hash)
                        for s in sizes:
                            match s:
                                case PhotoSizeProgressive(w, h, sizes):
                                    self._res_ = f"{w}x{h}"
                                    self._size_ = str(sorted(sizes)[-1])
                                    self._rounded_size_ = round_size(sorted(sizes)[-1])

    def link(self, resolve_peer_id: bool = False):
        return f"t.me/c/{self.peer_id.id}/{self.id}"

    def clean_text(self):
        return " ".join([ss for s in self.message.splitlines() if (ss := s.strip())])

    def __iter__(self) -> Iterator[str]:
        yield self.date.replace(tzinfo=None).isoformat(" ")
        yield self.link()
        yield self.clean_text()
        yield str(self.has_photo)
        yield str(self.has_video)
        yield self._rounded_size_
        yield self._res_
        yield self._duration_string_
        yield self._duration_secs_
        yield self._size_
        yield str(self.views or 0)
        yield str(self.forwards or 0)
        if self.reactions:
            yield str(self.reactions.total_reactions())
        else:
            yield "0"
        yield self.file_name
        yield self._file_id_
        yield self._file_uid_
        yield str(self.grouped_id or "")

    def __hash__(self):
        return hash(self.id)

    def __lt__(self, v: Self):
        return self._natsort < v._natsort

    def __le__(self, v: Self):
        return self._natsort <= v._natsort

    def __gt__(self, v: Self):
        return self._natsort > v._natsort

    def __ge__(self, v: Self):
        return self._natsort >= v._natsort

    @property
    def _natsort(self):
        return (self.peer_id.id, self.id)

    @property
    def file_name(self):
        return self._file_name_

    @property
    def has_photo(self):
        return self._has_photo_

    @property
    def has_video(self):
        return self._has_video_
