from enum import Enum

from msgspec import Struct


class Value(Struct):
    path: str
    arc: str


class FileType(Enum):
    Image = Value("Photo", "images")
    Video = Value("Video", "videos")
    Other = Value("", "files")

    @property
    def path(self):
        return self.value.path

    @property
    def arc(self):
        return self.value.arc
