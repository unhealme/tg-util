from pathlib import Path
from typing import ClassVar, Literal, Self

from msgspec import Struct, json, yaml

from .enums import FileType

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class Decodable(Struct):
    __jdec__: ClassVar[json.Decoder[Self]]

    @classmethod
    def decode_json(cls, buf: bytes, /) -> Self:
        try:
            return cls.__jdec__.decode(buf)
        except AttributeError:
            cls.__jdec__ = json.Decoder(cls, dec_hook=dec_hook)
            return cls.decode_json(buf)

    @classmethod
    def decode_yaml(cls, buf: bytes, /) -> Self:
        return yaml.decode(buf, type=cls, dec_hook=dec_hook)

    @classmethod
    def from_path(cls, fp: str | Path, fmt: Literal["json", "yaml"]) -> Self:
        match fmt:
            case "json":
                func = cls.decode_json
            case "yaml":
                func = cls.decode_yaml
            case Never:
                err = f"invalid format: {Never}"
                raise ValueError(err)
        return func(Path(fp).read_bytes())


class TLSchemaBase(Struct, tag=True, tag_field="_"):
    pass


class FileAttribute(Struct):
    width: int | None
    height: int | None
    duration: float | None
    size: int | None
    type: FileType
    id: int


class EntityStats(Struct):
    type: str
    title: str
    username: str
    id: int
    medias: int
    files: int
    messages: int

    @property
    def ratio(self):
        return (self.medias + self.files) / self.messages


def dec_hook(type: type, obj: "Any"):
    if type is Path:
        return Path(obj)
    err = f"type {type} is not implemented"
    raise NotImplementedError(err)
