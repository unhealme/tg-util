from typing import ClassVar, Self

from msgspec import Struct, json, yaml

from .enums import FileType


class Decodable(Struct):
    __json_dec: ClassVar[json.Decoder[Self]]

    @classmethod
    def decode_json(cls, buf: bytes, /) -> Self:
        try:
            return cls.__json_dec.decode(buf)
        except AttributeError:
            cls.__json_dec = json.Decoder(cls)
            return cls.decode_json(buf)

    @classmethod
    def decode_yaml(cls, buf: bytes, /) -> Self:
        return yaml.decode(buf, type=cls)


class TLSchemaBase(Struct, tag=True, tag_field="_"):
    pass


class FileAttribute(Struct):
    width: int | None
    height: int | None
    duration: float | None
    size: int | None
    type: FileType
    id: int
