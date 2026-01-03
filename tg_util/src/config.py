from enum import Enum

from msgspec import UNSET, UnsetType

from .types import Decodable


class Takeout(Enum):
    TRUE = "true"
    FALSE = "false"
    FALLBACK = "fallback"

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    def __str__(self):
        return self.value

    def __bool__(self):
        return self.use

    @property
    def use(self):
        return self is Takeout.TRUE or self is Takeout.FALLBACK


class Config(Decodable):
    archive: str | UnsetType = UNSET
    categorize: bool | UnsetType = UNSET
    create_sheet: bool | UnsetType = UNSET
    debug: bool | UnsetType = UNSET
    download_path: str | UnsetType = UNSET
    download_threads: int | UnsetType = UNSET
    export_path: str | UnsetType = UNSET
    ipv6: bool | UnsetType = UNSET
    min_ratio: float | UnsetType = UNSET
    overwrite: bool | UnsetType = UNSET
    proxy: str | UnsetType = UNSET
    reverse_download: bool | UnsetType = UNSET
    session: str | UnsetType = UNSET
    single_url: bool | UnsetType = UNSET
    takeout: Takeout | UnsetType = UNSET
    thumbs_only: bool | UnsetType = UNSET
    to_db: bool | UnsetType = UNSET
