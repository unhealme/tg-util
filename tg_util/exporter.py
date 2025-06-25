#!/usr/bin/env python
__version__ = "r2025.06.25-0"

import logging
from argparse import ArgumentParser
from enum import Enum
from pathlib import Path
from urllib.parse import ParseResult, parse_qs, urlparse

import aiofiles
from msgspec import UNSET, UnsetType, json
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError
from tqdm.contrib.logging import logging_redirect_tqdm

from .src import ABC, ARGSBase, arc
from .src.log import setup_logging
from .src.tg import sessions
from .src.tg.messages.export import MessageExport
from .src.tg.utils import (
    get_entity_stats,
    iter_messages,
    parse_hashtags,
    resolve_entity,
)
from .src.types import Decodable, EntityStats, tqdm
from .src.utils import parse_proxy

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any, Sequence

    from telethon.hints import Entity
    from telethon.tl.custom import Dialog

logger = logging.getLogger(__name__)


class Takeout(Enum):
    TRUE = "true"
    FALSE = "false"
    FALLBACK = "fallback"

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    def __str__(self):
        return self.value

    @property
    def use(self):
        return self is Takeout.TRUE or self is Takeout.FALLBACK


class Arguments(ARGSBase):
    config: Path | None
    ids: list[str | int]

    archive: str
    debug: bool
    export_path: Path
    min_ratio: float
    proxy: str | None
    session: str
    takeout: Takeout
    to_db: bool


class Config(Decodable):
    archive: str | UnsetType = UNSET
    debug: bool | UnsetType = UNSET
    export_path: str | UnsetType = UNSET
    min_ratio: float | UnsetType = UNSET
    proxy: str | UnsetType = UNSET
    session: str | UnsetType = UNSET
    takeout: Takeout | UnsetType = UNSET
    to_db: bool | UnsetType = UNSET


class TGExporter(ABC):
    _archive: arc.ArchiveBase
    _args: Arguments
    _client: TelegramClient
    _client_orig: TelegramClient
    _out: Path
    _takeout: Takeout
    _wait_time: float | None

    @property
    def _loop(self):
        return self._client.loop

    def __init__(self, args: Arguments, client: TelegramClient) -> None:
        self._args = args
        self._client = client
        self._out = args.export_path
        self._takeout = args.takeout
        self._wait_time = 0.0 if args.takeout.use else None
        if self._args.to_db:
            self._archive = arc.create(urlparse(self._args.archive))

    async def __aenter__(self):
        if self._args.to_db:
            self._archive = await self._archive.__aenter__()
        self._client = await self._client.__aenter__()
        if self._takeout.use:
            self._client_orig = self._client
            self._client = await self._client.takeout().__aenter__()
        return self

    async def __aexit__(self, *_exc: "Any"):
        if self._takeout.use:
            await self._client.__aexit__(*_exc)
            self._client = self._client_orig
        await self._client.__aexit__(*_exc)
        if self._args.to_db:
            await self._archive.__aexit__(*_exc)

    async def export_chat(
        self,
        c: TelegramClient,
        e: "Entity",
        fn: str,
        prog: tqdm["Any"],
        wait_time: float | None,
    ):
        total = 0
        async with aiofiles.open(self._out / f"{fn}.json", "wb", 0) as out:
            async for message, reply_id in iter_messages(c, e, wait_time=wait_time):
                total += 1
                if reply_id is None:
                    prog.update(1)
                message_d = message.to_dict()
                message_d["_hashtags"] = parse_hashtags(message)
                message_d["peer_id"]["_entity"] = e.to_dict()
                await out.write(json.encode(message_d) + b"\n")
                if self._args.to_db:
                    await self._archive.export(MessageExport.from_message(message))
        return total

    async def export_with_fallback(self, e: "Entity", s: EntityStats):
        fn = f"@{s.username}" if s.username else str(s.id)
        with tqdm(desc=str(s), total=s.messages) as prog:
            if (
                await self.export_chat(self._client, e, fn, prog, self._wait_time) == 0
                and self._takeout is Takeout.FALLBACK
            ):
                logger.debug(
                    "got 0 messages for %s using takeout session, "
                    "falling back to normal session",
                    s,
                )
                await self.export_chat(self._client_orig, e, fn, prog, None)

    async def export_dialogs(self, mr: float):
        td = getattr(await self._client.get_dialogs(0), "total", 0)
        dialog: Dialog
        async for dialog in tqdm(self._client.iter_dialogs(), "Dialogs", td):
            try:
                entity, stats = await resolve_entity(
                    self._client, dialog.input_entity, with_stats=True
                )
                if stats.messages == 0 and self._takeout is Takeout.FALLBACK:
                    stats = await get_entity_stats(self._client_orig, entity)
                if stats.ratio > mr:
                    logger.debug("processing %s with ratio: %.3f", stats, stats.ratio)
                    await self.export_with_fallback(entity, stats)
                else:
                    logger.debug("skipping %s with ratio: %.3f", stats, stats.ratio)
            except ChannelPrivateError:
                continue
            except Exception:
                logger.warning(
                    "skipping %s due to error", dialog.stringify(), exc_info=True
                )

    async def run(self):
        logger.debug("current loop %s", self._loop)
        match self._args.ids:
            case []:
                await self.export_dialogs(self._args.min_ratio)
            case ids:
                for i in ids:
                    try:
                        entity, stats = await resolve_entity(
                            self._client, i, with_stats=True
                        )
                        if stats.messages == 0 and self._takeout is Takeout.FALLBACK:
                            stats = await get_entity_stats(self._client_orig, entity)
                        await self.export_with_fallback(entity, stats)
                    except Exception:
                        logger.warning(
                            "skipping input: %r due to error", i, exc_info=True
                        )


async def main(_args: "Sequence[str] | None" = None):
    argparser, args = parse_args(_args)
    root = logging.getLogger(__package__)
    logging.root.setLevel(logging.ERROR)
    setup_logging((root,), debug=args.debug)
    logger.debug("using args: %s", args)
    match urlparse(args.session):
        case (
            ParseResult(
                username=str(),
                password=str(),
                hostname=str(),
                port=int(),
                query=query,
            ) as url
        ):
            proxy = None
            if args.proxy:
                proxy = parse_proxy(urlparse(args.proxy))
            qs = parse_qs(query)
            session = sessions.create(url)
            client = TelegramClient(
                session,
                int(qs["api_id"][0], 10),
                qs["api_hash"][0],
                proxy=proxy,  # type: ignore
                catch_up=False,
                receive_updates=False,
            )
        case Never:
            argparser.error(f"invalid or incomplete session: {Never}")
    with logging_redirect_tqdm((root,)):
        async with TGExporter(args, client) as tgex:
            await tgex.run()


def __main__():
    try:
        import uvloop as aio  # type: ignore
    except ImportError:
        import asyncio as aio

    aio.run(main())


def parse_args(_args: "Sequence[str] | None" = None):
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "ids",
        nargs="*",
        action="store",
        type=lambda s: int(s, 10) if s.isdigit() else s,
        help="user/chat/channel id or username",
        metavar="ID",
    )
    misc = parser.add_argument_group("misc")
    misc.add_argument("-h", "--help", action="help", help="print this help and exit")
    misc.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable debug log",
        dest="debug",
    )
    misc.add_argument(
        "-V",
        "--version",
        action="version",
        help="print version",
        version=f"%(prog)s {__version__}",
    )
    exports = parser.add_argument_group("exports")
    exports.add_argument(
        "-p",
        "--export-path",
        type=Path,
        default=None,
        help="(default to current directory)",
        metavar="PATH",
        dest="export_path",
    )
    exports.add_argument(
        "--mr",
        "--min-ratio",
        type=float,
        default=0.0,
        help="minimum media to message ratio for export",
        metavar="NUM",
        dest="min_ratio",
    )
    exports.add_argument(
        "--to-db",
        action="store_true",
        help="also export to archive db",
        dest="to_db",
    )
    options = parser.add_argument_group("options")
    options.add_argument(
        "-a",
        "--archive",
        dest="archive",
        default="sqlite::memory:",
        metavar="{sqlite,mysql}://user:pass@host:port/schema",
    )
    options.add_argument(
        "-s",
        "--session",
        metavar="mysql://user:pass@host:port/schema?api_id=id&api_hash=hash",
        dest="session",
    )
    options.add_argument(
        "-c",
        "--config",
        type=Path,
        default=None,
        help="load config from FILE",
        dest="config",
        metavar="FILE",
    )
    options.add_argument(
        "--proxy",
        default=None,
        dest="proxy",
        metavar="{http,socks4,socks5}://user:pass@host:port",
    )
    options.add_argument(
        "--takeout",
        dest="takeout",
        nargs="?",
        const=Takeout.TRUE,
        default=Takeout.FALSE,
        choices=list(Takeout),
        type=Takeout,
    )
    args = parser.parse_args(_args, Arguments())
    if args.config:
        config = Config.decode_yaml(args.config.read_bytes())
        for sf in config.__struct_fields__:
            sv = getattr(config, sf)
            if sv is not UNSET:
                match sf:
                    case "export_path":
                        sv = Path(sv)
                setattr(args, sf, sv)
    args = parser.parse_args(_args, args)
    return parser, args
