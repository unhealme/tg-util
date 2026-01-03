__version__ = "r2026.01.03-4"


import logging
from argparse import ArgumentParser
from enum import Enum
from pathlib import Path
from urllib.parse import ParseResult, parse_qs, urlparse

import aiofiles
from msgspec import UNSET, json
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
from telethon.tl.types import (
    ChannelForbidden,
    ChatForbidden,
    RestrictionReason,
    UserEmpty,
)
from tqdm.contrib.logging import logging_redirect_tqdm

from .src import ABC, ARGSBase, arc
from .src.config import Config, Takeout
from .src.log import setup_logging
from .src.tg import sessions
from .src.tg.messages.export import MessageExport
from .src.tg.utils import (
    get_entity_stats,
    iter_messages,
    parse_hashtags,
    resolve_entity,
)
from .src.types import ARGDefault, EntityStats, tqdm
from .src.utils import (
    add_misc_args,
    add_opts_args,
    encode_json_str,
    parse_proxy,
    unpack_default,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any, Sequence

    from telethon.hints import Entity
    from telethon.tl.custom import Dialog

logger = logging.getLogger(__name__)


class Mode(Enum):
    CLEANUP = 0
    EXPORT = 1

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"


class Arguments(ARGSBase):
    archive: str
    debug: bool
    export_path: str | None
    ipv6: bool
    min_ratio: float
    mode: Mode
    proxy: str | None
    session: str
    takeout: Takeout
    to_db: bool

    config: str | None
    ids: list[tuple[str | int, int]]


class TGExporter(ABC):
    _archive: arc.ArchiveBase
    _args: Arguments
    _client: TelegramClient
    _client_orig: TelegramClient
    _out: Path
    _takeout: Takeout
    _wait_time: float | None

    _export_ready: bool

    @property
    def _loop(self):
        return self._client.loop

    def __init__(self, args: Arguments, client: TelegramClient) -> None:
        self._args = args
        self._client = client
        self._export_ready = False
        self._takeout = args.takeout
        self._wait_time = 0.0 if args.takeout.use else None

    async def _init_export(self):
        if not self._export_ready:
            self._out = Path(self._args.export_path or Path.cwd())
            if self._args.to_db:
                self._archive = await arc.create(
                    urlparse(self._args.archive)
                ).__aenter__()
            self._export_ready = True

    async def __aenter__(self):
        self._client = await self._client.__aenter__()
        if self._takeout.use:
            self._client_orig = self._client
            self._client = await self._client.takeout().__aenter__()
        return self

    async def __aexit__(self, *exc: "Any"):
        if self._takeout.use:
            await self._client.__aexit__(*exc)
            self._client = self._client_orig
        await self._client.__aexit__(*exc)
        if hasattr(self, "_archive"):
            await self._archive.__aexit__(*exc)

    async def cleanup_chats(self):
        logger.info("cleaning up deleted chats")
        dialog: Dialog
        async for dialog in self._client.iter_dialogs():
            try:
                entity = await resolve_entity(self._client, dialog.input_entity)
                restrict: list[RestrictionReason] | None = getattr(
                    entity, "restriction_reason", None
                )
                if isinstance(entity, (ChannelForbidden, ChatForbidden, UserEmpty)):
                    try:
                        await dialog.delete()
                        logger.info("deleted %s", encode_json_str(entity.to_dict()))
                    except Exception:
                        logger.warning(
                            "failed to delete %s due to error",
                            encode_json_str(entity.to_dict()),
                            exc_info=True,
                        )
                    continue
                if restrict:
                    for reason in restrict:
                        match reason:
                            case RestrictionReason(reason="terms"):
                                try:
                                    await dialog.delete()
                                    logger.info(
                                        "deleted %s",
                                        encode_json_str(entity.to_dict()),
                                    )
                                except Exception:
                                    logger.warning(
                                        "failed to delete %s due to error",
                                        encode_json_str(entity.to_dict()),
                                        exc_info=True,
                                    )
                                continue
            except Exception:
                logger.warning(
                    "failed to process %s due to error",
                    encode_json_str(dialog.to_dict()),
                    exc_info=True,
                )

    async def export_chat(
        self,
        c: TelegramClient,
        e: "Entity",
        m: int,
        fn: str,
        prog: tqdm["Any"],
        wait_time: float | None,
    ):
        await self._init_export()
        total = 0
        async with aiofiles.open(self._out / f"{fn}.json", "wb", 0) as out:
            async for message, reply_id in iter_messages(
                c,
                e,
                min_id=m,
                wait_time=wait_time,
            ):
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

    async def export_with_fallback(self, e: "Entity", m: int, s: EntityStats):
        await self._init_export()
        fn = f"@{s.username}" if s.username else str(s.id)
        with tqdm(desc=str(s), initial=m, total=s.messages) as prog:
            if (
                await self.export_chat(self._client, e, m, fn, prog, self._wait_time)
                == 0
                and self._takeout is Takeout.FALLBACK
            ):
                logger.debug(
                    "got 0 messages for %s using takeout session, "
                    "falling back to normal session",
                    s,
                )
                await self.export_chat(self._client_orig, e, m, fn, prog, None)

    async def export_dialogs(self, mr: float):
        await self._init_export()
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
                    await self.export_with_fallback(entity, 0, stats)
                else:
                    logger.debug("skipping %s with ratio: %.3f", stats, stats.ratio)
            except ChannelPrivateError:
                continue
            except Exception:
                logger.warning(
                    "skipping %s due to error", dialog.stringify(), exc_info=True
                )

    async def export(self):
        await self._init_export()
        logger.debug("current loop %s", self._loop)
        match self._args.ids:
            case []:
                await self.export_dialogs(self._args.min_ratio)
            case ids:
                for i in ids:
                    try:
                        entity, stats = await resolve_entity(
                            self._client, i[0], with_stats=True
                        )
                        if stats.messages == 0 and self._takeout is Takeout.FALLBACK:
                            stats = await get_entity_stats(self._client_orig, entity)
                        await self.export_with_fallback(entity, i[1], stats)
                    except Exception:
                        logger.warning(
                            "skipping input: %r due to error", i, exc_info=True
                        )


async def main(_args: "Sequence[str] | None" = None):
    argparser, args = parse_args(_args)
    pkg = logging.getLogger(__package__)
    logging.root.setLevel(logging.ERROR)
    setup_logging((pkg,), debug=args.debug)
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
            if (proxy := args.proxy) is not None:
                proxy = parse_proxy(urlparse(proxy))
            qs = parse_qs(query)
            session = sessions.create(url, args.ipv6)
            client = TelegramClient(
                session,
                int(qs["api_id"][0], 10),
                qs["api_hash"][0],
                connection=ConnectionTcpAbridged,
                use_ipv6=args.ipv6,
                proxy=proxy,  # type: ignore
                catch_up=False,
                receive_updates=False,
            )
        case Never:
            argparser.error(f"invalid or incomplete session: {Never}")
    with logging_redirect_tqdm((pkg,)), session:
        async with TGExporter(args, client) as tgex:
            match args.mode:
                case Mode.EXPORT:
                    await tgex.export()
                case Mode.CLEANUP:
                    await tgex.cleanup_chats()


def parse_ids(i: str) -> tuple[str | int, int]:
    e, _, m = i.partition("/")
    pe = int(e, 10) if e.isdigit() else e
    if m:
        return pe, int(m, 10) - 1
    return pe, 0


def parse_args(_args: "Sequence[str] | None" = None):
    parser = ArgumentParser(add_help=False)
    add_misc_args(parser, __version__)
    subparser = parser.add_subparsers(required=True, metavar="mode")
    sub_export = subparser.add_parser("export", help=None, add_help=False)
    sub_export.set_defaults(mode=Mode.EXPORT)
    sub_export.add_argument(
        "ids",
        nargs="*",
        action="store",
        type=parse_ids,
        help="user/chat/channel id or username",
        metavar="ID",
    )
    add_misc_args(sub_export, __version__)
    exports = sub_export.add_argument_group("exports")
    exports.add_argument(
        "-p",
        "--export-path",
        default=ARGDefault(None),
        help="(default to current directory)",
        metavar="PATH",
        dest="export_path",
    )
    exports.add_argument(
        "--mr",
        "--min-ratio",
        type=float,
        default=ARGDefault(0.0),
        help="minimum media to message ratio for export",
        metavar="NUM",
        dest="min_ratio",
    )
    exports.add_argument(
        "--to-db",
        action="store_true",
        default=ARGDefault(value=False),
        help="also export to archive db",
        dest="to_db",
    )
    add_opts_args(sub_export)
    sub_cleanup = subparser.add_parser("cleanup", help=None, add_help=False)
    sub_cleanup.set_defaults(mode=Mode.CLEANUP)
    add_misc_args(sub_cleanup, __version__)
    add_opts_args(sub_cleanup)

    args = parser.parse_args(_args, Arguments())
    if args.config:
        config = Config.from_path(args.config, "yaml")
        for f, v in args.__iter_fields__():
            if isinstance(v, ARGDefault):
                if (nv := getattr(config, f)) is not UNSET:
                    setattr(args, f, nv)
                else:
                    setattr(args, f, unpack_default(v))
    return parser, args


def __main__():
    try:
        import uvloop as aio  # type: ignore
    except ImportError:
        import asyncio as aio

    aio.run(main())
