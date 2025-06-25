#!/usr/bin/env python
__version__ = "r2025.06.25-0"

import asyncio
import contextlib
import logging
import re
import sys
from argparse import ArgumentParser, BooleanOptionalAction
from enum import Enum
from pathlib import Path
from urllib.parse import ParseResult, parse_qs, urlparse

from msgspec import UNSET, Struct, UnsetType
from PIL import Image, ImageDraw
from telethon import TelegramClient
from tqdm.contrib import DummyTqdmFile
from tqdm.contrib.logging import logging_redirect_tqdm

from .src import ABC, ARGSBase, arc
from .src.input import InputFile
from .src.log import setup_logging
from .src.sheet import SheetGenerator
from .src.tg import sessions
from .src.tg.messages.wrapper import InputMessageWrapper, MessageWrapped
from .src.tg.utils import (
    get_file_attr,
    iter_messages,
    parse_entity,
    resolve_entity,
)
from .src.types import (
    Decodable,
    FileAlreadyExists,
    FileType,
    MessageHasNoFile,
    MessageValidationError,
)
from .src.types.tqdm import tqdm
from .src.utils import format_duration, parse_proxy, round_size, wrap_async

TYPE_CHECKING = False
if TYPE_CHECKING:
    from asyncio import _CoroutineLike
    from collections.abc import Sequence
    from typing import Any

    from telethon.hints import Entity
    from telethon.tl.custom import Message


logger = logging.getLogger(__name__)

TQDM_ERR = DummyTqdmFile(sys.stderr)
TQDM_OUT = DummyTqdmFile(sys.stdout)


class Mode(Enum):
    Interactive = 1
    File = 2
    URL = 3
    Unset = 0

    def __repr__(self):
        return self.name


class Arguments(ARGSBase):
    archive: str
    categorize: bool
    config: Path | None
    download_path: Path | None
    download_threads: int
    file: InputFile | None
    mode: Mode
    proxy: str | None
    session: str
    urls: list[tuple[str | int, int]]

    always_write_meta: bool
    create_sheet: bool
    debug: bool
    overwrite: bool
    reverse_download: bool
    single_url: bool
    thumbs_only: bool
    use_takeout: bool

    _ientity: str | int
    _imsg_id: list[tuple[int, int | None]]

    def __init__(self):
        self.mode = Mode.Unset
        self._ientity = ""
        self._imsg_id = []


class Config(Decodable):
    archive: str | UnsetType = UNSET
    categorize: bool | UnsetType = UNSET
    create_sheet: bool | UnsetType = UNSET
    debug: bool | UnsetType = UNSET
    download_path: str | UnsetType = UNSET
    download_threads: int | UnsetType = UNSET
    overwrite: bool | UnsetType = UNSET
    proxy: str | UnsetType = UNSET
    reverse_download: bool | UnsetType = UNSET
    session: str | UnsetType = UNSET
    single_url: bool | UnsetType = UNSET
    thumbs_only: bool | UnsetType = UNSET
    use_takeout: bool | UnsetType = UNSET


class DownloadResult(Struct, array_like=True):
    success: bool
    message: MessageWrapped
    context: dict[str, "Any"]


class TGDownloader(ABC):
    _archive: arc.ArchiveBase
    _args: Arguments
    _client: TelegramClient
    _client_orig: TelegramClient
    _input: InputFile
    _mode: Mode
    _sheet: SheetGenerator
    _tasks: set[asyncio.Task[DownloadResult]]
    _wait_time: float | None
    _wrapper: InputMessageWrapper

    @property
    def _loop(self):
        return self._client.loop

    def __init__(self, args: Arguments, client: TelegramClient):
        self._args = args
        self._client = client
        self._mode = args.mode
        self._wait_time = None
        self._tasks = set()
        self._archive = arc.create(urlparse(self._args.archive))
        if args.file:
            self._input = args.file
        if args.create_sheet:
            self._sheet = SheetGenerator()
        if self._args.use_takeout:
            self._wait_time = 0.0
        self._wrapper = InputMessageWrapper(
            client,
            args.download_path or Path.cwd(),
            args.categorize,
            args.create_sheet,
            args.overwrite,
            args.thumbs_only,
        )

    async def __aenter__(self):
        self._archive = await self._archive.__aenter__()
        if self._args.create_sheet:
            self._sheet = self._sheet.__enter__()
        self._client = await self._client.__aenter__()
        if self._args.use_takeout:
            self._client_orig = self._client
            self._client = await self._client.takeout().__aenter__()
        return self

    async def __aexit__(self, *_exc: "Any"):
        await self.wait_tasks()
        if self._args.use_takeout:
            await self._client.__aexit__(*_exc)
            self._client = self._client_orig
        await self._client.__aexit__(*_exc)
        await self._archive.__aexit__(*_exc)
        if self._args.create_sheet:
            self._sheet.__exit__(*_exc)

    async def run(self):
        logger.debug("current loop %s", self._loop)
        await self._archive.prepare()
        match self._mode:
            case Mode.Interactive:
                with tqdm() as prog:
                    await self.process_ids(
                        self._args._ientity,
                        self._args._imsg_id,
                        prog,
                    )
            case Mode.File:
                async with self._input.ensure_write() as f:
                    async for t in self.process_file():
                        await f.set_status(
                            t.context["lnum"],
                            "# %s" if t.success else "%s # error",
                        )
            case Mode.URL:
                with tqdm(total=len(self._args.urls), desc="Progress") as prog:
                    with tqdm() as subprog:
                        for entity, message_id in self._args.urls:
                            prog.update(1)
                            if self._args.single_url:
                                ids = (message_id, None)
                            else:
                                ids = (message_id - 1, 0)
                            await self.process_ids(entity, [ids], subprog)

    async def process_file(self):
        async for lnum, line in tqdm(aiter(self._input), "Overall", len(self._input)):
            if not (line := line.partition("#")[0].strip()):
                logger.debug("ignoring input at line %s", lnum)
                continue
            _entity, msg_id = parse_url_group(line)
            try:
                entity = await resolve_entity(self._client, _entity)
            except Exception:
                await self._input.set_status(lnum, "##%s (entity error)")
                continue

            async for message, reply_id in iter_messages(
                self._client,
                entity,
                ids=msg_id,
                wait_time=self._wait_time,
            ):
                if done := await self.add_task(
                    self.validate(message, entity, reply_id, lnum=lnum)
                ):
                    for t in done:
                        yield t
        for t in asyncio.as_completed(self._tasks):
            r = await self._handle_or_return(t)
            if r:
                yield r
        self._tasks.clear()

    async def process_ids(
        self,
        raw_entity: str | int,
        ids: list[tuple[int, int | None]],
        prog: "tqdm[Any]",
    ):
        entity = await resolve_entity(self._client, raw_entity)
        logger.debug("processing entity: %s", {raw_entity: str(entity)})
        prog.set_description(str(raw_entity))
        prog.reset(0)
        prog.update
        for start_id, end_id in ids:
            logger.debug("processing start: %s, end: %s", start_id, end_id)
            if end_id is None:
                pool = iter_messages(
                    self._client, entity, ids=start_id, wait_time=self._wait_time
                )
                prog.total += 1
            else:
                if end_id == 0:
                    last_id = getattr(
                        await anext(self._client.iter_messages(entity, limit=1)), "id"
                    )
                    logger.debug("fetching last message id got %s", last_id)
                    prog.total += last_id - start_id
                else:
                    prog.total += end_id - start_id - 1
                pool = iter_messages(
                    self._client,
                    entity,
                    min_id=start_id,
                    max_id=end_id,
                    wait_time=self._wait_time,
                    reverse=self._args.reverse_download,
                )
            async for message, reply_id in pool:
                if reply_id is None:
                    prog.update(1)
                await self.add_task(self.validate(message, entity, reply_id))
            prog.refresh()

    async def _handle_or_return(self, t: asyncio.Future[DownloadResult]):
        try:
            return await t
        except FileAlreadyExists as e:
            message, entity, file_id, meta_path = e.args
            try:
                await self._archive.set_complete(file_id)
            except Exception:
                pass
            if self._args.always_write_meta:
                await self._wrapper.write_meta(message, entity, meta_path)
        except MessageValidationError as e:
            if self._args.always_write_meta:
                await self._wrapper.write_meta(*e.args)
        except MessageHasNoFile:
            pass

    async def validate(
        self,
        message: "Message",
        entity: "Entity",
        reply_id: int | None,
        **ctx: "Any",
    ):
        entity_class, _, username, chat_id = parse_entity(entity)
        message_repr = self._wrapper.get_repr(
            message.id,
            entity_class,
            chat_id,
            username,
            reply_id,
        )
        if (file := message.file) is None:
            logger.debug("%s: message does not have any file", message_repr)
            raise MessageHasNoFile
        fattr = get_file_attr(file)
        target_path, meta_path = self._wrapper.resolve_path(
            chat_id,
            username,
            message.id,
            file.name,
            file.ext,
            reply_id,
            fattr,
        )

        if not self._wrapper.overwrite and target_path.exists():
            logger.debug(
                "%s: target file already exists, skipping download", message_repr
            )
            raise FileAlreadyExists(message, entity, fattr.id, meta_path)
        if (msg := await self._archive.check_id(fattr.id)) is not None:
            logger.debug(
                "%s: duplicate file id with message %s, skipping download",
                message_repr,
                msg,
            )
            raise MessageValidationError(message, entity, meta_path)
        wrapped = await self._wrapper.wrap(
            message,
            entity,
            fattr,
            target_path,
            meta_path,
            message_repr,
            reply_id,
        )
        match await self._archive.check_attr(
            wrapped.file_hash,
            fattr.width,
            fattr.height,
            fattr.size,
            fattr.duration,
        ):
            case msg, file_hash, downloaded:
                if not downloaded and msg == str(self):
                    pass
                elif file_hash == wrapped.file_hash:
                    logger.debug(
                        "%s: duplicate file hash with message %s, skipping download",
                        wrapped,
                        msg,
                    )
                    raise MessageValidationError(message, entity, meta_path)
                else:
                    logger.debug(
                        "%s: duplicate attribute with message %s, skipping download",
                        wrapped,
                        msg,
                    )
                    raise MessageValidationError(message, entity, meta_path)
            case None:
                await self._archive.update(
                    fattr.id,
                    str(wrapped),
                    wrapped.message.id,
                    chat_id,
                    username,
                    wrapped.file_hash,
                    fattr.width,
                    fattr.height,
                    fattr.size,
                    fattr.duration,
                    fattr.type.arc,
                )
        return await self.download_message(wrapped, **ctx)

    async def add_task(self, task: "_CoroutineLike[DownloadResult]"):
        self._tasks.add(self._loop.create_task(task))
        if len(self._tasks) >= self._args.download_threads:
            done, pending = await asyncio.wait(
                self._tasks, return_when=asyncio.FIRST_COMPLETED
            )
            self._tasks.difference_update(done)
            self._tasks.update(pending)
            return [r for t in done if (r := await self._handle_or_return(t))]
        return None

    async def wait_tasks(self):
        for t in asyncio.as_completed(self._tasks):
            await self._handle_or_return(t)
        self._tasks.clear()

    async def download_message(self, message: MessageWrapped, **ctx: "Any"):
        download_success = False
        logger.debug("downloading %s as %s", message, message.target_path.name)
        part_file = message.target_path.with_suffix(
            message.target_path.suffix + ".part"
        )
        try:
            fattr = message.file_attr
            with (
                contextlib.redirect_stderr(TQDM_ERR),  # type: ignore
                contextlib.redirect_stdout(TQDM_OUT),  # type: ignore
            ):
                if message.thumbs_only:
                    await self._client.download_media(
                        message.message,  # type: ignore
                        file=str(part_file.absolute()),
                        thumb=-1,
                    )
                else:
                    await self._client.download_media(
                        message.message,  # type: ignore
                        file=str(part_file.absolute()),
                    )
            part_file.rename(message.target_path)
            await self._wrapper.write_meta(
                message.message,
                message.entity,
                message.meta_path,
            )
            download_success = True
            await self._archive.set_complete(fattr.id)
            logger.info("%s: file downloaded", message)
            await self.post_download(message)
            return DownloadResult(download_success, message, ctx)
        except Exception:
            logger.exception("%s: download file error", message)
            return DownloadResult(download_success, message, ctx)
        finally:
            if not download_success:
                part_file.unlink(missing_ok=True)
                logger.debug("%s: uncomplete download, file deleted", message)

    async def post_download(self, message: MessageWrapped):
        fattr = message.file_attr
        if message.create_sheet:
            self._sheet.submit(message.target_path)
        if message.thumbs_only and fattr.type is FileType.Video:
            info = "%sx%s | %s | %s" % (
                fattr.width,
                fattr.height,
                round_size(fattr.size or 0),
                format_duration(fattr.duration or 0.0),
            )
            async with open_image(message.target_path) as img:
                draw = await wrap_async(ImageDraw.Draw, img)
                textbox = await wrap_async(
                    draw.textbbox,
                    (0, img.height),
                    info,
                    anchor="ls",
                )
                await wrap_async(draw.rectangle, textbox, fill=(10, 10, 10))
                await wrap_async(draw.text, (0, img.height), info, anchor="ls")
                await wrap_async(img.save, message.target_path)


@contextlib.asynccontextmanager
async def open_image(fp: Path):
    img = await wrap_async(Image.open, fp)
    try:
        yield img
    finally:
        await wrap_async(img.close)


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
        async with TGDownloader(args, client) as tgdl:
            await tgdl.run()


def __main__():
    try:
        import uvloop as aio  # type: ignore
    except ImportError:
        aio = asyncio

    aio.run(main())


def parse_url_group(s: str) -> tuple[str | int, int]:
    for pat in (
        re.compile(r"((?:https?://)?t.me/\w+)/(\d+)$"),
        re.compile(r"(?:https?://)?t.me/c/(\d+)/(\d+)$"),
    ):
        if p := pat.match(s):
            entity, msg_id = p.groups()
            return int(entity) if entity.isdigit() else entity, int(msg_id)
    raise ValueError(repr(s))


def parse_args(_args: "Sequence[str] | None" = None):
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "urls",
        type=parse_url_group,
        nargs="*",
        action="store",
        metavar="URL",
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
    downloads = parser.add_argument_group("downloads")
    downloads.add_argument(
        "-p",
        "--download-path",
        type=Path,
        default=None,
        help="(default: current directory)",
        metavar="PATH",
        dest="download_path",
    )
    downloads.add_argument(
        "-t",
        "--download-threads",
        type=lambda i: int(i, 10),
        default=8,
        help="(default: %(default)s)",
        metavar="NUM",
        dest="download_threads",
    )
    downloads.add_argument(
        "--categorize",
        action=BooleanOptionalAction,
        default=True,
        help="categorize downloads by chat username/id (default: %(default)s)",
        dest="categorize",
    )
    downloads.add_argument(
        "--overwrite",
        action=BooleanOptionalAction,
        default=True,
        help="overwrite downloaded files (default: %(default)s)",
        dest="overwrite",
    )
    downloads.add_argument(
        "--reverse-download",
        action=BooleanOptionalAction,
        default=False,
        help="download URL(s) in ascending order (default: %(default)s)",
        dest="reverse_download",
    )
    downloads.add_argument(
        "--single-url",
        action=BooleanOptionalAction,
        default=False,
        help="only fetch single message per URL(s) (default: %(default)s)",
        dest="single_url",
    )
    downloads.add_argument(
        "--thumbs-only",
        action=BooleanOptionalAction,
        default=False,
        help="download only thumbnails on videos (default: %(default)s)",
        dest="thumbs_only",
    )
    inputs = parser.add_argument_group("inputs")
    inputs.add_argument(
        "-i",
        "--interactive",
        action="store_const",
        help="force interactive mode",
        dest="mode",
        const=Mode.Interactive,
    )
    inputs.add_argument(
        "-f",
        type=InputFile,
        help="download URL(s) from FILE",
        dest="file",
        metavar="FILE",
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
        action=BooleanOptionalAction,
        default=False,
        dest="use_takeout",
        help="use takeout session (default: %(default)s)",
    )
    post = parser.add_argument_group("post-dl")
    post.add_argument(
        "--create-sheet",
        action=BooleanOptionalAction,
        default=False,
        help="create video contact sheets on videos (default: %(default)s)",
        dest="create_sheet",
    )
    post.add_argument(
        "--meta=always",
        action="store_true",
        help="always write meta even if download fails",
        dest="always_write_meta",
    )
    args = parser.parse_args(_args, Arguments())
    if args.config:
        config = Config.decode_yaml(args.config.read_bytes())
        for sf in config.__struct_fields__:
            sv = getattr(config, sf)
            if sv is not UNSET:
                match sf:
                    case "download_path":
                        sv = Path(sv)
                setattr(args, sf, sv)
    args = parser.parse_args(_args, args)
    if args.mode is Mode.Unset:
        if args.file:
            args.mode = Mode.File
        elif args.urls:
            args.mode = Mode.URL
        else:
            args.mode = Mode.Interactive
    if args.mode is Mode.Interactive:
        entity = input("peer/entity id: ").strip()
        args._ientity = int(entity, 10) if entity.isdigit() else entity
        for s in input("message ids: ").split(","):
            match [s.strip() for s in s.partition("-")]:
                case "", "", "":
                    args._imsg_id.append((0, 0))
                case start, "-", "":
                    args._imsg_id.append((int(start) - 1, 0))
                case "", "-", end:
                    args._imsg_id.append((0, int(end) + 1))
                case start, "", "":
                    args._imsg_id.append((int(start), None))
                case start, _, end:
                    _start, _end = sorted((int(start), int(end)))
                    args._imsg_id.append((_start - 1, _end + 1))
                case never:
                    err = f"invalid input range {never!r}"
                    raise ValueError(err)
        if not args._imsg_id:
            parser.error("not enough input")
    return parser, args
