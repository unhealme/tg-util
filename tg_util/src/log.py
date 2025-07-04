import logging

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TextIO

logFormat = logging.Formatter(
    r"%(asctime)s: %(module)s.%(funcName)s: %(levelname)s: %(message)s"
)
logHandler: "logging.StreamHandler[TextIO]"


def default_handler():
    global logHandler
    try:
        return logHandler
    except NameError:
        logHandler = logging.StreamHandler()
        logHandler.setFormatter(logFormat)
        return logHandler


def setup_logging(
    loggers: "Iterable[logging.Logger | str]",
    handler: logging.Handler | None = None,
    debug: bool = False,
):
    if not handler:
        handler = default_handler()
    for logger in loggers:
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
