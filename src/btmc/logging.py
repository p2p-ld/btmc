"""Loggers for btmc's parts, all under one logger that's set up from a :class:`.Config`"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING

from btmc.config import Config

if TYPE_CHECKING:
    from rich.console import Console

LOG_FILE = "btmc.log"
"""The log file's name, in :attr:`.Config.log_dir`"""


def init_logger(name: str, config: Config, console: Console | None = None) -> logging.Logger:
    """
    The logger for a part of btmc, like ``"protocol.swarm"``.

    Without a :attr:`.Config.log_root`, it's ``btmc.protocol.swarm``. The first call
    sets up the ``btmc`` logger with the config's logging settings: to stderr, and to a
    rotating file in :attr:`.Config.log_dir`, each at its own level. Later calls leave it be.

    With a log root, like ``"sciop"``, it's ``sciop.btmc.protocol.swarm``,
    and nothing is set up, so its logs go wherever that application's do.

    Args:
        name: the part of btmc, named like its module
        config: the logging settings
        console: the rich console to log to stderr through, like one showing progress bars,
            so logs are printed above them
    """
    if config.log_root is not None:
        return logging.getLogger(f"{config.log_root}.btmc.{name}")
    root = logging.getLogger("btmc")
    if not root.handlers:
        _add_handlers(root, config, console)
    return logging.getLogger(f"btmc.{name}")


def _add_handlers(root: logging.Logger, config: Config, console: Console | None) -> None:
    stderr: logging.Handler
    try:
        from rich.console import Console
        from rich.logging import RichHandler
    except ImportError:
        # rich comes with the cli extra
        stderr = logging.StreamHandler()
        stderr.setFormatter(logging.Formatter("%(levelname)s [%(name)s]: %(message)s"))
    else:
        stderr = RichHandler(console=console or Console(stderr=True), show_path=False)
        stderr.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
    stderr.setLevel(config.log_level_stderr)
    root.addHandler(stderr)

    if config.log_dir is not None:
        config.log_dir.mkdir(parents=True, exist_ok=True)
        # opened when the first line is written
        file = RotatingFileHandler(
            config.log_dir / LOG_FILE, maxBytes=2**22, backupCount=5, delay=True
        )
        file.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s]: %(message)s"))
        file.setLevel(config.log_level_file)
        root.addHandler(file)
    root.setLevel(min(handler.level for handler in root.handlers))
