import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys

LOG_DIR = Path("logs")


def _create_rotating_handler(filename: str, formatter: logging.Formatter):
    """
    Create a RotatingFileHandler that writes to logs/<filename>.
    """
    LOG_DIR.mkdir(exist_ok=True)

    handler = RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=10,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    return handler


def setup_logging():
    """
    Initializes three separate loggers:

      - MyPhotoApp.Core       -> logs/core.log
      - MyPhotoApp.UI         -> logs/ui.log
      - MyPhotoApp.Analytics  -> logs/analytics.log

    Console shows INFO and above only.
    """

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    # Shared console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Core logger
    core_logger = logging.getLogger("MyPhotoApp.Core")
    if not core_logger.handlers:
        core_logger.setLevel(logging.DEBUG)
        core_logger.addHandler(console_handler)
        core_logger.addHandler(_create_rotating_handler("core.log", formatter))
        core_logger.propagate = False

    # UI logger
    ui_logger = logging.getLogger("MyPhotoApp.UI")
    if not ui_logger.handlers:
        ui_logger.setLevel(logging.DEBUG)
        ui_logger.addHandler(console_handler)
        ui_logger.addHandler(_create_rotating_handler("ui.log", formatter))
        ui_logger.propagate = False

    # Analytics logger
    analytics_logger = logging.getLogger("MyPhotoApp.Analytics")
    if not analytics_logger.handlers:
        analytics_logger.setLevel(logging.DEBUG)
        analytics_logger.addHandler(console_handler)
        analytics_logger.addHandler(_create_rotating_handler("analytics.log", formatter))
        analytics_logger.propagate = False

    core_logger.debug("Logging subsystem initialized.")
    return core_logger
