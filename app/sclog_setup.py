"""sclog-lite integration for alt_celery3 (application logging middleware).

sclog-lite wires console, rotating-file and (optionally) an asynchronous,
batched MySQL log backend through :func:`sclog_lite.setup_logger`. Following
the lifecycle pattern of its application scaffold example
(``examples/app_scaffold.py`` / ``examples/mysql_from_env.py``), the logger is
configured exactly once when the application starts and flushed via
:func:`sclog_lite.shutdown` on teardown.

In this Celery application the "middleware" role is played by Celery signals:
``worker_process_init`` sets the logger up in every worker process and
``worker_shutdown`` flushes pending MySQL batches (see :mod:`app.celery_app`).
Producers and eager-mode runs go through :func:`get_logger` which lazily
initialises on first use, so no code path can log before configuration.

The MySQL backend is configured entirely through ``SCLOG_MYSQL_*`` environment
variables (loaded from the project ``.env`` file) by passing ``mysql=True``;
database outages must never break console/file logging — that isolation is
guaranteed by sclog-lite itself (bounded queue, retries, JSONL dead letters).
"""

from __future__ import annotations

import threading
from typing import Any

from app import config

#: Guards the one-time initialisation across threads / preforked workers.
_lock = threading.Lock()

#: The lazily created sclog-lite logger (Loguru-compatible). ``None`` until
#: :func:`get_logger` is called for the first time in this process.
_logger: Any | None = None


def get_logger() -> Any:
    """Return the shared application logger, initialising it on first use.

    The first call runs :func:`sclog_lite.setup_logger` with console and
    rotating-file output, plus the MySQL backend when ``SCLOG_MYSQL_ENABLED``
    is true (connection details come from the ``SCLOG_MYSQL_*`` environment
    variables documented in ``.env.example``).

    Returns:
        The configured Loguru-compatible logger instance.
    """
    global _logger
    if _logger is None:
        with _lock:
            if _logger is None:  # double-checked locking
                from sclog_lite import setup_logger

                options: dict[str, Any] = {
                    "console": True,
                    "file": True,
                    "log_dir": config.SCLOG_LOG_DIR,
                    "file_options": {"rotation": "10 MB", "retention": "7 days"},
                }
                if config.SCLOG_MYSQL_ENABLED:
                    # ``mysql=True`` makes sclog-lite read the SCLOG_MYSQL_*
                    # environment variables (host/port/user/password/database/
                    # table) and start its asynchronous batched writer.
                    options["mysql"] = True
                _logger = setup_logger(**options)
    return _logger


def shutdown_logging(timeout: float = 10.0) -> None:
    """Flush pending log batches and release sclog-lite resources.

    Mirrors the ``try/finally: shutdown()`` contract of the sclog-lite
    examples: MySQL writes happen on a background thread, so this must be
    invoked on controlled application teardown (worker shutdown, CLI exit) to
    avoid losing buffered entries.

    Args:
        timeout: Maximum number of seconds to wait for the queue to drain.
    """
    global _logger
    if _logger is None:
        return
    from sclog_lite import shutdown

    shutdown(timeout=timeout)
    _logger = None
