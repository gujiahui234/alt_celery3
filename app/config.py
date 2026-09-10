"""Environment-driven configuration for the alt_celery3 Celery application.

Every runtime setting can be tuned through environment variables without any
code change. All variables are documented in the project ``.env.example``
file, e.g. ``CELERY_BROKER_URL`` and ``CELERY_RESULT_BACKEND`` pointing at an
existing, password-protected redis-stack server.
"""

from __future__ import annotations

import os
from pathlib import Path

from celery.schedules import crontab
from dotenv import find_dotenv, load_dotenv

#: Absolute path of the project root (parent of this package directory).
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load a local ``.env`` file when present so the same defaults work on a
# developer laptop and inside Docker (where variables come from ``env_file``).
load_dotenv(dotenv_path=find_dotenv(str(PROJECT_ROOT / ".env"), usecwd=False))


def _env_bool(name: str, default: bool = False) -> bool:
    """Return the environment variable ``name`` parsed as a boolean.

    Args:
        name: Environment variable name.
        default: Value returned when the variable is not set.

    Returns:
        ``True`` when the value is one of 1/true/yes/on (case-insensitive).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    """Return the environment variable ``name`` parsed as an integer.

    Args:
        name: Environment variable name.
        default: Value used when the variable is unset or invalid.

    Returns:
        The parsed integer.
    """
    raw = os.getenv(name)
    try:
        return int(raw) if raw is not None else default
    except ValueError:
        return default


# --- Application identity ---------------------------------------------------
APP_NAME = "alt_celery3"

# Canonical task names. Keeping them here (instead of strings spread through
# the code) gives producers and the beat schedule a single source of truth.
TASK_EXAMPLE_ADD = "tasks.example.add"
TASK_SCHEDULED_ADD = "tasks.scheduled.add"
TASK_TRY_MYSQL = "tasks.db.try_mysql"
TASK_GET_ONE_STUDENT = "tasks.db.get_one_student"
TASK_GENERATE_MANY_STUDENTS = "tasks.db.generate_many_students"
TASK_INIT_WEB_DB = "tasks.db.init_web_db"
TASK_GET_UN_GROUPS = "tasks.ai.get_un_groups"

#: Prefix of the Redis key that remembers the most recent periodic execution.
LAST_RUN_KEY_PREFIX = "alt-celery3:last-run:"

# --- MySQL web_db (business database) ---------------------------------------
# Connection settings for the tasks that probe web_db connectivity and store
# generated students (see app.tasks.db_tasks). Keep credentials in `.env`.
WEB_DB_HOST: str = os.getenv("MYSQL_WEB_HOST", "127.0.0.1")
WEB_DB_PORT: int = _env_int("MYSQL_WEB_PORT", 3306)
WEB_DB_USER: str = os.getenv("MYSQL_WEB_USER", "web_user")
WEB_DB_PASSWORD: str = os.getenv("MYSQL_WEB_PASSWORD", "")
WEB_DB_DATABASE: str = os.getenv("MYSQL_WEB_DATABASE", "web_db")

# --- MySQL admin account (database/user lifecycle management) ----------------
# Privileged account used ONLY by the ``init_web_db`` task to drop/recreate
# the ``web_db`` / ``log_db`` databases and their owning users. Keep the
# credentials in `.env` (see `.env.example`); never hardcode them.
ADMIN_USER: str = os.getenv("MYSQL_ADMIN_USER", "root")
ADMIN_PASSWORD: str = os.getenv("MYSQL_ADMIN_PASSWORD", "")

# --- Broker / result backend ------------------------------------------------
# An existing, password-protected redis-stack server is used for both the
# message broker and the result backend (they may use different DB numbers).
BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1")
RESULT_EXPIRES: int = _env_int("CELERY_RESULT_EXPIRES", 86_400)

# --- Celery behaviour -------------------------------------------------------
TIMEZONE: str = os.getenv("CELERY_TIMEZONE", "Asia/Shanghai")
ENABLE_UTC: bool = _env_bool("CELERY_ENABLE_UTC", False)
TASK_ALWAYS_EAGER: bool = _env_bool("CELERY_TASK_ALWAYS_EAGER", False)
TASK_EAGER_PROPAGATES: bool = _env_bool("CELERY_TASK_EAGER_PROPAGATES", True)

# --- Beat / periodic tasks --------------------------------------------------
#: File used by celery beat to persist its schedule between restarts.
BEAT_SCHEDULE_FILE: str = os.getenv(
    "CELERY_BEAT_SCHEDULE_FILE",
    str(PROJECT_ROOT / "data" / "celerybeat-schedule"),
)
ENABLE_EXAMPLE_BEAT: bool = _env_bool("CELERY_ENABLE_EXAMPLE_BEAT", True)
EXAMPLE_BEAT_MINUTES: int = _env_int("CELERY_EXAMPLE_BEAT_MINUTES", 30)

# --- sclog-lite (operation logging) -----------------------------------------
#: Directory for sclog-lite rotating file logs.
SCLOG_LOG_DIR: str = os.getenv("SCLOG_LOG_DIR", str(PROJECT_ROOT / "logs"))
#: Log database owner account (also used by the ``init_web_db`` task).
SCLOG_MYSQL_USER: str = os.getenv("SCLOG_MYSQL_USER", "log_user")
SCLOG_MYSQL_PASSWORD: str = os.getenv("SCLOG_MYSQL_PASSWORD", "")
SCLOG_MYSQL_DATABASE: str = os.getenv("SCLOG_MYSQL_DATABASE", "log_db")
#: When true, ``setup_logger(mysql=True)`` reads the ``SCLOG_MYSQL_*`` variables
#: (loaded from `.env`` above) and enables the asynchronous MySQL log backend.
SCLOG_MYSQL_ENABLED: bool = _env_bool("SCLOG_MYSQL_ENABLED", True)

# --- SiliconFlow (硅基流动) LLM API ------------------------------------------
#: API key for the SiliconFlow Chat Completion endpoint (``.env``).
API_KEY_GJLD: str = os.getenv("API_KEY_GJLD", "")
#: Base URL of the OpenAI-compatible SiliconFlow endpoint (``.env``).
BASE_URL: str = os.getenv("BASE_URL", "https://api.siliconflow.cn/v1")
#: Chat model used by the AI tasks (overridable via ``.env``).
GJLD_MODEL: str = os.getenv("GJLD_MODEL", "deepseek-ai/DeepSeek-V4-Flash")


def build_beat_schedule() -> dict[str, dict[str, object]]:
    """Assemble the celery-beat schedule for this application.

    New periodic tasks should add an entry here (and their task module to the
    ``include`` list in :mod:`app.celery_app`). Entries are picked up every
    time beat (re)starts.

    Returns:
        Mapping of beat-entry names to Celery beat schedule options
        (``task``, ``schedule``, ``kwargs``).
    """
    schedule: dict[str, dict[str, object]] = {}

    if ENABLE_EXAMPLE_BEAT:
        # Demonstrates dispatching an ordinary task on a fixed interval.
        schedule["scheduled-add-example"] = {
            "task": TASK_SCHEDULED_ADD,
            "schedule": crontab(minute=f"*/{max(1, EXAMPLE_BEAT_MINUTES)}"),
            "kwargs": {"x": 21, "y": 21},
        }

    return schedule
