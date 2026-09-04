"""Example periodic task and helpers to inspect scheduled run results.

Celery beat dispatches the periodic tasks declared in
:func:`app.config.build_beat_schedule`. Every scheduled execution is an
ordinary task whose outcome is stored in the configured result backend under
a beat-generated task id. Because that id is not known to producers upfront,
this module keeps a best-effort Redis pointer ("last run" key) so operators
can fetch the result of the most recent scheduled execution later on.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, cast

import redis

from app import config
from app.celery_app import celery_app

logger = logging.getLogger(__name__)


def last_run_key(task_name: str) -> str:
    """Return the Redis key storing the latest task id of ``task_name``.

    Args:
        task_name: Canonical task name (e.g. ``tasks.scheduled.add``).

    Returns:
        The Redis key used to remember the most recent execution.
    """
    return f"{config.LAST_RUN_KEY_PREFIX}{task_name}"


def remember_last_run(task_name: str, task_id: str) -> bool:
    """Persist ``task_id`` as the latest executed id of ``task_name``.

    The write is best-effort on purpose: a transient Redis problem must never
    fail the task itself, only the convenience of looking up its last run.

    Args:
        task_name: Canonical task name of the periodic task.
        task_id: Celery request id of the current execution.

    Returns:
        ``True`` when the pointer was stored, ``False`` when it was skipped
        (eager mode) or could not be written.
    """
    if celery_app.conf.task_always_eager:
        # Eager runs do not touch a real result backend; nothing to record.
        return False
    try:
        client = redis.Redis.from_url(config.RESULT_BACKEND)
        client.set(last_run_key(task_name), task_id, ex=config.RESULT_EXPIRES)
        return True
    except redis.exceptions.RedisError as exc:
        logger.warning("Could not record last-run id for %s: %s", task_name, exc)
        return False


def fetch_last_run(task_name: str) -> str | None:
    """Read the task id of the most recent execution of a periodic task.

    Args:
        task_name: Canonical task name of the periodic task.

    Returns:
        The stored task id, or ``None`` when no execution has been recorded
        yet (e.g. beat never ran).

    Raises:
        redis.exceptions.RedisError: When the result backend is unreachable.
    """
    client = redis.Redis.from_url(config.RESULT_BACKEND)
    # redis>=6 types ``get()`` as ``Awaitable[Any] | Any`` (pipeline support);
    # the synchronous client always returns ``bytes | None`` here.
    raw = cast("bytes | None", client.get(last_run_key(task_name)))
    return raw.decode("utf-8") if raw else None


@celery_app.task(name=config.TASK_SCHEDULED_ADD, bind=True)
def scheduled_add(self: Any, x: int, y: int) -> dict[str, Any]:
    """Periodic counterpart of :func:`app.tasks.example_tasks.add`.

    Celery beat fires this task every ``CELERY_EXAMPLE_BEAT_MINUTES`` minutes
    (see :func:`app.config.build_beat_schedule`). Besides the sum itself it
    records its own request id, which allows ``run_tasks.py latest-scheduled``
    to retrieve the outcome of scheduled runs from the result backend.

    Args:
        x: First addend.
        y: Second addend.

    Returns:
        Dictionary with the sum, request id, whether the last-run pointer
        was recorded and a UTC timestamp of the execution.
    """
    total = x + y
    recorded = remember_last_run(config.TASK_SCHEDULED_ADD, self.request.id)
    return {
        "x": x,
        "y": y,
        "sum": total,
        "request_id": self.request.id,
        "recorded": recorded,
        "run_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
