"""Example plain tasks of alt_celery3.

Regular tasks are invoked by a producer and executed by a worker.
New task modules belong in this sub-package; give every task an explicit,
stable name so it can be referenced from scripts, beat schedules and Flower.
"""

from __future__ import annotations

from app import config
from app.celery_app import celery_app


@celery_app.task(name=config.TASK_EXAMPLE_ADD)
def add(x: int, y: int) -> int:
    """Return the sum of two integers — the canonical getting-started task.

    Args:
        x: First addend.
        y: Second addend.

    Returns:
        The integer sum of ``x`` and ``y``.

    Examples:
        Send it from any producer (e.g. ``python run_tasks.py add``)::

            result = add.delay(2, 5)     # or: add.apply_async(args=[2, 5])
            print(result.get(timeout=30))
    """
    return x + y
