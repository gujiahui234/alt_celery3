"""Bulk student generation task of alt_celery3 (million-scale, threaded).

``generate_many_students`` produces a large volume of simulated Chinese
students (via :func:`class_roster.simulation.simulate_class`) whose birthdays
fall into a configurable window, and persists them into the ``students``
table of ``web_db`` using :mod:`scdb_mysql_speed`. Every generated student is
stored with the 未高考 (``0``) enrollment status.

Performance design (target: >= 1,000,000 rows):

- The workload is partitioned into fixed-size batches (default 5,000 rows)
  **before** any thread starts, so each writer thread streams its own
  disjoint batches without cross-thread coordination (row identity comes
  from the ``AUTO_INCREMENT`` primary key, not from application numbering).
- A :class:`concurrent.futures.ThreadPoolExecutor` writes batches
  concurrently; the bottleneck is network/DB I/O, which threads overlap
  efficiently (the GIL is released inside the MySQL client).
- Every thread owns exactly one dedicated ``SCDBMySQLSpeed`` connection
  (``pool_size=1``, ``pool_max_overflow=0``) reused for all of its batches —
  no per-batch connection churn.
- Each batch is written with a single parameterised ``execute_many`` call
  (bulk INSERT), keeping round trips and SQL parsing overhead minimal.

All operations are recorded through the sclog-lite application logger
(see :mod:`app.sclog_setup`): console, rotating file and the asynchronous
MySQL log backend.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from scdb_mysql_speed import SCDBError, SCDBMySQLMeta, SCDBMySQLSpeed

from app import config
from app.celery_app import celery_app
from app.sclog_setup import get_logger
from app.tasks.db_tasks import (
    ENROLLMENT_STATUS_NONE,
    INSERT_STUDENT_SQL,
    STUDENTS_TABLE_SQL,
    gender_code,
    web_db_meta,
)

#: Default rows per batch (tuned for ``execute_many`` bulk inserts).
DEFAULT_BATCH_SIZE = 5_000
#: Hard upper bound of a single batch (protects the server packet size).
MAX_BATCH_SIZE = 50_000
#: Default number of writer threads.
DEFAULT_THREADS = 8
#: Hard upper bound of writer threads (MySQL ``max_connections`` courtesy).
MAX_THREADS = 32

#: Type alias for one materialised student row (name, gender, birthday, status).
StudentRow = tuple[str, str, Any, int]


def _writer_meta() -> SCDBMySQLMeta:
    """Build a single-connection metadata for one writer thread.

    Returns:
        A :class:`scdb_mysql_speed.SCDBMySQLMeta` identical to the shared
        ``web_db`` metadata but with a pool of exactly one connection, since
        each thread manages its own dedicated instance.
    """
    shared = web_db_meta()
    return replace(shared, pool_size=1, pool_max_overflow=0)


def _generate_batch(
    batch_index: int,
    rows_wanted: int,
    birthday_min: str | None,
    birthday_max: str | None,
) -> list[StudentRow]:
    """Simulate the rows of one batch and map them to storage tuples.

    Args:
        batch_index: Zero-based index of this batch (used for logging only).
        rows_wanted: Number of students to simulate in this batch.
        birthday_min: Lower bound of the birthday window (``class_roster``
            accepts a year, ``YYYY-MM``, ``YYYY-MM-DD`` or ``None``).
        birthday_max: Upper bound of the birthday window (same formats).

    Returns:
        List of ``(name, gender, birthday, enrollment_status)`` tuples ready
        for ``execute_many``; every row starts as 未高考 (``0``).
    """
    from class_roster.simulation import simulate_class

    roster = simulate_class(
        size=rows_wanted,
        birth_start=birthday_min,
        birth_end=birthday_max,
    )
    rows: list[StudentRow] = []
    for student in roster.students:
        rows.append(
            (
                student.name,
                gender_code(student.gender),
                student.birthday,
                ENROLLMENT_STATUS_NONE,
            )
        )
    logger = get_logger()
    logger.bind(component="generate_many_students").debug(
        f"batch={batch_index} 模拟生成 {len(rows)} 名学生（默认未高考）"
    )
    return rows


def _write_batches(
    thread_id: int,
    jobs: list[tuple[int, int]],
    birthday_min: str | None,
    birthday_max: str | None,
    batch_size: int,
    task: Any,
    progress: dict[str, int],
    progress_lock: threading.Lock,
) -> dict[str, int]:
    """Consume assigned batches: generate the rows and bulk-insert them.

    Each invocation of this function runs inside its own thread and owns one
    dedicated ``SCDBMySQLSpeed`` connection for the whole run.

    Args:
        thread_id: Identifier of the writer thread (for logs).
        jobs: List of ``(batch_index, rows_wanted)`` tuples.
        birthday_min: Lower bound of the birthday window (may be ``None``).
        birthday_max: Upper bound of the birthday window (may be ``None``).
        batch_size: Configured batch size (only used for progress logs).
        task: The bound Celery task (used to publish progress states; may be
            ``None`` in unit-test contexts).
        progress: Shared ``{"inserted": int}`` counter mutated under
            ``progress_lock``.
        progress_lock: Lock guarding ``progress``.

    Returns:
        Dictionary with this thread's ``thread_id``, ``inserted`` row count
        and ``batches`` count.

    Raises:
        SCDBError: Propagated from the underlying MySQL client on failure.
    """
    logger = get_logger()
    inserted = 0
    with SCDBMySQLSpeed(_writer_meta()) as db:
        for batch_index, rows_wanted in jobs:
            rows = _generate_batch(batch_index, rows_wanted, birthday_min, birthday_max)
            affected = db.execute_many(INSERT_STUDENT_SQL, rows)
            inserted += affected

            with progress_lock:
                progress["inserted"] += affected
                done = progress["inserted"]
            logger.bind(component="generate_many_students").info(
                f"thread={thread_id} batch={batch_index} 已写入 {affected} 行，"
                f"累计 {done} 行"
            )
            if task is not None:
                try:
                    task.update_state(
                        state="PROGRESS", meta={"inserted": done}
                    )
                except Exception:  # noqa: BLE001 — progress is best-effort.
                    pass

    return {"thread_id": thread_id, "inserted": inserted, "batches": len(jobs)}


@celery_app.task(name=config.TASK_GENERATE_MANY_STUDENTS, bind=True)
def generate_many_students(
    self: Any,
    numbers: int,
    birthday_min: str | None = None,
    birthday_max: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    threads: int = DEFAULT_THREADS,
) -> dict[str, Any]:
    """Generate many simulated students and bulk-insert them into ``web_db``.

    The task validates the request, reads the current ``MAX(number)`` so new
    rows continue the existing numbering, then splits ``numbers`` into
    ``batch_size`` chunks distributed over a thread pool. Each thread streams
    its batches through one dedicated ``scdb-mysql-speed`` connection. Any
    failure aborts the run and reports the partial counts already committed.

    Args:
        self: Bound Celery task instance (``bind=True``), used to publish
            ``PROGRESS`` state updates.
        numbers: Total number of students to generate (must be positive).
        birthday_min: Lower bound of the birthday window — ``"2000"``,
            ``"2000-09"``, ``"2000-09-01"`` or ``None`` for unbounded.
        birthday_max: Upper bound of the birthday window (same formats).
        batch_size: Rows per bulk insert (clamped to ``[100, 50000]``).
        threads: Concurrent writer threads (clamped to ``[1, 32]``).

    Returns:
        Dictionary with ``ok``, ``requested``, ``inserted``, timing/rate
        statistics, the effective ``batch_size``/``threads``, the assigned
        ``id_start``/``id_end`` (AUTO_INCREMENT range) and a UTC
        ``finished_at`` timestamp. On failure ``ok`` is ``False`` and
        ``error`` carries the message.
    """
    logger = get_logger()
    started = time.perf_counter()

    # --- validate request ----------------------------------------------------
    if numbers <= 0:
        return {
            "ok": False,
            "error": f"numbers 必须为正整数，收到 {numbers!r}",
            "inserted": 0,
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    batch_size = max(100, min(int(batch_size), MAX_BATCH_SIZE))
    threads = max(1, min(int(threads), MAX_THREADS))

    # Shared progress counter — defined before the try block so the error
    # branches can always report how many rows were committed.
    progress: dict[str, int] = {"inserted": 0}

    try:
        # --- prepare: table guard + current id high-water mark -----------------
        with SCDBMySQLSpeed(web_db_meta()) as db:
            db.execute(STUDENTS_TABLE_SQL)
            id_base = int(
                db.fetch_one("SELECT COALESCE(MAX(id), 0) FROM students")[0]
            )

        # --- partition into batches (row identity via AUTO_INCREMENT) ----------
        jobs: dict[int, list[tuple[int, int]]] = {
            worker: [] for worker in range(threads)
        }
        batch_index = 0
        remaining = numbers
        while remaining > 0:
            rows_wanted = min(batch_size, remaining)
            worker = batch_index % threads
            jobs[worker].append((batch_index, rows_wanted))
            remaining -= rows_wanted
            batch_index += 1

        total_batches = batch_index
        logger.bind(component="generate_many_students").info(
            f"批量生成启动 numbers={numbers} birthday=[{birthday_min}, "
            f"{birthday_max}] batch_size={batch_size} threads={threads} "
            f"batches={total_batches} id_start={id_base + 1}"
        )

        # --- fan out ------------------------------------------------------------
        progress_lock = threading.Lock()
        thread_results: list[dict[str, int]] = []
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [
                pool.submit(
                    _write_batches,
                    worker,
                    worker_jobs,
                    birthday_min,
                    birthday_max,
                    batch_size,
                    self,
                    progress,
                    progress_lock,
                )
                for worker, worker_jobs in jobs.items()
                if worker_jobs
            ]
            for future in as_completed(futures):
                thread_results.append(future.result())

        inserted = sum(item["inserted"] for item in thread_results)
        elapsed = time.perf_counter() - started
        rate = round(inserted / elapsed, 1) if elapsed > 0 else 0.0
        logger.bind(component="generate_many_students", inserted=inserted).info(
            f"批量生成完成 inserted={inserted}/{numbers} "
            f"elapsed_s={elapsed:.2f} rate={rate}/s"
        )
        return {
            "ok": True,
            "requested": numbers,
            "inserted": inserted,
            "batches": total_batches,
            "batch_size": batch_size,
            "threads": threads,
            "id_start": id_base + 1,
            "id_end": id_base + inserted,
            "birthday_min": birthday_min,
            "birthday_max": birthday_max,
            "elapsed_seconds": round(elapsed, 2),
            "rows_per_second": rate,
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except ValueError as exc:
        # Invalid birthday window or other class_roster validation errors.
        logger.bind(component="generate_many_students").error(
            f"批量生成参数无效 numbers={numbers} birthday=[{birthday_min}, "
            f"{birthday_max}] error={exc}"
        )
        return {
            "ok": False,
            "requested": numbers,
            "inserted": progress["inserted"],
            "error": f"参数无效：{exc}",
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except SCDBError as exc:
        # Database failures: report how many rows were already committed.
        logger.bind(component="generate_many_students").error(
            f"批量生成数据库失败 numbers={numbers} error={exc}"
        )
        return {
            "ok": False,
            "requested": numbers,
            "inserted": progress["inserted"],
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
