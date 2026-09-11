"""Simulation pipeline tasks of alt_celery3 (高考 → 录取 → 在读考试 → 毕业).

Four tasks model the lifecycle of a student cohort, each one operating on
millions of ``students`` rows and therefore built for throughput:

- ``tasks.simu.ncee``      — simulated college entrance exam (高考)
- ``tasks.simu.admission`` — tier-based university admission (高校录取)
- ``tasks.simu.exam``      — in-university exam simulation (本科考试)
- ``tasks.simu.graduate``  — graduation with GPA computation (本科毕业)

Performance design (mirrors :mod:`app.tasks.bulk_student_tasks`):

- Students are processed in **id windows** pulled from a thread-safe shared
  cursor, so work is dynamically balanced without global locks on the data.
- Every thread owns exactly one dedicated ``scdb-mysql-speed`` connection
  (``pool_size=1``) reused across all of its windows.
- Writes use parameterised ``execute_many`` bulk statements; idempotency is
  provided by ``INSERT IGNORE`` against the tables' unique keys, so re-runs
  never duplicate rows.
- Each window publishes a ``PROGRESS`` state for the polling front end.

All operations are recorded through the sclog-lite application logger
(see :mod:`app.sclog_setup`).
"""

from __future__ import annotations

import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from typing import Any

from scdb_mysql_speed import SCDBError, SCDBMySQLSpeed

from app import config
from app.celery_app import celery_app
from app.sclog_setup import get_logger
from app.tasks.bulk_student_tasks import _writer_meta
from app.tasks.db_tasks import (
    ENROLLMENT_STATUS_ENROLLED,
    ENROLLMENT_STATUS_EXAMINED,
    ENROLLMENT_STATUS_GRADUATED,
    ENROLLMENT_STATUS_NONE,
    web_db_meta,
)

#: Default number of writer threads per task.
DEFAULT_THREADS = 8
#: Hard upper bound of writer threads.
MAX_THREADS = 32
#: Width of one student-id window pulled by a thread (rows are sparser than
#: the window, so this over-provisions slightly to amortise query round trips).
STUDENT_WINDOW = 20_000
#: Rows per ``execute_many`` bulk statement.
BULK_CHUNK = 5_000

#: 高考总分分布：正态分布参数与得分区间。
NCEE_MEAN = 530
NCEE_STD = 50
NCEE_MIN = 400
NCEE_MAX = 660

#: 本科考试分数分布：0-100 分的正态分布参数。
UG_MEAN = 70
UG_STD = 15

#: 本科考试课程池（随机指派到每次考试）。
UG_SUBJECTS: tuple[str, ...] = (
    "高等数学",
    "大学英语",
    "线性代数",
    "大学物理",
    "数据结构",
    "操作系统",
    "计算机组成原理",
    "概率论与数理统计",
    "思想政治",
    "体育",
)

#: 录取梯队定义：累计百分位上限 → 高校性质。
#: 985 录取前 5%，211 录取 5%-15%，一本录取 15%-30%，其余进入"其他"。
ADMISSION_TIERS: tuple[tuple[float, str], ...] = (
    (0.05, "985"),
    (0.15, "211"),
    (0.30, "一本"),
    (1.00, "其他"),
)


def _clamp(value: float, low: int, high: int) -> int:
    """Clamp a float into the inclusive integer range ``[low, high]``.

    Args:
        value: Raw sampled value.
        low: Lower bound of the range.
        high: Upper bound of the range.

    Returns:
        The clamped value rounded to the nearest integer.
    """
    return max(low, min(high, round(value)))


def _id_bounds(where: str, params: tuple[Any, ...]) -> tuple[int | None, int | None]:
    """Return ``(MIN(id), MAX(id))`` of the students matching a condition.

    Args:
        where: WHERE body referencing the ``students`` table (no alias).
        params: Query parameters for the WHERE body.

    Returns:
        ``(min_id, max_id)`` or ``(None, None)`` when no rows match.
    """
    with SCDBMySQLSpeed(web_db_meta()) as db:
        row = db.fetch_one(
            f"SELECT MIN(id) AS lo, MAX(id) AS hi FROM students WHERE {where}",
            params,
        )
    if not row or row[0] is None:
        return None, None
    return int(row[0]), int(row[1])


class _IdWindowPool:
    """Thread-safe shared cursor handing out student-id windows.

    Each call to :meth:`next_window` atomically reserves the next
    ``[lo, hi)`` half-open id range; a thread processes the students inside
    that range and pulls the next window until the pool is exhausted.
    """

    def __init__(self, lo: int, hi: int, width: int = STUDENT_WINDOW) -> None:
        """Initialise the pool.

        Args:
            lo: First student id (inclusive).
            hi: Last student id (exclusive upper bound).
            width: Width of each reserved window.
        """
        self._lo = lo
        self._hi = hi
        self._width = max(1, width)
        self._cursor = lo
        self._lock = threading.Lock()

    def next_window(self) -> tuple[int, int] | None:
        """Reserve and return the next window.

        Returns:
            ``(lo, hi)`` half-open id range, or ``None`` when exhausted.
        """
        with self._lock:
            if self._cursor >= self._hi:
                return None
            lo = self._cursor
            self._cursor = min(self._cursor + self._width, self._hi)
            return lo, self._cursor


@celery_app.task(name=config.TASK_SIMU_NCEE, bind=True)
def simu_ncee(
    self: Any, ncee_year: int, threads: int = DEFAULT_THREADS
) -> dict[str, Any]:
    """Simulate the college entrance exam (高考) for one year.

    Eligible students: ``enrollment_status = 0`` (未高考) whose birthday puts
    them in the 高三 age band (17-19 years old at exam time). Every eligible
    student gets one score sampled from ``Normal(530, 50)`` clamped to
    ``[400, 660]``; the exam date is fixed at ``{ncee_year}-06-20``. After
    scoring, each student's ``enrollment_status`` moves to ``10`` (已高考未入学).

    Args:
        self: Bound Celery task instance (progress reporting).
        ncee_year: Exam year; the exam date is ``{ncee_year}-06-20``.
        threads: Concurrent writer threads (clamped to ``[1, 32]``).

    Returns:
        Dictionary with ``ok``, ``examined`` (students scored),
        ``status_updated``, timing statistics and a UTC ``finished_at``
        timestamp. On failure ``ok`` is ``False`` and ``error`` carries the
        message.
    """
    logger = get_logger()
    started = time.perf_counter()
    threads = max(1, min(int(threads), MAX_THREADS))
    exam_date = date(ncee_year, 6, 20)
    birthday_lo = date(ncee_year - 19, 1, 1)
    birthday_hi = date(ncee_year - 17, 12, 31)
    where = "enrollment_status = %s AND birthday BETWEEN %s AND %s"
    params: tuple[Any, ...] = (ENROLLMENT_STATUS_NONE, birthday_lo, birthday_hi)

    def _progress(done: int, scored: int) -> None:
        """Publish best-effort PROGRESS metadata.

        Args:
            done: Number of windows processed so far.
            scored: Students scored so far.
        """
        try:
            self.update_state(
                state="PROGRESS",
                meta={"phase": "ncee", "windows_done": done, "examined": scored},
            )
        except Exception:  # noqa: BLE001 — progress reporting is best-effort.
            pass

    try:
        lo, hi = _id_bounds(where, params)
        if lo is None or hi is None:
            logger.bind(component="simu_ncee").info(
                f"没有符合高三年龄段且未参加高考的学生 year={ncee_year}"
            )
            return {
                "ok": True,
                "ncee_year": ncee_year,
                "examined": 0,
                "status_updated": 0,
                "note": "没有符合高三年龄段且未参加高考的学生",
                "elapsed_seconds": round(time.perf_counter() - started, 2),
                "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }

        logger.bind(component="simu_ncee").info(
            f"高考模拟启动 year={ncee_year} exam_date={exam_date} "
            f"threads={threads} id_range=[{lo}, {hi}]"
        )
        pool = _IdWindowPool(lo, hi + 1)
        counters = {"windows": 0, "examined": 0, "updated": 0}
        lock = threading.Lock()
        rng = random.Random()

        def _run_window() -> None:
            """Process one id window: score students and update status.

            Raises:
                SCDBError: Propagated from the underlying MySQL client.
            """
            window = pool.next_window()
            if window is None:
                return
            w_lo, w_hi = window
            with SCDBMySQLSpeed(_writer_meta()) as db:
                rows = db.fetch_all(
                    "SELECT id FROM students "
                    "WHERE id >= %s AND id < %s AND enrollment_status = %s "
                    "AND birthday BETWEEN %s AND %s",
                    (w_lo, w_hi, ENROLLMENT_STATUS_NONE, birthday_lo, birthday_hi),
                )
                if not rows:
                    return
                scored_rows = [
                    (
                        int(row[0]),
                        _clamp(rng.gauss(NCEE_MEAN, NCEE_STD), NCEE_MIN, NCEE_MAX),
                        exam_date,
                    )
                    for row in rows
                ]
                for start in range(0, len(scored_rows), BULK_CHUNK):
                    db.execute_many(
                        "INSERT IGNORE INTO gaokao_scores "
                        "(student_id, score, exam_date) VALUES (%s, %s, %s)",
                        scored_rows[start : start + BULK_CHUNK],
                    )
                db.execute(
                    "UPDATE students SET enrollment_status = %s "
                    "WHERE id >= %s AND id < %s AND enrollment_status = %s",
                    (ENROLLMENT_STATUS_EXAMINED, w_lo, w_hi, ENROLLMENT_STATUS_NONE),
                )
                with lock:
                    counters["windows"] += 1
                    counters["examined"] += len(scored_rows)
                    counters["updated"] += len(scored_rows)
                    done = counters["windows"]
                    scored = counters["examined"]
            _progress(done, scored)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(_run_window) for _ in range(threads * 4)]
            for future in as_completed(futures):
                future.result()

        examined = counters["examined"]
        elapsed = time.perf_counter() - started
        rate = round(examined / elapsed, 1) if elapsed > 0 else 0.0
        logger.bind(component="simu_ncee", examined=examined).info(
            f"高考模拟完成 examined={examined} elapsed_s={elapsed:.2f} rate={rate}/s"
        )
        return {
            "ok": True,
            "ncee_year": ncee_year,
            "examined": examined,
            "status_updated": counters["updated"],
            "exam_date": exam_date.isoformat(),
            "threads": threads,
            "elapsed_seconds": round(elapsed, 2),
            "rows_per_second": rate,
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except SCDBError as exc:
        logger.bind(component="simu_ncee").error(f"高考模拟失败 error={exc}")
        return {
            "ok": False,
            "ncee_year": ncee_year,
            "examined": counters["examined"],
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

@celery_app.task(name=config.TASK_SIMU_ADMISSION, bind=True)
def simu_admission(
    self: Any, ncee_year: int, threads: int = DEFAULT_THREADS
) -> dict[str, Any]:
    """Admit the ``{ncee_year}`` exam cohort into universities by tier.

    Score percentile bands (by score, descending): top 5% -> 985, 5%-15% ->
    211, 15%-30% -> 一本, the rest -> 其他. Within a tier each student gets a
    random university of that nature and a random major group of that
    university. Admissions are written to ``enrollments`` (academic year =
    ``ncee_year``) and students move to ``20`` (在读).

    Args:
        self: Bound Celery task instance (progress reporting).
        ncee_year: Exam year; students with scores in that year are admitted.
        threads: Concurrent writer threads (clamped to ``[1, 32]``).

    Returns:
        Dictionary with ``ok``, ``admitted``, per-nature admission counts and
        timing statistics. On failure ``ok`` is ``False`` and ``error``
        carries the message.
    """
    logger = get_logger()
    started = time.perf_counter()
    threads = max(1, min(int(threads), MAX_THREADS))
    date_lo = date(ncee_year, 6, 1)
    date_hi = date(ncee_year, 6, 30)

    def _progress(**meta: Any) -> None:
        """Publish best-effort PROGRESS metadata.

        Args:
            **meta: Arbitrary progress metadata.
        """
        try:
            self.update_state(state="PROGRESS", meta=meta)
        except Exception:  # noqa: BLE001 — progress reporting is best-effort.
            pass

    try:
        with SCDBMySQLSpeed(web_db_meta()) as db:
            total = int(
                db.fetch_one(
                    "SELECT COUNT(*) FROM gaokao_scores "
                    "WHERE exam_date BETWEEN %s AND %s",
                    (date_lo, date_hi),
                )[0]
            )
            if total == 0:
                logger.bind(component="simu_admission").info(
                    f"{ncee_year} 年没有高考成绩，无需录取"
                )
                return {
                    "ok": True,
                    "ncee_year": ncee_year,
                    "fetched": 0,
                    "admitted": 0,
                    "note": "该年份没有高考成绩",
                    "elapsed_seconds": round(time.perf_counter() - started, 2),
                    "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
                }
            bounds: list[int] = []
            for pct, _nature in ADMISSION_TIERS:
                offset = min(total - 1, max(0, int(total * pct) - 1))
                row = db.fetch_one(
                    "SELECT score FROM gaokao_scores "
                    "WHERE exam_date BETWEEN %s AND %s "
                    "ORDER BY score DESC LIMIT 1 OFFSET %s",
                    (date_lo, date_hi, offset),
                )
                bounds.append(int(row[0]))
            univ_rows = db.fetch_all(
                "SELECT id, nature FROM universities", result_format="dict"
            )
            group_rows = db.fetch_all(
                "SELECT id, university_id FROM major_groups", result_format="dict"
            )

        groups_by_university: dict[int, list[int]] = {}
        for row in group_rows:
            groups_by_university.setdefault(int(row["university_id"]), []).append(
                int(row["id"])
            )
        pools: dict[str, list[int]] = {}
        for row in univ_rows:
            univ_id = int(row["id"])
            if groups_by_university.get(univ_id):
                pools.setdefault(str(row["nature"]), []).append(univ_id)

        def _pick(nature: str, rng: random.Random) -> tuple[int, int] | None:
            """Pick a random (university, major group) pair of one nature.

            Args:
                nature: University nature tier to pick from.
                rng: Random generator used for assignment.

            Returns:
                ``(university_id, major_group_id)`` or ``None`` when the tier
                has no universities with major groups.
            """
            candidates = pools.get(nature) or []
            fallback = pools.get("其他") or []
            for pool_ids in (candidates, fallback):
                if not pool_ids:
                    continue
                univ_id = rng.choice(pool_ids)
                return univ_id, rng.choice(groups_by_university[univ_id])
            return None

        band_defs: list[tuple[str, int, int]] = [
            ("985", bounds[0], NCEE_MAX + 1),
            ("211", bounds[1], bounds[0]),
            ("一本", bounds[2], bounds[1]),
            ("其他", NCEE_MIN - 1, bounds[2]),
        ]
        out_queue: "queue.Queue[list[tuple[int, int, int]] | None]" = queue.Queue()
        admitted_by_nature: dict[str, int] = {
            nature: 0 for _, nature in ADMISSION_TIERS
        }
        counters = {"admitted": 0, "status_updated": 0}
        lock = threading.Lock()
        rng = random.Random()
        processed = 0

        with SCDBMySQLSpeed(web_db_meta()) as db:
            sid_row = db.fetch_one(
                "SELECT MIN(student_id) AS lo, MAX(student_id) AS hi "
                "FROM gaokao_scores WHERE exam_date BETWEEN %s AND %s",
                (date_lo, date_hi),
            )
        if sid_row is None or sid_row[0] is None:
            logger.bind(component="simu_admission").info(
                f"{ncee_year} 年没有高考成绩，无需录取"
            )
            return {
                "ok": True,
                "ncee_year": ncee_year,
                "fetched": 0,
                "admitted": 0,
                "note": "该年份没有高考成绩",
                "elapsed_seconds": round(time.perf_counter() - started, 2),
                "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }
        pool = _IdWindowPool(int(sid_row[0]), int(sid_row[1]) + 1)

        def _produce() -> None:
            """Stream score rows band by band, enqueueing write chunks."""
            nonlocal processed
            while True:
                window = pool.next_window()
                if window is None:
                    break
                w_lo, w_hi = window
                with SCDBMySQLSpeed(web_db_meta()) as db:
                    rows = db.fetch_all(
                        "SELECT student_id, score FROM gaokao_scores "
                        "WHERE exam_date BETWEEN %s AND %s "
                        "AND student_id >= %s AND student_id < %s",
                        (date_lo, date_hi, w_lo, w_hi),
                    )
                if not rows:
                    continue
                chunk: list[tuple[int, int, int]] = []
                for student_id, score in rows:
                    sid = int(student_id)
                    score_value = int(score)
                    for nature, band_lo, band_hi in band_defs:
                        if band_lo <= score_value < band_hi:
                            picked = _pick(nature, rng)
                            if picked is not None:
                                chunk.append((sid, picked[0], picked[1]))
                                admitted_by_nature[nature] += 1
                            break
                if chunk:
                    for start in range(0, len(chunk), BULK_CHUNK):
                        out_queue.put(chunk[start : start + BULK_CHUNK])
                with lock:
                    processed += len(rows)
                    current = processed
                _progress(
                    phase="assign",
                    processed=current,
                    total=total,
                    admitted_by_nature=dict(admitted_by_nature),
                )
            for _ in range(threads):
                out_queue.put(None)

        def _writer() -> None:
            """Consume write chunks: insert enrollments and update status.

            Raises:
                SCDBError: Propagated from the underlying MySQL client.
            """
            with SCDBMySQLSpeed(_writer_meta()) as db:
                while True:
                    chunk = out_queue.get()
                    if chunk is None:
                        return
                    enrollment_rows = [
                        (sid, univ_id, group_id, ncee_year)
                        for sid, univ_id, group_id in chunk
                    ]
                    for start in range(0, len(enrollment_rows), BULK_CHUNK):
                        db.execute_many(
                            "INSERT IGNORE INTO enrollments "
                            "(student_id, university_id, major_group_id, academic_year) "
                            "VALUES (%s, %s, %s, %s)",
                            enrollment_rows[start : start + BULK_CHUNK],
                        )
                    student_ids = [sid for sid, _u, _g in chunk]
                    placeholders = ", ".join(["%s"] * len(student_ids))
                    db.execute(
                        "UPDATE students SET enrollment_status = %s "
                        f"WHERE id IN ({placeholders})",
                        tuple([ENROLLMENT_STATUS_ENROLLED, *student_ids]),
                    )
                    with lock:
                        counters["admitted"] += len(enrollment_rows)
                        counters["status_updated"] += len(enrollment_rows)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(_writer) for _ in range(threads)]
            _produce()
            for future in futures:
                future.result()

        admitted = counters["admitted"]
        elapsed = time.perf_counter() - started
        logger.bind(component="simu_admission", admitted=admitted).info(
            f"录取完成 admitted={admitted} elapsed_s={elapsed:.2f} "
            f"by_nature={admitted_by_nature}"
        )
        return {
            "ok": True,
            "ncee_year": ncee_year,
            "fetched": total,
            "admitted": admitted,
            "status_updated": counters["status_updated"],
            "admitted_by_nature": admitted_by_nature,
            "threads": threads,
            "elapsed_seconds": round(elapsed, 2),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except SCDBError as exc:
        logger.bind(component="simu_admission").error(f"录取失败 error={exc}")
        return {
            "ok": False,
            "ncee_year": ncee_year,
            "admitted": counters["admitted"],
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }


@celery_app.task(name=config.TASK_SIMU_EXAM, bind=True)
def simu_exam(
    self: Any, academic_year: int, threads: int = DEFAULT_THREADS
) -> dict[str, Any]:
    """Simulate 5-10 in-university exams for every enrolled student.

    Eligible students: ``enrollment_status = 20`` (在读) whose enrollment
    academic year falls within ``[academic_year - 3, academic_year]``. Exam
    dates are sampled without replacement from the non-vacation periods of
    the academic year; scores are sampled from ``Normal(70, 15)`` clamped
    to ``[0, 100]``.

    Args:
        self: Bound Celery task instance (progress reporting).
        academic_year: Starting year of the academic year (e.g. 2027).
        threads: Concurrent writer threads (clamped to ``[1, 32]``).

    Returns:
        Dictionary with ``ok``, ``students_examined``, ``exams_recorded``
        and timing statistics. On failure ``ok`` is ``False``.
    """
    logger = get_logger()
    started = time.perf_counter()
    threads = max(1, min(int(threads), MAX_THREADS))
    enroll_lo = academic_year - 3
    ranges: list[tuple[date, date]] = [
        (date(academic_year, 9, 1), date(academic_year + 1, 1, 14)),
        (date(academic_year + 1, 2, 21), date(academic_year + 1, 7, 10)),
    ]

    def _progress(done: int, students: int, exams: int) -> None:
        """Publish best-effort PROGRESS metadata.

        Args:
            done: Number of windows processed so far.
            students: Students processed so far.
            exams: Exam rows recorded so far.
        """
        try:
            self.update_state(
                state="PROGRESS",
                meta={
                    "phase": "exam",
                    "windows_done": done,
                    "students_examined": students,
                    "exams_recorded": exams,
                },
            )
        except Exception:  # noqa: BLE001 — progress reporting is best-effort.
            pass

    try:
        lo, hi = _id_bounds(
            "id IN (SELECT student_id FROM enrollments WHERE academic_year "
            "BETWEEN %s AND %s) AND enrollment_status = %s",
            (enroll_lo, academic_year, ENROLLMENT_STATUS_ENROLLED),
        )
        if lo is None or hi is None:
            logger.bind(component="simu_exam").info(
                f"没有在读学生 academic_year={academic_year}"
            )
            return {
                "ok": True,
                "academic_year": academic_year,
                "students_examined": 0,
                "exams_recorded": 0,
                "note": "没有符合条件（在读）的学生",
                "elapsed_seconds": round(time.perf_counter() - started, 2),
                "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }

        logger.bind(component="simu_exam").info(
            f"本科考试模拟启动 academic_year={academic_year}-"
            f"{academic_year + 1} threads={threads} id_range=[{lo}, {hi}]"
        )
        pool = _IdWindowPool(lo, hi + 1)
        counters = {"windows": 0, "students": 0, "exams": 0}
        lock = threading.Lock()
        rng = random.Random()

        def _random_dates(count: int) -> list[date]:
            """Sample ``count`` distinct dates from non-vacation ranges.

            Args:
                count: Number of distinct dates to sample.

            Returns:
                Sorted list of sampled exam dates.
            """
            spans: list[tuple[int, int, date]] = []
            for span_lo, span_hi in ranges:
                days = (span_hi - span_lo).days + 1
                spans.append((0 if not spans else spans[-1][1], days, span_lo))
            total = spans[-1][1]
            picks = sorted(rng.sample(range(total), min(count, total)))
            dates: list[date] = []
            for offset in picks:
                for base, days, span_lo in spans:
                    if offset < days:
                        dates.append(span_lo + timedelta(days=offset))
                        break
            return dates

        def _run_window() -> None:
            """Process one id window: generate and record exam rows.

            Raises:
                SCDBError: Propagated from the underlying MySQL client.
            """
            window = pool.next_window()
            if window is None:
                return
            w_lo, w_hi = window
            with SCDBMySQLSpeed(_writer_meta()) as db:
                rows = db.fetch_all(
                    "SELECT DISTINCT e.student_id FROM enrollments e "
                    "JOIN students s ON s.id = e.student_id "
                    "WHERE e.student_id >= %s AND e.student_id < %s "
                    "AND e.academic_year BETWEEN %s AND %s "
                    "AND s.enrollment_status = %s",
                    (
                        w_lo,
                        w_hi,
                        enroll_lo,
                        academic_year,
                        ENROLLMENT_STATUS_ENROLLED,
                    ),
                )
                if not rows:
                    return
                exam_rows: list[tuple[int, int, str, float, date]] = []
                for row in rows:
                    student_id = int(row[0])
                    k = rng.randint(5, 10)
                    for exam_date in _random_dates(k):
                        score = _clamp(rng.gauss(UG_MEAN, UG_STD), 0, 100)
                        exam_rows.append(
                            (
                                student_id,
                                academic_year,
                                rng.choice(UG_SUBJECTS),
                                score,
                                exam_date,
                            )
                        )
                for start in range(0, len(exam_rows), BULK_CHUNK):
                    db.execute_many(
                        "INSERT IGNORE INTO undergraduate_scores "
                        "(student_id, academic_year, subject, score, exam_date) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        exam_rows[start : start + BULK_CHUNK],
                    )
                with lock:
                    counters["windows"] += 1
                    counters["students"] += len(rows)
                    counters["exams"] += len(exam_rows)
                    done = counters["windows"]
                    students_done = counters["students"]
                    exams_done = counters["exams"]
            _progress(done, students_done, exams_done)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(_run_window) for _ in range(threads * 4)]
            for future in as_completed(futures):
                future.result()

        students_examined = counters["students"]
        exams_recorded = counters["exams"]
        elapsed = time.perf_counter() - started
        rate = round(exams_recorded / elapsed, 1) if elapsed > 0 else 0.0
        logger.bind(
            component="simu_exam", exams_recorded=exams_recorded
        ).info(
            f"本科考试模拟完成 students={students_examined} "
            f"exams={exams_recorded} elapsed_s={elapsed:.2f} rate={rate}/s"
        )
        return {
            "ok": True,
            "academic_year": academic_year,
            "students_examined": students_examined,
            "exams_recorded": exams_recorded,
            "threads": threads,
            "elapsed_seconds": round(elapsed, 2),
            "rows_per_second": rate,
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except SCDBError as exc:
        logger.bind(component="simu_exam").error(f"本科考试模拟失败 error={exc}")
        return {
            "ok": False,
            "academic_year": academic_year,
            "students_examined": counters["students"],
            "exams_recorded": counters["exams"],
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }


@celery_app.task(name=config.TASK_SIMU_GRADUATE, bind=True)
def simu_graduate(
    self: Any, graduate_year: int, threads: int = DEFAULT_THREADS
) -> dict[str, Any]:
    """Graduate the cohort enrolled 4 years before ``graduate_year``.

    Eligible students: ``enrollment_status = 20`` (在读) whose enrollment
    academic year equals ``graduate_year - 4``. The GPA is the average of
    standard 4.0-scale grade points mapped from every undergraduate exam
    score; the graduation date is fixed at ``{graduate_year}-07-01``.
    Graduates move to ``30`` (已毕业).

    Args:
        self: Bound Celery task instance (progress reporting).
        graduate_year: Graduation year; graduation date is July 1st.
        threads: Concurrent writer threads (clamped to ``[1, 32]``).

    Returns:
        Dictionary with ``ok``, ``graduated``, ``without_scores``,
        ``average_gpa`` and timing statistics. On failure ``ok`` is ``False``.
    """
    logger = get_logger()
    started = time.perf_counter()
    threads = max(1, min(int(threads), MAX_THREADS))
    enroll_year = graduate_year - 4
    graduate_date = date(graduate_year, 7, 1)

    def _progress(done: int, students: int, graduated: int) -> None:
        """Publish best-effort PROGRESS metadata.

        Args:
            done: Number of windows processed so far.
            students: Students processed so far.
            graduated: Students graduated so far.
        """
        try:
            self.update_state(
                state="PROGRESS",
                meta={
                    "phase": "graduate",
                    "windows_done": done,
                    "students_processed": students,
                    "graduated": graduated,
                },
            )
        except Exception:  # noqa: BLE001 — progress reporting is best-effort.
            pass

    try:
        lo, hi = _id_bounds(
            "id IN (SELECT student_id FROM enrollments WHERE academic_year = %s) "
            "AND enrollment_status = %s",
            (enroll_year, ENROLLMENT_STATUS_ENROLLED),
        )
        if lo is None or hi is None:
            logger.bind(component="simu_graduate").info(
                f"没有符合毕业条件的学生 graduate_year={graduate_year}"
            )
            return {
                "ok": True,
                "graduate_year": graduate_year,
                "graduated": 0,
                "note": "没有符合条件（在读且已满四年）的学生",
                "elapsed_seconds": round(time.perf_counter() - started, 2),
                "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }

        logger.bind(component="simu_graduate").info(
            f"毕业模拟启动 graduate_year={graduate_year} "
            f"enroll_year={enroll_year} threads={threads} id_range=[{lo}, {hi}]"
        )
        pool = _IdWindowPool(lo, hi + 1)
        counters = {"windows": 0, "students": 0, "graduated": 0, "gpa_sum": 0.0}
        lock = threading.Lock()

        def _run_window() -> None:
            """Process one id window: compute GPA, record and update status.

            Raises:
                SCDBError: Propagated from the underlying MySQL client.
            """
            window = pool.next_window()
            if window is None:
                return
            w_lo, w_hi = window
            with SCDBMySQLSpeed(_writer_meta()) as db:
                rows = db.fetch_all(
                    "SELECT e.student_id FROM enrollments e "
                    "JOIN students s ON s.id = e.student_id "
                    "WHERE e.student_id >= %s AND e.student_id < %s "
                    "AND e.academic_year = %s AND s.enrollment_status = %s",
                    (w_lo, w_hi, enroll_year, ENROLLMENT_STATUS_ENROLLED),
                )
                if not rows:
                    return
                graduated = 0
                gpa_sum = 0.0
                for start in range(0, len(rows), 1000):
                    batch = rows[start : start + 1000]
                    student_ids = [int(row[0]) for row in batch]
                    placeholders = ", ".join(["%s"] * len(student_ids))
                    gpa_rows = db.fetch_all(
                        "SELECT student_id, ROUND(AVG(CASE "
                        "WHEN score >= 90 THEN 4.0 WHEN score >= 85 THEN 3.7 "
                        "WHEN score >= 82 THEN 3.3 WHEN score >= 78 THEN 3.0 "
                        "WHEN score >= 75 THEN 2.7 WHEN score >= 72 THEN 2.3 "
                        "WHEN score >= 68 THEN 2.0 WHEN score >= 64 THEN 1.5 "
                        "WHEN score >= 60 THEN 1.0 ELSE 0.0 END), 2) AS gpa "
                        "FROM undergraduate_scores "
                        "WHERE student_id IN (" + placeholders + ") "
                        "GROUP BY student_id",
                        tuple(student_ids),
                    )
                    if not gpa_rows:
                        continue
                    grad_rows = [
                        (int(row[0]), float(row[1]), graduate_date)
                        for row in gpa_rows
                    ]
                    db.execute_many(
                        "INSERT IGNORE INTO graduation_scores "
                        "(student_id, gpa, graduation_date) VALUES (%s, %s, %s)",
                        grad_rows,
                    )
                    status_ph = ", ".join(["%s"] * len(grad_rows))
                    db.execute(
                        "UPDATE students SET enrollment_status = %s "
                        "WHERE id IN (" + status_ph + ")",
                        tuple(
                            [ENROLLMENT_STATUS_GRADUATED, *(r[0] for r in grad_rows)]
                        ),
                    )
                    graduated += len(grad_rows)
                    gpa_sum += sum(r[1] for r in grad_rows)
                with lock:
                    counters["windows"] += 1
                    counters["students"] += len(rows)
                    counters["graduated"] += graduated
                    counters["gpa_sum"] += gpa_sum
                    done = counters["windows"]
                    students_done = counters["students"]
                    graduated_done = counters["graduated"]
            _progress(done, students_done, graduated_done)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(_run_window) for _ in range(threads * 4)]
            for future in as_completed(futures):
                future.result()

        graduated = counters["graduated"]
        gpa_sum = counters["gpa_sum"]
        average_gpa = round(gpa_sum / graduated, 2) if graduated else 0.0
        elapsed = time.perf_counter() - started
        logger.bind(component="simu_graduate", graduated=graduated).info(
            f"毕业模拟完成 graduated={graduated} average_gpa={average_gpa} "
            f"elapsed_s={elapsed:.2f}"
        )
        return {
            "ok": True,
            "graduate_year": graduate_year,
            "graduated": graduated,
            "without_scores": counters["students"] - graduated,
            "average_gpa": average_gpa,
            "threads": threads,
            "elapsed_seconds": round(elapsed, 2),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except SCDBError as exc:
        logger.bind(component="simu_graduate").error(f"毕业模拟失败 error={exc}")
        return {
            "ok": False,
            "graduate_year": graduate_year,
            "graduated": counters["graduated"],
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
