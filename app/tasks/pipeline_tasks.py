"""One-stop graduation pipeline task of alt_celery3 (一条龙毕业).

``one_stop_graduation`` chains the four existing simulation tasks for a
single cohort, in order:

1. ``tasks.simu.ncee``      — college entrance exam of ``ncee_year``
2. ``tasks.simu.admission`` — tier-based admission of the ``ncee_year`` cohort
3. ``tasks.simu.exam``      — undergraduate exams, repeated for ``exam_years``
                              consecutive academic years starting at
                              ``ncee_year`` (each call covers the cohort
                              because its enrollment year is ``ncee_year``)
4. ``tasks.simu.graduate``  — graduation of the cohort in ``ncee_year + 4``

The sub-tasks are executed inline (``task.apply``) inside the worker process
that received the pipeline task — no broker round trip, no risk of deadlocking
a single-worker deployment. Each sub-task already publishes its own fine
grained ``PROGRESS`` states; this task publishes coarse step-level progress
(``step`` / ``done`` / ``total``) so polling front ends can show where the
pipeline currently is.

On completion the aggregated statistics (examined / admitted /
exams_recorded / graduated / average_gpa) are returned together with the
per-step result dictionaries. When a step reports ``ok=False`` the chain
stops immediately and the partial summary carries ``failed_step``.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from celery import Task

from app import config
from app.celery_app import celery_app
from app.sclog_setup import get_logger
from app.tasks.simulation_tasks import (
    DEFAULT_THREADS,
    MAX_THREADS,
    simu_admission,
    simu_exam,
    simu_graduate,
    simu_ncee,
)

#: Hard upper bound of simulated academic years for the exam step.
MAX_EXAM_YEARS = 4

#: Type alias for one step result payload returned by a sub-task.
StepResult = dict[str, Any]


def _clamp_step_counts(threads: int, exam_years: int) -> tuple[int, int]:
    """Clamp the pipeline parameters into their valid ranges.

    Args:
        threads: Requested writer threads.
        exam_years: Requested number of academic years of exams.

    Returns:
        The clamped ``(threads, exam_years)`` pair.
    """
    return max(1, min(int(threads), MAX_THREADS)), max(1, min(int(exam_years), MAX_EXAM_YEARS))


@celery_app.task(name=config.TASK_ONE_STOP_GRADUATION, bind=True)
def one_stop_graduation(
    self: Task,
    ncee_year: int,
    threads: int = DEFAULT_THREADS,
    exam_years: int = 1,
) -> dict[str, Any]:
    """Run 高考 → 录取 → 日常考试 → 毕业 for one cohort and aggregate stats.

    Args:
        self: Bound Celery task instance (step-level progress reporting).
        ncee_year: Cohort year; exam date ``{ncee_year}-06-20``, graduation
            date ``{ncee_year + 4}-07-01``.
        threads: Concurrent writer threads forwarded to every sub-task
            (clamped to ``[1, 32]``).
        exam_years: How many consecutive academic years of undergraduate
            exams to simulate starting at ``ncee_year`` (clamped to [1, 4]).

    Returns:
        Dictionary with ``ok``, the cohort ``ncee_year``, aggregated counters
        (``examined`` / ``admitted`` / ``exams_recorded`` / ``graduated`` /
        ``average_gpa`` / ``admitted_by_nature``), per-step payloads under
        ``steps`` and timing statistics. On a failed step ``ok`` is ``False``
        and ``failed_step`` carries the step name; the partial results stay
        in ``steps``.
    """
    logger = get_logger()
    started = time.perf_counter()
    threads, exam_years = _clamp_step_counts(threads, exam_years)

    steps: list[StepResult] = []

    def _progress(step: str, done: int, total: int) -> None:
        """Publish best-effort step-level PROGRESS metadata.

        Args:
            step: Name of the step that is starting.
            done: Number of finished steps.
            total: Total number of steps in the pipeline.
        """
        try:
            self.update_state(
                state="PROGRESS",
                meta={"step": step, "done": done, "total": total},
            )
        except Exception:  # noqa: BLE001 — progress reporting is best-effort.
            pass

    # Build the ordered step plan. The graduation year is ncee_year + 4 so the
    # cohort admitted in ncee_year is the one being graduated.
    plan: list[tuple[str, Callable[..., Any], dict[str, Any]]] = [
        ("ncee", simu_ncee, {"ncee_year": ncee_year, "threads": threads}),
        ("admission", simu_admission, {"ncee_year": ncee_year, "threads": threads}),
    ]
    plan.extend(
        ("exam", simu_exam, {"academic_year": ncee_year + offset, "threads": threads})
        for offset in range(exam_years)
    )
    plan.append(
        ("graduate", simu_graduate, {"graduate_year": ncee_year + 4, "threads": threads})
    )
    total_steps = len(plan)

    logger.bind(component="one_stop_graduation").info(
        f"一条龙毕业启动 ncee_year={ncee_year} threads={threads} "
        f"exam_years={exam_years} steps={total_steps}"
    )

    for index, (step_name, task_obj, kwargs) in enumerate(plan):
        _progress(step=step_name, done=index, total=total_steps)
        logger.bind(component="one_stop_graduation").info(
            f"步骤开始 step={step_name} kwargs={kwargs} ({index + 1}/{total_steps})"
        )
        step_started = time.perf_counter()

        # Inline execution inside the current worker process: no broker round
        # trip, so the chain cannot deadlock even on a concurrency=1 worker.
        eager = task_obj.apply(kwargs=kwargs)
        if eager.failed():
            # task_eager_propagates=False captures the exception into the
            # EagerResult; surface it as a failed step instead of re-raising.
            error = eager.result
            logger.bind(component="one_stop_graduation").error(
                f"步骤失败 step={step_name} error={error}"
            )
            steps.append({"step": step_name, "ok": False, "error": str(error)})
            return {
                "ok": False,
                "ncee_year": ncee_year,
                "failed_step": step_name,
                "steps": steps,
                "elapsed_seconds": round(time.perf_counter() - started, 2),
                "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }

        result = eager.result
        elapsed = round(time.perf_counter() - step_started, 2)
        summary = {"step": step_name, "ok": True, "elapsed_seconds": elapsed, **result}
        steps.append(summary)
        logger.bind(component="one_stop_graduation").info(
            f"步骤完成 step={step_name} elapsed_s={elapsed:.2f} "
            f"ok={result.get('ok')}"
        )

    # --- aggregate the statistics across all steps ---------------------------
    by_step: dict[str, StepResult] = {item["step"]: item for item in steps}
    ncee_result = by_step.get("ncee", {})
    admission_result = by_step.get("admission", {})
    graduate_result = by_step.get("graduate", {})
    exam_results = [item for item in steps if item["step"] == "exam"]

    totals = {
        "examined": int(ncee_result.get("examined", 0) or 0),
        "admitted": int(admission_result.get("admitted", 0) or 0),
        "exams_recorded": sum(int(item.get("exams_recorded", 0) or 0) for item in exam_results),
        "graduated": int(graduate_result.get("graduated", 0) or 0),
        "average_gpa": graduate_result.get("average_gpa", 0.0),
        "admitted_by_nature": admission_result.get("admitted_by_nature", {}),
    }

    elapsed = time.perf_counter() - started
    logger.bind(component="one_stop_graduation", **totals).info(
        f"一条龙毕业完成 examined={totals['examined']} admitted={totals['admitted']} "
        f"exams={totals['exams_recorded']} graduated={totals['graduated']} "
        f"elapsed_s={elapsed:.2f}"
    )
    return {
        "ok": True,
        "ncee_year": ncee_year,
        "exam_years": exam_years,
        "threads": threads,
        **totals,
        "steps": steps,
        "elapsed_seconds": round(elapsed, 2),
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
