"""FastAPI service exposing the alt_celery3 task platform over HTTP.

The service lists every task registered on the shared Celery application
(merged with the contract catalog metadata and the configured beat
schedule), dispatches tasks for asynchronous execution and reports their
progress/results, so producers and operators can drive the platform
without reading the source code.

Endpoints:

- ``GET /api/tasks``                     — all platform tasks with metadata.
- ``GET /api/tasks/{name}``              — one task by its canonical name.
- ``POST /api/tasks/{name}/dispatch``    — dispatch a task (async).
- ``GET /api/tasks/{name}/result/{id}``  — poll state/progress/result.
- ``GET /api/tasks/{name}/eligible-count?year=`` — eligible students of a
  simulation task in one year.
- ``GET /api/tasks/beat``                — the celery-beat schedule.
- ``GET /healthz``                       — liveness probe.

Run it locally with ``python run_api.py`` (defaults to port 8012, see
``API_HOST`` / ``API_PORT`` in ``.env``).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from scdb_mysql_speed import SCDBError, SCDBMySQLSpeed

from alt_celery3_contract import TASK_CATALOG, TaskName

from app.celery_app import celery_app
from app.tasks.db_tasks import (
    ENROLLMENT_STATUS_ENROLLED,
    ENROLLMENT_STATUS_EXAMINED,
    ENROLLMENT_STATUS_NONE,
    web_db_meta,
)

#: Prefix shared by every project task name (e.g. ``tasks.example.add``).
_TASK_NAME_PREFIX = "tasks."

#: Eligible-count SQL rules per simulation task. Each rule maps a simulation
#: year to a ``(FROM/WHERE body, params)`` pair counting the students that
#: the task would process; the body must be a valid ``SELECT COUNT(*) FROM
#: <body>`` fragment.
_ELIGIBLE_COUNT_RULES: dict[str, Any] = {
    TaskName.TASK_SIMU_NCEE.value: lambda year: (
        "students WHERE enrollment_status = %s AND birthday BETWEEN %s AND %s",
        (
            ENROLLMENT_STATUS_NONE,
            date(year - 19, 1, 1),
            date(year - 17, 12, 31),
        ),
    ),
    TaskName.TASK_SIMU_ADMISSION.value: lambda year: (
        "gaokao_scores g JOIN students s ON s.id = g.student_id "
        "WHERE g.exam_date BETWEEN %s AND %s AND s.enrollment_status = %s",
        (
            date(year, 6, 1),
            date(year, 6, 30),
            ENROLLMENT_STATUS_EXAMINED,
        ),
    ),
    TaskName.TASK_SIMU_EXAM.value: lambda year: (
        "students s WHERE s.enrollment_status = %s AND s.id IN "
        "(SELECT student_id FROM enrollments WHERE academic_year BETWEEN %s AND %s)",
        (ENROLLMENT_STATUS_ENROLLED, year - 3, year),
    ),
    TaskName.TASK_SIMU_GRADUATE.value: lambda year: (
        "students s WHERE s.enrollment_status = %s AND s.id IN "
        "(SELECT student_id FROM enrollments WHERE academic_year = %s)",
        (ENROLLMENT_STATUS_ENROLLED, year - 4),
    ),
    # 一条龙流水线的入批口径与首步（高考评测）一致：未高考且 17-19 岁。
    TaskName.TASK_ONE_STOP_GRADUATION.value: lambda year: (
        "students WHERE enrollment_status = %s AND birthday BETWEEN %s AND %s",
        (
            ENROLLMENT_STATUS_NONE,
            date(year - 19, 1, 1),
            date(year - 17, 12, 31),
        ),
    ),
}


def _task_payload_schema(name: str) -> dict[str, Any] | None:
    """Return the JSON schema of a task payload model when available.

    Args:
        name: Canonical task name.

    Returns:
        The JSON schema dictionary, or ``None`` when the task takes no
        structured payload.
    """
    contract = TASK_CATALOG.get(name)
    if contract is None or contract.schema is None:
        return None
    model_json_schema = getattr(contract.schema, "model_json_schema", None)
    return model_json_schema() if callable(model_json_schema) else None


def _task_entry(name: str) -> dict[str, Any]:
    """Build the API representation of one registered task.

    Args:
        name: Canonical task name present in the Celery registry.

    Returns:
        Dictionary with name, module, bound flag, payload schema and beat
        schedule entries that reference the task.
    """
    contract = TASK_CATALOG.get(name)
    beat_entries = [
        {
            "entry": entry_name,
            "schedule": schedule,
        }
        for entry_name, entry in celery_app.conf.beat_schedule.items()
        if entry.get("task") == name
        for schedule in [str(entry.get("schedule", ""))]
    ]
    return {
        "name": name,
        "module": contract.module if contract else None,
        "bound": contract.bound if contract else None,
        "payload_schema": _task_payload_schema(name),
        "beat_entries": beat_entries,
    }


def _registered_task_names() -> list[str]:
    """List the canonical names of all tasks registered on the app.

    Returns:
        Sorted task names owned by this platform (``tasks.*`` namespace).
    """
    # In a producer process the ``include`` modules are only imported when a
    # worker starts, so import them here to populate the task registry.
    celery_app.loader.import_default_modules()
    return sorted(
        task_name
        for task_name in celery_app.tasks
        if task_name.startswith(_TASK_NAME_PREFIX)
    )


def create_api_app() -> FastAPI:
    """Build the FastAPI application for the task platform.

    Returns:
        A :class:`~fastapi.FastAPI` instance with the task catalog and
        beat schedule endpoints.
    """
    app = FastAPI(
        title="alt_celery3 Tasks API",
        description="Celery 任务平台的任务目录与调度信息查询服务。",
        version="0.1.0",
    )

    @app.get("/api/tasks", tags=["tasks"], summary="列出平台所有任务")
    def list_tasks() -> dict[str, Any]:
        """List every task registered on the Celery platform.

        Returns:
            Dictionary with ``total`` and the sorted ``tasks`` entries.
        """
        names = _registered_task_names()
        return {"total": len(names), "tasks": [_task_entry(name) for name in names]}

    @app.get("/api/tasks/beat", tags=["tasks"], summary="列出 beat 周期调度")
    def list_beat_schedule() -> dict[str, Any]:
        """List the configured celery-beat periodic schedule.

        Returns:
            Dictionary with ``total`` and the ``entries`` of the schedule.
        """
        entries = [
            {
                "entry": entry_name,
                "task": entry.get("task"),
                "schedule": str(entry.get("schedule", "")),
                "kwargs": entry.get("kwargs", {}),
            }
            for entry_name, entry in celery_app.conf.beat_schedule.items()
        ]
        return {"total": len(entries), "entries": entries}

    @app.get("/api/tasks/{task_name}", tags=["tasks"], summary="按名称查询任务")
    def get_task(task_name: str) -> dict[str, Any]:
        """Fetch one task by its canonical name.

        Args:
            task_name: Canonical task name, e.g. ``tasks.db.try_mysql``.

        Returns:
            The task entry dictionary.

        Raises:
            HTTPException: 404 when the task name is not registered.
        """
        if task_name not in _registered_task_names():
            raise HTTPException(status_code=404, detail=f"任务 {task_name} 未注册")
        return _task_entry(task_name)

    @app.post(
        "/api/tasks/{task_name}/dispatch",
        tags=["tasks"],
        summary="派发任务（异步执行）",
    )
    def dispatch_task(
        task_name: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Dispatch a registered task to the Celery platform.

        The payload is validated against the contract catalog schema when
        the task declares one; the validated model is then serialised into
        the task kwargs.

        Args:
            task_name: Canonical task name, e.g. ``tasks.simu.ncee``.
            payload: Optional JSON body with the task payload fields.

        Returns:
            Dictionary with ``task_id``, ``task_name`` and the initial
            ``state`` (usually ``PENDING``).

        Raises:
            HTTPException: 404 when the task is not registered, 422 when
                the payload does not satisfy the contract schema.
        """
        contract = TASK_CATALOG.get(task_name)
        if contract is None or task_name not in _registered_task_names():
            raise HTTPException(status_code=404, detail=f"任务 {task_name} 未注册")

        kwargs: dict[str, Any] = dict(payload or {})
        if contract.schema is not None:
            try:
                validated = contract.schema.model_validate(kwargs)
            except ValidationError as exc:
                raise HTTPException(status_code=422, detail=exc.errors()) from exc
            kwargs = validated.model_dump()

        async_result = celery_app.send_task(task_name, kwargs=kwargs)
        return {
            "task_id": async_result.id,
            "task_name": task_name,
            "state": async_result.state,
        }

    @app.get(
        "/api/tasks/{task_name}/result/{task_id}",
        tags=["tasks"],
        summary="查询任务执行结果",
    )
    def get_task_result(task_name: str, task_id: str) -> dict[str, Any]:
        """Poll the execution state and result of one dispatched task.

        Args:
            task_name: Canonical task name the task was dispatched under.
            task_id: Celery task id returned by the dispatch endpoint.

        Returns:
            Dictionary with ``state``, ``ready`` and either the final
            ``result`` or the ``progress`` metadata published so far.
        """
        result = celery_app.AsyncResult(task_id)
        payload: dict[str, Any] = {
            "task_id": task_id,
            "task_name": task_name,
            "state": result.state,
            "ready": result.ready(),
        }
        if result.ready():
            payload["ok"] = result.successful()
            payload["result"] = (
                result.result if result.successful() else str(result.result)
            )
        else:
            payload["progress"] = (
                result.info if isinstance(result.info, dict) else None
            )
        return payload

    @app.get(
        "/api/tasks/{task_name}/eligible-count",
        tags=["tasks"],
        summary="查询某年符合条件的参与人数",
    )
    def eligible_count(task_name: str, year: int) -> dict[str, Any]:
        """Count the students eligible for one simulation task in a year.

        The eligibility rules mirror the WHERE conditions used by the
        corresponding simulation task:

        - ``tasks.simu.ncee``      — 未高考 (status 0) aged 17-19.
        - ``tasks.simu.admission`` — 已高考未入学 (status 10) with a score
          in the exam year.
        - ``tasks.simu.exam``      — 在读 (status 20) enrolled in
          ``[year-3, year]``.
        - ``tasks.simu.graduate``  — 在读 (status 20) enrolled in
          ``year-4``.

        Args:
            task_name: Canonical name of the simulation task.
            year: Simulation year (e.g. exam / graduation year).

        Returns:
            Dictionary with ``task_name``, ``year`` and ``eligible`` count.

        Raises:
            HTTPException: 404 when the task is unknown or not a simulation
                task with a count rule.
        """
        rule = _ELIGIBLE_COUNT_RULES.get(task_name)
        if rule is None:
            raise HTTPException(
                status_code=404,
                detail=f"任务 {task_name} 没有参与人数统计规则",
            )
        where, params = rule(year)
        try:
            with SCDBMySQLSpeed(web_db_meta()) as db:
                row = db.fetch_one(
                    f"SELECT COUNT(*) FROM {where}",
                    params,
                )
        except SCDBError as exc:
            raise HTTPException(
                status_code=503, detail=f"业务库查询失败：{exc}"
            ) from exc
        return {
            "task_name": task_name,
            "year": year,
            "eligible": int(row[0]) if row else 0,
        }

    @app.get("/healthz", tags=["ops"], summary="健康检查")
    def healthz() -> dict[str, str]:
        """Report service liveness.

        Returns:
            Dictionary with the overall status.
        """
        return {"status": "ok"}

    return app


app = create_api_app()
