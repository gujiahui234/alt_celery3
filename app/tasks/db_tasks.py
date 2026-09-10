"""MySQL-related tasks of alt_celery3: connectivity probe and student generator.

``try_mysql`` checks that the ``web_db`` MySQL server configured through the
``MYSQL_WEB_*`` environment variables is reachable (via the
``scdb-mysql-speed`` pool wrapper). ``get_one_student`` simulates one student
with the ``class_roster`` package and persists it into the ``students`` table
of ``web_db``; ``gender`` is stored as the single-letter code (M/F) and every
newly generated student starts in the 未高考 (0) enrollment state.

Both tasks record what they do through the sclog-lite application logger
(see :mod:`app.sclog_setup`), so every operation is visible on the console,
in the rotating file log and in the asynchronous MySQL log backend.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from scdb_mysql_speed import (
    SCDBError,
    SCDBMySQLMeta,
    SCDBMySQLSpeed,
)

from app import config
from app.celery_app import celery_app
from app.sclog_setup import get_logger

#: ``students`` table DDL — the canonical business schema. ``gender`` stores
#: the single-letter code (``M``=男, ``F``=女) and ``enrollment_status`` tracks
#: the admission lifecycle (0=未高考, 10=已高考未入学, 20=在读, 30=已毕业).
#: Secondary indexes keep common filters fast when the table grows to tens of
#: millions of rows (status-driven lists, gender/birthday demographics).
STUDENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS students (
    id                BIGINT       NOT NULL AUTO_INCREMENT  COMMENT '主键',
    name              VARCHAR(50)  NOT NULL                 COMMENT '姓名',
    birthday          DATE         NOT NULL                 COMMENT '出生日期',
    gender            CHAR(1)      NOT NULL                 COMMENT '性别: M=男, F=女',
    enrollment_status TINYINT      NOT NULL DEFAULT 0       COMMENT '入学状态: 0=未高考, 10=已高考未入学, 20=在读, 30=已毕业',
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_students_status (enrollment_status),
    KEY idx_students_gender_birthday (gender, birthday),
    KEY idx_students_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='学生信息表'
"""

#: Parameterised INSERT used to persist a generated student. Newly generated
#: students always start in the 未高考 (``0``) enrollment state.
INSERT_STUDENT_SQL = (
    "INSERT INTO students (name, gender, birthday, enrollment_status) "
    "VALUES (%s, %s, %s, %s)"
)

#: Mapping from ``class_roster`` gender labels to the storage codes.
_GENDER_CODES = {"男": "M", "女": "F"}


def gender_code(gender: str) -> str:
    """Convert a ``class_roster`` gender label into the storage code.

    Args:
        gender: Gender label produced by ``class_roster`` (``男`` or ``女``).

    Returns:
        ``M`` for 男, ``F`` for 女.

    Raises:
        ValueError: When the label is neither 男 nor 女.
    """
    try:
        return _GENDER_CODES[gender]
    except KeyError as error:
        raise ValueError(f"未知的性别标签：{gender!r}") from error


#: ``students.enrollment_status`` values (入学状态生命周期).
ENROLLMENT_STATUS_NONE = 0       # 未高考
ENROLLMENT_STATUS_EXAMINED = 10  # 已高考未入学
ENROLLMENT_STATUS_ENROLLED = 20  # 在读
ENROLLMENT_STATUS_GRADUATED = 30  # 已毕业


def web_db_meta() -> SCDBMySQLMeta:
    """Build the immutable connection metadata for ``web_db``.

    Returns:
        A :class:`scdb_mysql_speed.SCDBMySQLMeta` built from the
        ``MYSQL_WEB_*`` settings in :mod:`app.config` (sourced from ``.env``).
    """
    return SCDBMySQLMeta(
        host=config.WEB_DB_HOST,
        port=config.WEB_DB_PORT,
        user=config.WEB_DB_USER,
        password=config.WEB_DB_PASSWORD,
        database=config.WEB_DB_DATABASE,
    )


@celery_app.task(name=config.TASK_TRY_MYSQL)
def try_mysql() -> dict[str, Any]:
    """Test the connectivity of the MySQL ``web_db`` database.

    Opens a pooled connection through :class:`scdb_mysql_speed.SCDBMySQLSpeed`
    and runs its ``SELECT 1`` smoke test, timing the round trip. The outcome —
    success or failure — is recorded with the sclog-lite logger.

    Returns:
        Dictionary with ``ok``, connection metadata (without credentials),
        the measured ``latency_ms`` and a UTC ``checked_at`` timestamp. On
        failure ``ok`` is ``False`` and ``error`` carries the message.
    """
    logger = get_logger()
    started = time.perf_counter()
    try:
        with SCDBMySQLSpeed(web_db_meta()) as db:
            ok = db.test_connection()
    except SCDBError as exc:
        logger.bind(component="try_mysql").error(
            f"web_db 连通性测试失败 host={config.WEB_DB_HOST}:{config.WEB_DB_PORT} "
            f"database={config.WEB_DB_DATABASE} error={exc}"
        )
        return {
            "ok": False,
            "host": config.WEB_DB_HOST,
            "port": config.WEB_DB_PORT,
            "database": config.WEB_DB_DATABASE,
            "error": str(exc),
            "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    latency_ms = round((time.perf_counter() - started) * 1000, 2)
    logger.bind(component="try_mysql", ok=ok).info(
        f"web_db 连通性测试成功 host={config.WEB_DB_HOST}:{config.WEB_DB_PORT} "
        f"database={config.WEB_DB_DATABASE} latency_ms={latency_ms}"
    )
    return {
        "ok": ok,
        "host": config.WEB_DB_HOST,
        "port": config.WEB_DB_PORT,
        "database": config.WEB_DB_DATABASE,
        "latency_ms": latency_ms,
        "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


@celery_app.task(name=config.TASK_GET_ONE_STUDENT)
def get_one_student() -> dict[str, Any]:
    """Generate one simulated student and save it into ``web_db.students``.

    The student comes from :func:`class_roster.simulation.simulate_class`
    (a randomised Chinese class roster: 学号/姓名/性别/出生日期). The task
    ensures the ``students`` table exists, inserts the row with a
    parameterised statement (no string concatenation, per the
    ``scdb-mysql-speed`` security contract) and logs each step with sclog.

    Returns:
        Dictionary with ``ok``, the stored ``student`` fields and the number
        of ``affected_rows``. On failure ``ok`` is ``False`` and ``error``
        carries the message.
    """
    from class_roster.simulation import simulate_class

    logger = get_logger()
    student = simulate_class(size=1).students[0]
    gender = gender_code(student.gender)
    logger.bind(component="get_one_student").info(
        f"生成学生成功 name={student.name} gender={gender} "
        f"birthday={student.birthday.isoformat()}"
    )

    try:
        with SCDBMySQLSpeed(web_db_meta()) as db:
            db.execute(STUDENTS_TABLE_SQL)
            affected = db.execute(
                INSERT_STUDENT_SQL,
                (student.name, gender, student.birthday, ENROLLMENT_STATUS_NONE),
            )
    except SCDBError as exc:
        logger.bind(component="get_one_student").error(
            f"学生保存失败 name={student.name} database="
            f"{config.WEB_DB_DATABASE} error={exc}"
        )
        return {
            "ok": False,
            "student": {
                "name": student.name,
                "gender": gender,
                "birthday": student.birthday.isoformat(),
                "enrollment_status": ENROLLMENT_STATUS_NONE,
            },
            "error": str(exc),
            "saved_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    logger.bind(component="get_one_student", affected_rows=affected).info(
        f"学生已保存到 {config.WEB_DB_DATABASE}.students name={student.name}"
    )
    return {
        "ok": True,
        "student": {
            "name": student.name,
            "gender": gender,
            "birthday": student.birthday.isoformat(),
            "enrollment_status": ENROLLMENT_STATUS_NONE,
        },
        "affected_rows": affected,
        "saved_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
