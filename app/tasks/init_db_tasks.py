"""Database initialisation task of alt_celery3 (``tasks.db.init_web_db``).

``init_web_db`` rebuilds the two application databases from scratch:

1. Drop the old ``web_db`` / ``log_db`` databases and the ``web_user`` /
   ``log_user`` accounts when they exist.
2. Recreate ``web_db`` (managed by ``web_user``) and ``log_db`` (managed by
   ``log_user``) with ``utf8mb4``.
3. Create the business tables inside ``web_db`` — existing tables are kept
   (``CREATE TABLE IF NOT EXISTS``) and the ``students`` table is migrated
   in place when the ``enrollment_status`` column is still missing.

The drop/create steps require a privileged MySQL account whose credentials
come from ``MYSQL_ADMIN_USER`` / ``MYSQL_ADMIN_PASSWORD`` in ``.env``. The
business tables carry secondary indexes tuned for tens of millions of
``students`` rows (status-driven lists, gender/birthday demographics,
per-student score lookups), and every ``INSERT`` relies on the
``AUTO_INCREMENT`` primary key for row identity.

All operations are recorded through the sclog-lite application logger
(see :mod:`app.sclog_setup`): console, rotating file and the asynchronous
MySQL log backend.
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from scdb_mysql_speed import SCDBError, SCDBMySQLMeta, SCDBMySQLSpeed

from app import config
from app.celery_app import celery_app
from app.sclog_setup import get_logger
from app.tasks.db_tasks import STUDENTS_TABLE_SQL, web_db_meta

#: Validates identifier names (databases / users) interpolated into DDL.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")

#: Default schema for connections that must exist *before* the application
#: databases exist. MySQLdb fails the handshake on an empty default database,
#: so admin/log-owner connections anchor on the always-present system schema
#: and qualify every statement explicitly.
_ADMIN_DEFAULT_SCHEMA = "mysql"

#: ``universities`` table DDL — unique name/code keys keep LLM-collected
#: rows deduplicated at the storage layer.
UNIVERSITIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS universities (
    id          BIGINT        NOT NULL AUTO_INCREMENT  COMMENT '主键',
    name        VARCHAR(100)  NOT NULL                 COMMENT '高校名称',
    code        CHAR(10)      NOT NULL                 COMMENT '高校代码（五位数字）',
    type        VARCHAR(10)   NOT NULL                 COMMENT '高校类型：民办/公办',
    nature      VARCHAR(10)   NOT NULL                 COMMENT '高校性质：985/211/一本/其他',
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_name (name),
    UNIQUE KEY uk_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='高校信息表'
"""

#: ``major_groups`` table DDL — scoped to one university, cascade-deleted
#: together with its owning university.
MAJOR_GROUPS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS major_groups (
    id              BIGINT        NOT NULL AUTO_INCREMENT  COMMENT '主键',
    university_id   BIGINT        NOT NULL                 COMMENT '所属高校 ID',
    name            VARCHAR(100)  NOT NULL                 COMMENT '专业组名称',
    code            CHAR(10)      NOT NULL                 COMMENT '专业组代码（五位数字）',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_uni_name (university_id, name),
    UNIQUE KEY uk_uni_code (university_id, code),
    CONSTRAINT fk_major_groups_universities
        FOREIGN KEY (university_id) REFERENCES universities (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='专业组信息表'
"""

#: ``gaokao_scores`` table DDL — one college-entrance exam per student, so
#: the unique key doubles as the foreign-key lookup index.
GAOKAO_SCORES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gaokao_scores (
    id          BIGINT   NOT NULL AUTO_INCREMENT COMMENT '主键',
    student_id  BIGINT   NOT NULL                COMMENT '学生 ID (students.id)',
    score       INT      NOT NULL                COMMENT '高考总分',
    exam_date   DATE     NOT NULL                COMMENT '高考日期',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_gaokao_student (student_id),
    KEY idx_gaokao_exam_date (exam_date),
    CONSTRAINT fk_gaokao_scores_students
        FOREIGN KEY (student_id) REFERENCES students (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='学生高考成绩表'
"""

#: ``undergraduate_scores`` table DDL — one row per student, academic year
#: and subject; the composite index serves per-student transcript reads.
UNDERGRADUATE_SCORES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS undergraduate_scores (
    id             BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    student_id     BIGINT       NOT NULL                COMMENT '学生 ID (students.id)',
    academic_year  SMALLINT     NOT NULL                COMMENT '学年，如 2024 表示 2024-2025 学年',
    subject        VARCHAR(30)  NOT NULL                COMMENT '考试科目/课程名称',
    score          DECIMAL(5,1) NOT NULL                COMMENT '考试成绩',
    exam_date      DATE         NOT NULL                COMMENT '考试日期',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_ug_student_year (student_id, academic_year),
    KEY idx_ug_exam_date (exam_date),
    CONSTRAINT fk_undergraduate_scores_students
        FOREIGN KEY (student_id) REFERENCES students (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='学生本科成绩表'
"""

#: ``graduation_scores`` table DDL — one graduation record per student.
GRADUATION_SCORES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS graduation_scores (
    id              BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    student_id      BIGINT       NOT NULL                COMMENT '学生 ID (students.id)',
    gpa             DECIMAL(4,2) NOT NULL                COMMENT '平均绩点',
    graduation_date DATE         NOT NULL                COMMENT '毕业日期',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_grad_student (student_id),
    CONSTRAINT fk_graduation_scores_students
        FOREIGN KEY (student_id) REFERENCES students (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='学生毕业成绩表'
"""

#: ``enrollments`` table DDL — which student enrolled into which university
#: major group in which academic year.
ENROLLMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS enrollments (
    id             BIGINT    NOT NULL AUTO_INCREMENT COMMENT '主键',
    student_id     BIGINT    NOT NULL                COMMENT '学生 ID (students.id)',
    university_id  BIGINT    NOT NULL                COMMENT '入学高校 ID (universities.id)',
    major_group_id BIGINT    NOT NULL                COMMENT '入学专业组 ID (major_groups.id)',
    academic_year  SMALLINT  NOT NULL                COMMENT '入学学年，如 2024 表示 2024-2025 学年',
    created_at     DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_enroll_student_year (student_id, academic_year),
    KEY idx_enroll_university (university_id),
    KEY idx_enroll_major_group (major_group_id),
    KEY idx_enroll_year (academic_year),
    CONSTRAINT fk_enrollments_students
        FOREIGN KEY (student_id) REFERENCES students (id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_enrollments_universities
        FOREIGN KEY (university_id) REFERENCES universities (id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_enrollments_major_groups
        FOREIGN KEY (major_group_id) REFERENCES major_groups (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='学生-高校-专业组入学关系表'
"""

#: All business tables created inside ``web_db``, in dependency order.
BUSINESS_TABLE_SQL: dict[str, str] = {
    "students": STUDENTS_TABLE_SQL,
    "universities": UNIVERSITIES_TABLE_SQL,
    "major_groups": MAJOR_GROUPS_TABLE_SQL,
    "gaokao_scores": GAOKAO_SCORES_TABLE_SQL,
    "undergraduate_scores": UNDERGRADUATE_SCORES_TABLE_SQL,
    "graduation_scores": GRADUATION_SCORES_TABLE_SQL,
    "enrollments": ENROLLMENTS_TABLE_SQL,
}


def _validate_identifier(name: str, kind: str) -> str:
    """Validate a database/user identifier before interpolating it into DDL.

    DDL statements cannot be parameterised for identifiers, so every name
    that reaches a formatted statement must pass this whitelist first (the
    ``scdb-mysql-speed`` security contract forbids uncontrolled concatenation).

    Args:
        name: The identifier to validate.
        kind: Human-readable label used in the error message.

    Returns:
        The validated identifier, unchanged.

    Raises:
        ValueError: When the identifier contains characters outside
            ``[A-Za-z0-9_]``.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"非法的{kind}名称：{name!r}")
    return name


def _admin_meta() -> SCDBMySQLMeta:
    """Build a single-connection metadata for the admin account.

    Returns:
        A :class:`scdb_mysql_speed.SCDBMySQLMeta` anchored on the always
        present ``mysql`` system schema (a database is still required for the
        handshake) using the admin credentials from ``MYSQL_ADMIN_USER`` /
        ``MYSQL_ADMIN_PASSWORD``.

    Raises:
        RuntimeError: When the admin credentials are missing.
    """
    if not config.ADMIN_PASSWORD:
        raise RuntimeError(
            "MYSQL_ADMIN_PASSWORD 未配置，请在 .env 中提供数据库管理员密码"
        )
    return replace(
        web_db_meta(),
        user=config.ADMIN_USER,
        password=config.ADMIN_PASSWORD,
        database=_ADMIN_DEFAULT_SCHEMA,
        pool_size=1,
        pool_max_overflow=0,
    )


def _log_db_meta() -> SCDBMySQLMeta:
    """Build a single-connection metadata for the log database owner.

    Returns:
        A :class:`scdb_mysql_speed.SCDBMySQLMeta` for the log database owner
        connected to its own database (the only schema it can access), taking
        host/port/user/password from the sclog-lite MySQL backend settings in
        ``.env``.
    """
    return replace(
        web_db_meta(),
        user=config.SCLOG_MYSQL_USER,
        password=config.SCLOG_MYSQL_PASSWORD,
        database=config.SCLOG_MYSQL_DATABASE,
        pool_size=1,
        pool_max_overflow=0,
    )


def _column_exists(db: SCDBMySQLSpeed, table: str, column: str) -> bool:
    """Check whether a column already exists in a table of the current schema.

    Args:
        db: An open connection to the ``web_db`` database.
        table: Table name (validated identifier).
        column: Column name (validated identifier).

    Returns:
        ``True`` when the column exists, ``False`` otherwise.
    """
    row = db.fetch_one(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (table, column),
    )
    return bool(row and int(row[0]) > 0)


@celery_app.task(name=config.TASK_INIT_WEB_DB)
def init_web_db() -> dict[str, Any]:
    """Rebuild ``web_db`` / ``log_db`` and their owning users from scratch.

    Steps:
        1. Drop ``web_db`` / ``log_db`` and ``web_user`` / ``log_user`` when
           they exist (admin account).
        2. Recreate both databases with ``utf8mb4`` and re-create the owner
           accounts with the passwords from ``.env``, granting full
           privileges on their respective database only.
        3. As ``web_user``: create every business table with
           ``CREATE TABLE IF NOT EXISTS`` (existing tables are kept) and add
           the ``students.enrollment_status`` column when it is missing.
        4. Verify that ``log_user`` can connect to ``log_db``.

    Returns:
        Dictionary with ``ok``, the dropped/created databases and users, the
        created ``tables`` list, whether ``enrollment_status`` needed an
        in-place migration and a UTC ``finished_at`` timestamp. On failure
        ``ok`` is ``False`` and ``error`` carries the message.
    """
    logger = get_logger()
    web_db_name = _validate_identifier(config.WEB_DB_DATABASE, "数据库")
    web_user_name = _validate_identifier(config.WEB_DB_USER, "用户")
    log_db = _validate_identifier(config.SCLOG_MYSQL_DATABASE, "数据库")
    log_user = _validate_identifier(config.SCLOG_MYSQL_USER, "用户")

    try:
        # --- step 1+2: drop old databases/users, recreate them -----------------
        with SCDBMySQLSpeed(_admin_meta()) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {web_db_name}")
            admin.execute(f"DROP DATABASE IF EXISTS {log_db}")
            admin.execute(f"DROP USER IF EXISTS '{web_user_name}'@'%'")
            admin.execute(f"DROP USER IF EXISTS '{log_user}'@'%'")
            logger.bind(component="init_web_db").info(
                f"已删除旧数据库/用户 databases=[{web_db_name}, {log_db}] "
                f"users=[{web_user_name}, {log_user}]"
            )

            admin.execute(
                f"CREATE DATABASE {web_db_name} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            admin.execute(
                f"CREATE DATABASE {log_db} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            admin.execute(
                # ``%%`` escapes to a literal ``%`` because the statement is
                # parameterised (MySQLdb applies %-formatting when params are
                # given).
                f"CREATE USER '{web_user_name}'@'%%' IDENTIFIED BY %s",
                (config.WEB_DB_PASSWORD,),
            )
            admin.execute(
                f"GRANT ALL PRIVILEGES ON {web_db_name}.* "
                f"TO '{web_user_name}'@'%'"
            )
            admin.execute(
                f"CREATE USER '{log_user}'@'%%' IDENTIFIED BY %s",
                (config.SCLOG_MYSQL_PASSWORD,),
            )
            admin.execute(
                f"GRANT ALL PRIVILEGES ON {log_db}.* TO '{log_user}'@'%'"
            )
            admin.execute("FLUSH PRIVILEGES")
            logger.bind(component="init_web_db").info(
                f"数据库与用户已重建 databases=[{web_db_name}, {log_db}] "
                f"users=[{web_user_name}, {log_user}]"
            )

        # --- step 3: create the business tables as the web owner ---------------
        tables: list[str] = []
        enrollment_status_added = False
        with SCDBMySQLSpeed(web_db_meta()) as db:
            for table, ddl in BUSINESS_TABLE_SQL.items():
                db.execute(ddl)
                tables.append(table)
                logger.bind(component="init_web_db").info(
                    f"业务表就绪 table={table}"
                )
            if not _column_exists(db, "students", "enrollment_status"):
                db.execute(
                    "ALTER TABLE students "
                    "ADD COLUMN enrollment_status TINYINT NOT NULL DEFAULT 0 "
                    "COMMENT '入学状态: 0=未高考, 10=已高考未入学, 20=在读, 30=已毕业' "
                    "AFTER gender, "
                    "ADD INDEX idx_students_status (enrollment_status), "
                    "ADD INDEX idx_students_gender_birthday (gender, birthday)"
                )
                enrollment_status_added = True
                logger.bind(component="init_web_db").info(
                    "旧 students 表已原地迁移：新增 enrollment_status 列"
                )

        # --- step 4: verify the log account can reach its database -------------
        with SCDBMySQLSpeed(_log_db_meta()) as log_conn:
            log_conn.execute("SELECT 1")

        logger.bind(component="init_web_db", tables=tables).info(
            f"数据库初始化完成 web_db={web_db_name} log_db={log_db} "
            f"tables={len(tables)} enrollment_status_added={enrollment_status_added}"
        )
        return {
            "ok": True,
            "dropped_databases": [web_db_name, log_db],
            "dropped_users": [f"{web_user_name}@%", f"{log_user}@%"],
            "created_databases": [web_db_name, log_db],
            "created_users": [f"{web_user_name}@%", f"{log_user}@%"],
            "tables": tables,
            "enrollment_status_added": enrollment_status_added,
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    except SCDBError as exc:
        logger.bind(component="init_web_db").error(f"数据库初始化失败 error={exc}")
        return {
            "ok": False,
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    except (RuntimeError, ValueError) as exc:
        logger.bind(component="init_web_db").error(
            f"数据库初始化配置错误 error={exc}"
        )
        return {
            "ok": False,
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
