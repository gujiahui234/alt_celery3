"""One-off script: create the universities / major_groups tables in web_db."""

from app.tasks.db_tasks import web_db_meta
from scdb_mysql_speed import SCDBMySQLSpeed

DDL_UNIVERSITIES = """
CREATE TABLE IF NOT EXISTS universities (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL COMMENT '高校名称',
    code CHAR(5) NOT NULL DEFAULT '' COMMENT '高校代码',
    type VARCHAR(10) NOT NULL DEFAULT '' COMMENT '高校分类(民办/公办)',
    nature VARCHAR(10) NOT NULL DEFAULT '' COMMENT '高校性质(985/211/一本/其他)',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_universities_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

DDL_MAJOR_GROUPS = """
CREATE TABLE IF NOT EXISTS major_groups (
    id BIGINT NOT NULL AUTO_INCREMENT,
    university_id BIGINT NOT NULL COMMENT '所属高校 id',
    name VARCHAR(100) NOT NULL COMMENT '专业组名称',
    code CHAR(6) NOT NULL DEFAULT '' COMMENT '专业组代码(6位国标)',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_major_groups_university (university_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def main() -> None:
    with SCDBMySQLSpeed(web_db_meta()) as db:
        db.execute(DDL_UNIVERSITIES)
        db.execute(DDL_MAJOR_GROUPS)
        print("universities:", db.fetch_one("SELECT COUNT(*) FROM universities")[0])
        print("major_groups:", db.fetch_one("SELECT COUNT(*) FROM major_groups")[0])
    print("表创建完成")


if __name__ == "__main__":
    main()
