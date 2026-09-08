"""AI-powered tasks of alt_celery3: LLM-driven university data collection.

The tasks in this module talk to the SiliconFlow (硅基流动) Chat Completion
API through the OpenAI-compatible SDK (configured via ``API_KEY_GJLD`` /
``BASE_URL`` in ``.env``; see :mod:`app.config`).

``get_un_groups`` asks the model for a fixed number of universities together
with their major groups, expects a strict JSON array back, de-duplicates the
answer against the ``universities`` / ``major_groups`` tables of ``web_db``
(by name) and persists only the new rows through :mod:`scdb_mysql_speed`.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from openai import OpenAI
from scdb_mysql_speed import SCDBError, SCDBMySQLSpeed

from app import config
from app.celery_app import celery_app
from app.sclog_setup import get_logger
from app.tasks.db_tasks import web_db_meta

#: Default number of universities requested per run.
DEFAULT_UN_COUNT = 3

#: Parameterised INSERT for a new university row.
INSERT_UNIVERSITY_SQL = (
    "INSERT INTO universities (name, code, type, nature) VALUES (%s, %s, %s, %s)"
)

#: Parameterised INSERT for a new major-group row of a given university.
INSERT_MAJOR_GROUP_SQL = (
    "INSERT INTO major_groups (university_id, name, code) VALUES (%s, %s, %s)"
)

#: The fixed instruction baked into :func:`get_un_groups`. It pins the exact
#: JSON contract so the model's answer can be parsed deterministically.
_UN_GROUPS_PROMPT = (
    "请列举{count}所中国普通高等学校的信息，并给出每所学校下属的若干个专业组。"
    "严格只输出一个 JSON 数组，不要输出任何其他文字、解释或 Markdown 代码块。"
    "数组元素格式如下：\n"
    "[\n"
    "  {{\n"
    '    "name": "高校名称",\n'
    '    "code": "高校代码，5位数字字符串",\n'
    '    "type": "高校分类，只能是 民办 或 公办",\n'
    '    "nature": "高校性质，只能是 985、211、一本 或 其他",\n'
    '    "majors": [\n'
    '      {{"name": "专业组名称", "code": "专业组代码，6位数字字符串"}}\n'
    "    ]\n"
    "  }}\n"
    "]\n"
    "要求：name 为真实存在的高校全称；code 为该校真实的教育部院校代码；"
    "majors 给出 3 个左右真实存在的专业（组）及其国标专业代码；"
    "type 从 民办/公办 中选择；nature 从 985/211/一本/其他 中选择。"
)


def gjld_chat_completion(question: str, *, model: str | None = None) -> str:
    """Ask the SiliconFlow Chat Completion API an arbitrary question.

    Thin, reusable wrapper around the OpenAI-compatible SDK: builds a client
    from ``API_KEY_GJLD`` / ``BASE_URL`` (see :mod:`app.config`) and returns
    the plain text answer of the model.

    Args:
        question: The user question sent to the model.
        model: Chat model to use; defaults to ``GJLD_MODEL`` from config.

    Returns:
        The assistant's reply as a plain string.

    Raises:
        Exception: Propagates SDK/network errors (auth failure, quota,
            timeouts) to the caller.
    """
    client = OpenAI(api_key=config.API_KEY_GJLD, base_url=config.BASE_URL)
    response = client.chat.completions.create(
        model=model or config.GJLD_MODEL,
        messages=[{"role": "user", "content": question}],
        temperature=0.2,
    )
    content = (response.choices[0].message.content or "").strip()
    # Reasoning models (e.g. DeepSeek-V4-Flash) may prefix a ``<think>…</think>``
    # trace; strip it so downstream JSON extraction only sees the answer.
    return re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()


def _extract_json_array(text: str) -> list[dict[str, Any]]:
    """Extract the first JSON array from a model reply.

    The model may wrap the payload in Markdown fences or add stray prose;
    this helper locates the outermost ``[...]`` block and parses it.

    Args:
        text: Raw assistant reply.

    Returns:
        The parsed JSON array (list of dictionaries).

    Raises:
        ValueError: If no JSON array can be located or parsing fails.
    """
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        raise ValueError("模型返回中未找到 JSON 数组")
    parsed = json.loads(match.group(0))
    if not isinstance(parsed, list):
        raise ValueError("模型返回的 JSON 不是数组")
    return parsed


@celery_app.task(name=config.TASK_GET_UN_GROUPS, bind=True)
def get_un_groups(self: Any, count: int = DEFAULT_UN_COUNT) -> dict[str, Any]:
    """Fetch universities + major groups from the LLM and store new rows.

    The question is fixed inside the task (see ``_UN_GROUPS_PROMPT``); the
    model is asked for ``count`` universities with a strict JSON contract.
    To keep the collection productive, the names already stored in
    ``web_db.universities`` are passed to the model as an exclusion list, so
    it returns universities that are *not* in the database yet. Existing rows
    are still looked up **by name** as a safety net — universities whose name
    already exists are skipped entirely (including their major groups), and
    major groups whose name already exists for that university are skipped as
    well. Only genuinely new rows are inserted, through
    :mod:`scdb_mysql_speed` parameterised statements.

    Args:
        count: Number of universities to request from the model (1..20).

    Returns:
        Dictionary with ``ok``, ``requested``, ``fetched``, dedup counters
        (``skipped_universities`` / ``skipped_major_groups``), insert counters
        (``inserted_universities`` / ``inserted_major_groups``), the stored
        ``universities`` payload and a UTC ``finished_at`` timestamp. On
        failure ``ok`` is ``False`` and ``error`` carries the message.
    """
    logger = get_logger()
    count = max(1, min(int(count), 20))

    def _progress(**meta: Any) -> None:
        """Best-effort ``PROGRESS`` state update for the polling front end.

        Args:
            **meta: Arbitrary progress metadata (phase, counters, ...).
        """
        try:
            self.update_state(state="PROGRESS", meta=meta)
        except Exception:  # noqa: BLE001 — progress reporting is best-effort.
            pass

    # --- step 1: ask the LLM --------------------------------------------------
    try:
        # Tell the model which universities are already stored so it returns
        # fresh ones; without this the model keeps returning the same famous
        # universities and every run ends up fully de-duplicated away.
        existing_names: list[str] = []
        try:
            with SCDBMySQLSpeed(web_db_meta()) as db:
                rows = db.fetch_all(
                    "SELECT name FROM universities ORDER BY id LIMIT 200",
                    result_format="dict",
                )
            existing_names = [str(row["name"]) for row in rows]
        except SCDBError as exc:
            logger.bind(component="get_un_groups").warning(
                f"读取已有高校清单失败，本次不做排除 error={exc}"
            )

        question = _UN_GROUPS_PROMPT.format(count=count)
        if existing_names:
            question += (
                "\n\n重要：以下高校已存在于数据库中，严禁在返回结果里出现，"
                "请返回其它高校：\n"
                + "、".join(existing_names)
            )
        _progress(
            phase="llm",
            processed=0,
            total=count,
            inserted_universities=0,
            inserted_major_groups=0,
        )
        reply = gjld_chat_completion(question)
        payload = _extract_json_array(reply)
    except (ValueError, json.JSONDecodeError) as exc:
        logger.bind(component="get_un_groups").error(f"模型返回解析失败 error={exc}")
        return {
            "ok": False,
            "error": f"模型返回解析失败：{exc}",
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    except Exception as exc:  # noqa: BLE001 — SDK/network errors become task results.
        logger.bind(component="get_un_groups").error(
            f"调用硅基流动 API 失败 error={exc}"
        )
        return {
            "ok": False,
            "error": f"调用硅基流动 API 失败：{exc}",
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    logger.bind(component="get_un_groups").info(
        f"模型返回 {len(payload)} 所高校，开始查重并入库 count={count}"
    )

    # --- step 2: de-duplicate by name and persist -----------------------------
    inserted_universities = 0
    inserted_major_groups = 0
    skipped_universities = 0
    skipped_major_groups = 0
    stored: list[dict[str, Any]] = []

    try:
        with SCDBMySQLSpeed(web_db_meta()) as db:
            for position, item in enumerate(payload, start=1):
                name = str(item.get("name", "")).strip()
                if not name:
                    continue

                # University-level dedup: by name.
                if db.fetch_one(
                    "SELECT id FROM universities WHERE name = %s", (name,)
                ):
                    skipped_universities += 1
                    logger.bind(component="get_un_groups").info(
                        f"高校已存在，跳过 name={name}"
                    )
                    _progress(
                        phase="db",
                        processed=position,
                        total=len(payload),
                        inserted_universities=inserted_universities,
                        inserted_major_groups=inserted_major_groups,
                    )
                    continue

                inserted_universities += db.execute(
                    INSERT_UNIVERSITY_SQL,
                    (
                        name,
                        str(item.get("code", ""))[:5],
                        str(item.get("type", ""))[:10],
                        str(item.get("nature", ""))[:10],
                    ),
                )
                university_id = int(
                    db.fetch_one(
                        "SELECT id FROM universities WHERE name = %s", (name,)
                    )[0]
                )
                stored.append(
                    {
                        "name": name,
                        "code": item.get("code"),
                        "type": item.get("type"),
                        "nature": item.get("nature"),
                        "majors": item.get("majors", []),
                    }
                )
                logger.bind(component="get_un_groups").info(
                    f"高校入库 name={name} id={university_id}"
                )

                # Major-group-level dedup: by name within this university.
                for major in item.get("majors", []) or []:
                    major_name = str(major.get("name", "")).strip()
                    if not major_name:
                        continue
                    if db.fetch_one(
                        "SELECT id FROM major_groups "
                        "WHERE university_id = %s AND name = %s",
                        (university_id, major_name),
                    ):
                        skipped_major_groups += 1
                        continue
                    inserted_major_groups += db.execute(
                        INSERT_MAJOR_GROUP_SQL,
                        (
                            university_id,
                            major_name,
                            str(major.get("code", ""))[:6],
                        ),
                    )

                _progress(
                    phase="db",
                    processed=position,
                    total=len(payload),
                    inserted_universities=inserted_universities,
                    inserted_major_groups=inserted_major_groups,
                )

    except SCDBError as exc:
        logger.bind(component="get_un_groups").error(
            f"高校数据入库失败 inserted_universities={inserted_universities} "
            f"error={exc}"
        )
        return {
            "ok": False,
            "requested": count,
            "fetched": len(payload),
            "inserted_universities": inserted_universities,
            "inserted_major_groups": inserted_major_groups,
            "error": str(exc),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    return _summarize(
        logger,
        count=count,
        payload=payload,
        stored=stored,
        inserted_universities=inserted_universities,
        inserted_major_groups=inserted_major_groups,
        skipped_universities=skipped_universities,
        skipped_major_groups=skipped_major_groups,
    )


def _summarize(
    logger: Any,
    *,
    count: int,
    payload: list[dict[str, Any]],
    stored: list[dict[str, Any]],
    inserted_universities: int,
    inserted_major_groups: int,
    skipped_universities: int,
    skipped_major_groups: int,
) -> dict[str, Any]:
    """Log the final outcome and build the success result dictionary.

    Args:
        logger: The sclog-lite application logger.
        count: Originally requested number of universities.
        payload: Parsed JSON items returned by the model.
        stored: University payloads that were actually inserted.
        inserted_universities: Number of new ``universities`` rows.
        inserted_major_groups: Number of new ``major_groups`` rows.
        skipped_universities: Universities skipped as duplicates.
        skipped_major_groups: Major groups skipped as duplicates.

    Returns:
        The success result dictionary for the Celery result backend.
    """
    logger.bind(
        component="get_un_groups",
        inserted_universities=inserted_universities,
        inserted_major_groups=inserted_major_groups,
    ).info(
        f"高校数据入库完成 新增高校={inserted_universities} "
        f"新增专业组={inserted_major_groups} 跳过高校={skipped_universities} "
        f"跳过专业组={skipped_major_groups}"
    )
    return {
        "ok": True,
        "requested": count,
        "fetched": len(payload),
        "inserted_universities": inserted_universities,
        "inserted_major_groups": inserted_major_groups,
        "skipped_universities": skipped_universities,
        "skipped_major_groups": skipped_major_groups,
        "universities": stored,
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
