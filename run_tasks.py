"""Producer / inspector CLI for the example tasks of alt_celery3.

Run it from the project root. Examples::

    python run_tasks.py add --x 2 --y 5      # send the plain addition task
    python run_tasks.py schedules            # list the configured beat schedule
    python run_tasks.py trigger-scheduled    # fire one periodic task by hand
    python run_tasks.py latest-scheduled     # fetch the latest scheduled result
    python run_tasks.py ping                 # ping every connected worker

Tasks are executed asynchronously by a worker over the Redis broker defined
in ``.env``. When no broker is available pass ``--eager`` (or export
``CELERY_TASK_ALWAYS_EAGER=true``) so tasks run in-process for quick demos.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import redis
from kombu.exceptions import OperationalError

from app import config
from app.celery_app import celery_app
from app.tasks import scheduled_tasks
from app.tasks.db_tasks import get_one_student, try_mysql
from app.tasks.example_tasks import add

#: Seconds this CLI is willing to wait for an asynchronous result.
RESULT_TIMEOUT = 60.0

_EPILOG = """\
commands:
  add                send the example addition task and wait for its result
  schedules          print the celery-beat schedule registered on this app
  trigger-scheduled  dispatch one periodic task immediately (simulates a beat
                     tick) so you can verify it end to end
  latest-scheduled   print the result of the most recent scheduled execution
                     (read from the Redis "last-run" pointer + result backend)
  try-mysql          test MySQL web_db connectivity (tasks.db.try_mysql)
  student            generate one student and save it to web_db
                     (tasks.db.get_one_student)
  ping               ping every running worker (broker connectivity smoke test)

Examples:
  python run_tasks.py add --x 40 --y 2
  python run_tasks.py trigger-scheduled --x 40 --y 2
  python run_tasks.py latest-scheduled
  python run_tasks.py try-mysql
  python run_tasks.py student
"""


def _poll_and_print(result: Any, title: str, timeout: float = RESULT_TIMEOUT) -> int:
    """Wait until an async result is ready and print it.

    Args:
        result: Celery ``AsyncResult`` (or eager result) to poll.
        title: Human readable label printed with the outcome.
        timeout: Maximum number of seconds to wait.

    Returns:
        Process exit code (0 on success, nonzero on failure/timeout).
    """
    deadline = time.monotonic() + timeout
    while not result.ready():
        if time.monotonic() > deadline:
            print(
                f"[timeout] {title} is still {result.state} after "
                f"{timeout:.0f}s - is a worker running against the same "
                "broker and result backend? Hint: "
                "`python run_celery.py worker --loglevel=INFO`."
            )
            return 3
        time.sleep(0.5)

    if result.successful():
        value = result.get(propagate=True)
        print(f"[ok] {title}: state={result.state}")
        if isinstance(value, dict):
            for key, item in value.items():
                print(f"    {key}: {item}")
        else:
            print(f"    value: {value!r}")
        date_done = getattr(result, "date_done", None)
        if date_done is not None:
            print(f"    date_done: {date_done}")
        return 0

    print(f"[failed] {title}: state={result.state}")
    traceback_text = getattr(result, "traceback", None)
    if traceback_text:
        print("---- task traceback ----")
        print(traceback_text)
    else:
        print("    no traceback available - check the worker logs.")
    return 1


def _report_broker_error(exc: Exception, action: str) -> int:
    """Print a broker/backend connectivity error with an actionable hint.

    Args:
        exc: The exception raised while connecting.
        action: Description of the failing operation.

    Returns:
        Process exit code 2.
    """
    print(
        f"[error] could not {action}: {exc}\n"
        "Make sure a password-protected redis-stack server is reachable and "
        "configured in `.env` (see `.env.example`)."
    )
    return 2


def cmd_add(args: argparse.Namespace) -> int:
    """Send the example plain task ``tasks.example.add`` and wait for its result.

    Args:
        args: Parsed command line arguments (x, y).

    Returns:
        Process exit code.
    """
    kwargs = {"x": args.x, "y": args.y}
    print(f"[send] {config.TASK_EXAMPLE_ADD} kwargs={kwargs}")
    try:
        result = add.apply_async(kwargs=kwargs)
    except OperationalError as exc:
        return _report_broker_error(exc, f"send {config.TASK_EXAMPLE_ADD}")
    print(f"[sent] request id={result.id} - waiting for the worker ...")
    return _poll_and_print(result, config.TASK_EXAMPLE_ADD)


def _dispatch_task(task_obj: Any, task_name: str, kwargs: dict[str, Any]) -> int:
    """Dispatch a task object (eager or via broker) and wait for its result.

    Args:
        task_obj: The bound Celery task to dispatch.
        task_name: Canonical task name used for messages.
        kwargs: Keyword arguments forwarded to the task.

    Returns:
        Process exit code.
    """
    if celery_app.conf.task_always_eager:
        result = task_obj.apply(kwargs=kwargs)
    else:
        try:
            result = task_obj.apply_async(kwargs=kwargs)
        except OperationalError as exc:
            return _report_broker_error(exc, f"send {task_name}")
    print(f"[sent] {task_name} kwargs={kwargs} request id={result.id}")
    return _poll_and_print(result, task_name)


def cmd_try_mysql(args: argparse.Namespace) -> int:
    """Send the ``tasks.db.try_mysql`` connectivity task and wait for it.

    Args:
        args: Parsed command line arguments (unused).

    Returns:
        Process exit code.
    """
    return _dispatch_task(try_mysql, config.TASK_TRY_MYSQL, {})


def cmd_get_one_student(args: argparse.Namespace) -> int:
    """Send the ``tasks.db.get_one_student`` task and wait for it.

    Args:
        args: Parsed command line arguments (unused).

    Returns:
        Process exit code.
    """
    return _dispatch_task(get_one_student, config.TASK_GET_ONE_STUDENT, {})


def cmd_schedules(args: argparse.Namespace) -> int:
    """Print every periodic task registered in the beat schedule.

    Args:
        args: Parsed command line arguments (unused).

    Returns:
        Process exit code.
    """
    entries = celery_app.conf.beat_schedule
    if not entries:
        print(
            "The beat schedule is empty. Enable the example entry with "
            "CELERY_ENABLE_EXAMPLE_BEAT=true and restart the application."
        )
        return 0
    print(f"{len(entries)} periodic entr(y/ies) registered on {config.APP_NAME}:")
    for name, entry in entries.items():
        print(f"- {name}")
        print(f"    task:     {entry.get('task')}")
        print(f"    schedule: {entry.get('schedule')!r}")
        entry_kwargs = entry.get("kwargs")
        if entry_kwargs:
            print(f"    kwargs:   {entry_kwargs}")
    print(
        "\nPeriodic tasks are dispatched automatically while celery beat runs "
        "(`python run_celery.py beat` or the `beat` Docker service)."
    )
    return 0


def cmd_trigger_scheduled(args: argparse.Namespace) -> int:
    """Dispatch one periodic task immediately, mimicking a single beat tick.

    Args:
        args: Parsed command line arguments (task, optional x/y overrides).

    Returns:
        Process exit code.
    """
    task_name = args.task or config.TASK_SCHEDULED_ADD
    base_kwargs: dict[str, Any] = {"x": 21, "y": 21}
    for entry in celery_app.conf.beat_schedule.values():
        if entry.get("task") == task_name and entry.get("kwargs"):
            base_kwargs = dict(entry["kwargs"])  # type: ignore[arg-type]
            break

    kwargs = {
        "x": args.x if args.x is not None else base_kwargs.get("x", 21),
        "y": args.y if args.y is not None else base_kwargs.get("y", 21),
    }
    print(f"[send] periodic task {task_name} kwargs={kwargs}")
    if celery_app.conf.task_always_eager:
        # In eager mode ``send_task`` is ignored by Celery; run the task
        # in-process instead so offline demos behave like a real dispatch.
        result = scheduled_tasks.scheduled_add.apply(kwargs=kwargs)
    else:
        try:
            result = celery_app.send_task(task_name, kwargs=kwargs)
        except OperationalError as exc:
            return _report_broker_error(exc, f"send {task_name}")
    print(f"[sent] request id={result.id} - waiting for the worker ...")
    print("(celery beat dispatches this task on its own; this run is manual.)")
    return _poll_and_print(result, f"manual {task_name}")


def cmd_latest_scheduled(args: argparse.Namespace) -> int:
    """Print the result of the most recent scheduled execution of a task.

    Args:
        args: Parsed command line arguments (task name).

    Returns:
        Process exit code.
    """
    task_name = args.task or config.TASK_SCHEDULED_ADD
    try:
        task_id = scheduled_tasks.fetch_last_run(task_name)
    except redis.exceptions.RedisError as exc:
        print(f"[error] result backend unreachable: {exc}")
        return 2

    if task_id is None:
        print(
            f"No recorded execution of {task_name} yet.\n"
            "Start beat (`python run_celery.py beat` or "
            "`docker compose up -d beat`) and wait for the next tick, or "
            "dispatch one manually:\n"
            "    python run_tasks.py trigger-scheduled"
        )
        return 0

    result = celery_app.AsyncResult(task_id)
    print(f"[found] {task_name} last request id={task_id}, state={result.state}")
    return _poll_and_print(result, f"last scheduled run of {task_name}")


def cmd_ping(args: argparse.Namespace) -> int:
    """Ping all workers connected to the broker (smoke test).

    Args:
        args: Parsed command line arguments (unused).

    Returns:
        Process exit code.
    """
    try:
        responses = celery_app.control.ping(timeout=10)
    except OperationalError as exc:
        return _report_broker_error(exc, "ping the workers")
    if not responses:
        print("[warn] no worker answered - start one first.")
        return 1
    print(f"{len(responses)} worker(s) answered:")
    for response in responses:
        for hostname, answer in response.items():
            print(f"- {hostname}: {answer}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser.

    Returns:
        A configured ``argparse.ArgumentParser``.
    """
    parser = argparse.ArgumentParser(
        prog="run_tasks",
        description="Call the alt_celery3 example tasks and inspect their "
        "(including scheduled) results.",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--eager",
        action="store_true",
        help="run tasks locally without a broker/worker (offline demos/tests)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_add = subparsers.add_parser("add", help="send the addition example task")
    parser_add.add_argument("--x", type=int, default=2, help="first addend (default: 2)")
    parser_add.add_argument("--y", type=int, default=5, help="second addend (default: 5)")
    parser_add.set_defaults(func=cmd_add)

    parser_schedules = subparsers.add_parser(
        "schedules", help="show the configured beat schedule"
    )
    parser_schedules.set_defaults(func=cmd_schedules)

    parser_trigger = subparsers.add_parser(
        "trigger-scheduled", help="dispatch a periodic task once, by hand"
    )
    parser_trigger.add_argument(
        "--task",
        default=config.TASK_SCHEDULED_ADD,
        help=f"periodic task name (default: {config.TASK_SCHEDULED_ADD})",
    )
    parser_trigger.add_argument("--x", type=int, default=None, help="override addend x")
    parser_trigger.add_argument("--y", type=int, default=None, help="override addend y")
    parser_trigger.set_defaults(func=cmd_trigger_scheduled)

    parser_latest = subparsers.add_parser(
        "latest-scheduled", help="show the latest scheduled execution result"
    )
    parser_latest.add_argument(
        "--task",
        default=config.TASK_SCHEDULED_ADD,
        help=f"periodic task name (default: {config.TASK_SCHEDULED_ADD})",
    )
    parser_latest.set_defaults(func=cmd_latest_scheduled)

    parser_ping = subparsers.add_parser("ping", help="ping all connected workers")
    parser_ping.set_defaults(func=cmd_ping)

    parser_try_mysql = subparsers.add_parser(
        "try-mysql", help="test MySQL web_db connectivity"
    )
    parser_try_mysql.set_defaults(func=cmd_try_mysql)

    parser_student = subparsers.add_parser(
        "student", help="generate one student and save it to web_db"
    )
    parser_student.set_defaults(func=cmd_get_one_student)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Command line entry point.

    Args:
        argv: Arguments without the program name (``None`` uses ``sys.argv``).

    Returns:
        Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.eager:
        celery_app.conf.update(task_always_eager=True, task_eager_propagates=True)
        print("Eager mode: tasks execute locally without broker or worker.")
    try:
        return int(args.func(args) or 0)
    finally:
        # sclog-lite writes to MySQL asynchronously; always flush before the
        # producer process exits (same contract as the worker shutdown hook).
        from app.sclog_setup import shutdown_logging

        shutdown_logging(timeout=10.0)


if __name__ == "__main__":
    sys.exit(main())
