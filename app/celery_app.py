"""Shared Celery application instance for alt_celery3.

Workers, beat and producers all use this single object
(``celery -A app.celery_app ...``) so the broker, the result backend, the
serializers and the beat schedule are configured exactly once.
"""

from __future__ import annotations

from celery import Celery
from celery.signals import worker_process_init, worker_shutdown

from app import config

#: The one Celery application to rule them all.
celery_app = Celery(
    config.APP_NAME,
    broker=config.BROKER_URL,
    backend=config.RESULT_BACKEND,
    include=[
        # Task modules imported together with the application so tasks can be
        # addressed both by reference and by their canonical name.
        "app.tasks.example_tasks",
        "app.tasks.scheduled_tasks",
        "app.tasks.db_tasks",
        "app.tasks.bulk_student_tasks",
    ],
)

celery_app.conf.update(
    # JSON keeps tasks interoperable (e.g. with producers written in other
    # languages) and avoids pickle-related security issues.
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Timezone handling for timestamps and beat schedules.
    timezone=config.TIMEZONE,
    enable_utc=config.ENABLE_UTC,
    # Automatic cleanup of finished task results in the backend.
    result_expires=config.RESULT_EXPIRES,
    result_persistent=False,
    task_track_started=True,
    # Retry the initial connection when a worker starts before Redis is up.
    broker_connection_retry_on_startup=True,
    # Periodic task definitions (see app.config.build_beat_schedule()).
    beat_schedule=config.build_beat_schedule(),
    beat_schedule_filename=config.BEAT_SCHEDULE_FILE,
    # Optional in-process execution used for offline demos and tests.
    task_always_eager=config.TASK_ALWAYS_EAGER,
    task_eager_propagates=config.TASK_EAGER_PROPAGATES,
)


# --- sclog-lite logging lifecycle (application log middleware) ---------------
# Follows the sclog-lite scaffold contract: set the logger up when the
# application starts and always flush it with shutdown() on teardown. Each
# preforked worker process initialises its own logger via worker_process_init.

@worker_process_init.connect
def _setup_sclog_on_worker_init(**_kwargs: object) -> None:
    """Initialise the sclog-lite logger in every worker process."""
    from app.sclog_setup import get_logger

    get_logger().bind(component="celery-worker").info("worker process initialized")


@worker_shutdown.connect
def _shutdown_sclog_on_worker_exit(**_kwargs: object) -> None:
    """Flush pending sclog-lite batches (e.g. MySQL writes) on shutdown."""
    from app.sclog_setup import shutdown_logging

    shutdown_logging(timeout=10.0)
