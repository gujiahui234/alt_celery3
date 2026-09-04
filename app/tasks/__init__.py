"""Task registry sub-package of alt_celery3.

Guideline: keep the *subject* of every task in its own module inside this
folder (e.g. ``example_tasks.py`` for getting-started examples) and register
new modules either in the ``include`` list of :mod:`app.celery_app` or by
importing them here.
"""

from app.tasks import db_tasks, example_tasks, scheduled_tasks  # noqa: F401
