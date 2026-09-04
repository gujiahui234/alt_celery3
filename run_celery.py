"""Launch the alt_celery3 Celery application locally (no Docker required).

This script is a thin wrapper around ``celery -A app.celery_app`` and accepts
any sub-command of the Celery CLI. Examples (run from the project root)::

    python run_celery.py worker --loglevel=INFO --concurrency=2
    python run_celery.py beat --loglevel=INFO
    python run_celery.py flower --address=0.0.0.0 --port=5555
    python run_celery.py --help

Before running, copy ``.env.example`` to ``.env`` and set the broker and
result-backend URLs of the existing, password-protected redis-stack server.
Python >= 3.13 is required.
"""

from __future__ import annotations

import subprocess
import sys


def main(argv: list[str]) -> int:
    """Run the local Celery command with the given arguments forwarded.

    Args:
        argv: Command line arguments received from the user (program name
            excluded), e.g. ``["worker", "--loglevel=INFO"]``.

    Returns:
        Exit code of the underlying ``celery`` process.
    """
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(__doc__)
        return 0

    if argv[0] == "beat" and not any(a.startswith("--schedule") for a in argv):
        # Celery beat needs to write its persistent schedule file; make sure
        # the configured parent directory exists before starting it.
        from pathlib import Path

        from app import config

        Path(config.BEAT_SCHEDULE_FILE).expanduser().resolve().parent.mkdir(
            parents=True, exist_ok=True
        )

    command = [sys.executable, "-m", "celery", "-A", "app.celery_app", *argv]
    print("Running:", " ".join(command))
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
