"""Local entry point for the alt_celery3 tasks API service.

Starts the FastAPI service on the host machine::

    python run_api.py

Host/port come from ``API_HOST`` / ``API_PORT`` in ``.env``
(defaults: ``0.0.0.0:8012``).
"""

from __future__ import annotations

import uvicorn

from app import config


def main() -> None:
    """Run the ASGI server with the configured host and port."""
    uvicorn.run(
        "app.api:app",
        host=config.API_HOST,
        port=config.API_PORT,
    )


if __name__ == "__main__":
    main()
