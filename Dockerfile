# syntax=docker/dockerfile:1.7
# ============================================================================
# alt_celery3 image.
#
# One image is shared by the worker, beat and flower services (see
# docker-compose.yml). Only the command changes between services.
#
# Runtime requirements:
#   * Python >= 3.13
#   * A reachable, password-protected redis-stack broker configured through
#     CELERY_BROKER_URL / CELERY_RESULT_BACKEND (see .env.example).
# ============================================================================
FROM python:3.13-slim

# --- Global environment -------------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=UTC

WORKDIR /srv/alt_celery3

# --- OS packages & the dedicated non-privileged runtime user -----------------
# ``passwd`` provides useradd/groupadd on the slim Debian base image.
# ``git`` is required by pip for the git+https custom-package dependencies
# in requirements.txt (scdb-mysql-speed / class-roster-simulator / sclog-lite).
# ``build-essential + pkg-config + default-libmysqlclient-dev`` are needed to
# compile the ``mysqlclient`` wheel that scdb-mysql-speed depends on.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates passwd tzdata git \
        build-essential pkg-config default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 celeuser \
    && useradd \
        --system \
        --uid 10001 \
        --gid 10001 \
        --home-dir /home/celeuser \
        --create-home \
        --shell /usr/sbin/nologin \
        celeuser

# --- Python dependencies (cached unless the manifests/wheels change) ---------
# requirements-docker.txt lists the same packages as requirements.txt, but the
# GitHub-hosted custom packages come from the committed wheelhouse/ directory
# instead of git+https URLs: docker01's build network cannot reach github.com
# reliably, while PyPI works. --find-links resolves them from local wheels.
COPY requirements-docker.txt ./
# NOTE: `COPY wheelhouse ./` would flatten the directory contents into
# WORKDIR; the explicit target keeps them under ./wheelhouse.
COPY wheelhouse ./wheelhouse
RUN pip install --no-cache-dir --find-links=/srv/alt_celery3/wheelhouse \
        -r requirements-docker.txt

# --- Application code (owned by the non-privileged runtime user) -------------
COPY --chown=celeuser:celeuser . .

# Persistent volume mount point used by celery beat for its schedule file.
RUN mkdir -p /data && chown -R celeuser:celeuser /data

# Never run the containers as root.
USER celeuser

# Default command: start a worker. Overridden per service in docker-compose.yml.
CMD ["celery", "-A", "app.celery_app", "worker", "--loglevel=INFO"]
