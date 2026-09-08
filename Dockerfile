# ============================================================================
# NOTE: no `# syntax=` directive on purpose — pulling the dockerfile frontend
# image from Docker Hub is unnecessary for the features used here, and some
# build hosts cannot reach auth.docker.io at all (BuildKit falls back to its
# builtin frontend, which covers everything this Dockerfile uses).
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
# PIP_INDEX_URL points at the Tsinghua mirror: docker01's direct PyPI route is
# slow (tens of KB/s) and flaky. PIP_NO_CACHE_DIR is deliberately NOT set so
# the cache mount below can keep downloaded wheels between builds.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=60 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    TZ=UTC

WORKDIR /srv/alt_celery3

# --- OS packages & the dedicated non-privileged runtime user -----------------
# ``passwd`` provides useradd/groupadd on the slim Debian base image.
# ``git`` is required by pip for the git+https custom-package dependencies
# in requirements.txt (scdb-mysql-speed / class-roster-simulator / sclog-lite).
# ``build-essential + pkg-config + default-libmysqlclient-dev`` are needed to
# compile the ``mysqlclient`` wheel that scdb-mysql-speed depends on.
# The apt caches live in BuildKit cache mounts so repeat builds skip the
# package downloads entirely.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates passwd tzdata git \
        build-essential pkg-config default-libmysqlclient-dev \
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
# reliably, while the mirror works. --find-links resolves them from local
# wheels; the pip cache mount keeps every downloaded wheel across builds.
COPY requirements-docker.txt ./
# NOTE: `COPY wheelhouse ./` would flatten the directory contents into
# WORKDIR; the explicit target keeps them under ./wheelhouse.
COPY wheelhouse ./wheelhouse
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install --find-links=/srv/alt_celery3/wheelhouse \
        -r requirements-docker.txt

# --- Application code (owned by the non-privileged runtime user) -------------
COPY --chown=celeuser:celeuser . .

# Persistent volume mount point used by celery beat for its schedule file.
RUN mkdir -p /data && chown -R celeuser:celeuser /data

# Never run the containers as root.
USER celeuser

# Default command: start a worker. Overridden per service in docker-compose.yml.
CMD ["celery", "-A", "app.celery_app", "worker", "--loglevel=INFO"]
