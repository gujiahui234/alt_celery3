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
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates passwd tzdata \
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

# --- Python dependencies (cached unless requirements.txt changes) ------------
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# --- Application code (owned by the non-privileged runtime user) -------------
COPY --chown=celeuser:celeuser . .

# Persistent volume mount point used by celery beat for its schedule file.
RUN mkdir -p /data && chown -R celeuser:celeuser /data

# Never run the containers as root.
USER celeuser

# Default command: start a worker. Overridden per service in docker-compose.yml.
CMD ["celery", "-A", "app.celery_app", "worker", "--loglevel=INFO"]
