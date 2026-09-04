#!/usr/bin/env bash
# ============================================================================
# update.sh — pull the latest code from GitHub and recreate the Docker stack.
#
# Usage (on the deployment machine):
#     ./update.sh
#
# Prerequisites:
#   * run from the project directory (the script cd's into its own folder)
#   * `.env` exists (copy it from `.env.example` once)
#   * git + docker compose available on the server
# ============================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f .env ]]; then
    echo "error: .env not found. Create it first:" >&2
    echo "    cp .env.example .env   # then edit the values" >&2
    exit 1
fi

echo "==> 1/3 Pulling latest code from GitHub (git pull --ff-only)"
git pull --ff-only

echo "==> 2/3 Rebuilding the alt_celery3 image and recreating services"
docker compose up -d --build

echo "==> 3/3 Done. Current stack:"
docker compose ps
