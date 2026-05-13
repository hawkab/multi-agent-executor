#!/bin/sh
set -eu

export NANOBOT_WORKSPACE="${NANOBOT_WORKSPACE:-/root/.nanobot/workspace}"

mkdir -p "${NANOBOT_WORKSPACE}"

python /app/bootstrap_env.py

exec nanobot "$@"
