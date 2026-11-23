#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${LAB_DIR}"
docker compose down --volumes --remove-orphans
echo "Kafka lab stopped and all named broker volumes were deleted."
echo "Run 'docker compose up -d' to create a fresh cluster."
