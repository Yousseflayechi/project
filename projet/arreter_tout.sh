#!/usr/bin/env bash
# Par défaut : tue les scripts Python/Streamlit + docker compose down -v + nettoyage logs.
# Option --keep-volumes : garde les volumes Docker (pas de remise à zéro totale).
#
# Compatibilité :
# - Linux / macOS : OK
# - Windows : utiliser de préférence WSL2.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.yml)
LOG_DIR="$ROOT/logs"
PIDS_FILE="$LOG_DIR/etudiants.pids"
KEEP_VOLUMES=false

if [[ "${1:-}" == "--keep-volumes" ]]; then
  KEEP_VOLUMES=true
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Commande requise introuvable: docker" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose (plugin 'docker compose') est requis." >&2
  exit 1
fi

echo "=== Arrêt des scripts lancés localement ==="
if [[ -f "$PIDS_FILE" ]]; then
  while read -r pid; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      echo " - arrêt PID $pid"
    fi
  done <"$PIDS_FILE"
fi

# Rattrapage si les scripts ont été lancés hors fichier PIDs.
if command -v pkill >/dev/null 2>&1; then
  for pattern in \
    "$ROOT/enonce/src/producer_votes.py" \
    "$ROOT/enonce/src/validator_votes.py" \
    "$ROOT/enonce/src/load_to_cassandra.py" \
    "$ROOT/enonce/src/dashboard_streamlit.py"; do
    pkill -f "$pattern" 2>/dev/null || true
  done
else
  echo "Info: pkill introuvable, arrêt via fichier PID uniquement."
fi

echo "=== Arrêt Docker ==="
if $KEEP_VOLUMES; then
  "${COMPOSE[@]}" down --remove-orphans
  echo "Conteneurs arrêtés, volumes conservés."
else
  "${COMPOSE[@]}" down -v --remove-orphans
  echo "Conteneurs + volumes supprimés (remise à zéro)."
fi

echo "=== Nettoyage logs ==="
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log "$PIDS_FILE" 2>/dev/null || true

echo "Terminé."
