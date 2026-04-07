#!/usr/bin/env bash
# Prépare le livrable étudiant sans lancer du code non implémenté.
# Par défaut : infra + venv + données + topics.
# Option --run-apps : lance aussi producer/validator/loader/streamlit
#                     MAIS uniquement si les TODO critiques sont remplis.
#
# Compatibilité :
# - Linux / macOS : OK
# - Windows : utiliser de préférence WSL2 (bash + docker compose côté Linux)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.yml)
LOG_DIR="$ROOT/logs"
PIDS_FILE="$LOG_DIR/etudiants.pids"
RUN_APPS=false

if [[ "${1:-}" == "--run-apps" ]]; then
  RUN_APPS=true
fi

mkdir -p "$LOG_DIR"
rm -f "$PIDS_FILE"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Commande requise introuvable: $cmd" >&2
    exit 1
  fi
}

preflight() {
  require_cmd docker
  require_cmd python3
  require_cmd grep
  if ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose (plugin 'docker compose') est requis." >&2
    exit 1
  fi
}

wait_kafka() {
  local n=0
  echo "   Attente Kafka..."
  while ! "${COMPOSE[@]}" exec -T kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    sleep 2
    n=$((n + 1))
    if (( n > 90 )); then
      echo "Timeout Kafka." >&2
      exit 1
    fi
  done
  echo "   Kafka OK"
}

wait_cassandra() {
  local n=0
  echo "   Attente Cassandra..."
  while ! "${COMPOSE[@]}" exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
    sleep 3
    n=$((n + 1))
    if (( n > 60 )); then
      echo "Timeout Cassandra." >&2
      exit 1
    fi
  done
  echo "   Cassandra OK"
}

wait_ksqldb() {
  local n=0
  echo "   Attente ksqlDB..."
  while ! "${COMPOSE[@]}" exec -T ksqldb curl -sf http://localhost:8088/info >/dev/null 2>&1; do
    sleep 3
    n=$((n + 1))
    if (( n > 60 )); then
      echo "Timeout ksqlDB." >&2
      exit 1
    fi
  done
  echo "   ksqlDB OK"
}

check_ready_to_run_apps() {
  local failed=0

  if grep -En "return \[\]|key = None|value = None|TODO_CITY" "enonce/src/producer_votes.py" >/dev/null; then
    echo " - Producer non prêt (TODO critiques détectés)." >&2
    failed=1
  fi
  if grep -En "^[[:space:]]*pass[[:space:]]*$|is_valid = True$" "enonce/src/validator_votes.py" >/dev/null; then
    echo " - Validateur non prêt (TODO critiques détectés)." >&2
    failed=1
  fi
  if grep -En "session = None|^[[:space:]]*pass[[:space:]]*$" "enonce/src/load_to_cassandra.py" >/dev/null; then
    echo " - Chargeur Cassandra non prêt (TODO critiques détectés)." >&2
    failed=1
  fi
  if grep -En "TODO" "enonce/src/dashboard_streamlit.py" >/dev/null; then
    echo " - Dashboard non prêt (TODO restants détectés)." >&2
    failed=1
  fi

  return $failed
}

append_pid() {
  echo "$1" >>"$PIDS_FILE"
}

preflight

echo "=== 1/5 Démarrage Docker ==="
"${COMPOSE[@]}" up -d
wait_kafka
wait_cassandra
wait_ksqldb

echo "=== 2/5 Environnement Python ==="
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
# shellcheck source=/dev/null
source .venv/bin/activate
pip install -q -r enonce/requirements.txt

echo "=== 3/5 Génération des données ==="
python enonce/src/generate_votes_data.py

echo "=== 4/5 Création topics de base ==="
for topic_spec in \
  "vote_events_raw:6" \
  "vote_events_valid:6" \
  "vote_events_rejected:6" \
  "vote_agg_city_minute:3"; do
  IFS=: read -r t parts <<<"$topic_spec"
  "${COMPOSE[@]}" exec -T kafka bash -c \
    "kafka-topics --create --if-not-exists --topic ${t} --partitions ${parts} --replication-factor 1 --bootstrap-server localhost:9092" >/dev/null
done

echo "=== 5/5 Préparation terminée ==="
echo "Kafka UI : http://localhost:8080 | ksqlDB : http://localhost:8088"

if ! $RUN_APPS; then
  echo
  echo "Mode sûr (par défaut) : aucune app métier lancée."
  echo "Quand les TODO sont complétés, lance : ./lancer_tout.sh --run-apps"
  echo "Arrêt complet : ./arreter_tout.sh"
  exit 0
fi

echo
echo "Vérification des TODO critiques avant lancement des apps..."
if ! check_ready_to_run_apps; then
  echo
  echo "Refus de lancer les apps : des TODO critiques restent présents."
  echo "Terminer le code puis relancer : ./lancer_tout.sh --run-apps"
  exit 2
fi

echo "Lancement producer + validator + loader + streamlit..."
nohup python enonce/src/producer_votes.py >>"$LOG_DIR/producer.log" 2>&1 &
append_pid $!
sleep 1
nohup python enonce/src/validator_votes.py >>"$LOG_DIR/validator.log" 2>&1 &
append_pid $!
sleep 1
nohup python enonce/src/load_to_cassandra.py >>"$LOG_DIR/load_to_cassandra.log" 2>&1 &
append_pid $!
sleep 1
nohup streamlit run enonce/src/dashboard_streamlit.py --server.headless true >>"$LOG_DIR/streamlit.log" 2>&1 &
append_pid $!

echo "Apps lancées en arrière-plan. Logs: $LOG_DIR"
echo "Streamlit : http://localhost:8501"
echo "Arrêt complet : ./arreter_tout.sh"
