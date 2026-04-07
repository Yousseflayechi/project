#!/usr/bin/env python3
"""Producteur Kafka — topic vote_events_raw complet."""

import json
import os
import time
import random
import uuid
import csv
import datetime as dt
from pathlib import Path

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "vote_events_raw"
_ROOT = Path(__file__).resolve().parent.parent.parent
COMMUNES_FILE = _ROOT / "data" / "communes_fr.json"
SAMPLE_COMMUNES = _ROOT / "data" / "communes_fr_sample.json"
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))  # <=0: infinite live stream
START_DELAY_MS = float(os.getenv("START_DELAY_MS", "20"))

# Cache global pour éviter de relire le CSV à chaque itération
_CANDIDATES_CACHE = []

def load_candidates() -> list[dict]:
    """Charge les candidats depuis le CSV."""
    if not _CANDIDATES_CACHE:
        try:
            with open(CANDIDATES_FILE, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    _CANDIDATES_CACHE.append(row)
        except Exception as e:
            print(f"Avertissement : Impossible de charger {CANDIDATES_FILE} ({e}). Utilisation de candidats par défaut.")
            return [
                {"id": "C01", "name": "Candidat A", "party": "PARTI 1", "political_block": "gauche"},
                {"id": "C02", "name": "Candidat B", "party": "PARTI 2", "political_block": "droite"}
            ]
    return _CANDIDATES_CACHE

def load_communes() -> list[dict]:
    """
    TODO 0 — Charger les communes
    """
    file_to_open = COMMUNES_FILE if COMMUNES_FILE.exists() else SAMPLE_COMMUNES
    try:
        with open(file_to_open, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Filtrer : ne garder que les dicts où code et nom sont non vides
        valid_communes = [c for c in data if c.get("code") and c.get("nom")]
        
        if not valid_communes:
            raise RuntimeError("La liste des communes est vide après filtrage.")
            
        return valid_communes
    except Exception as e:
        raise RuntimeError(f"Erreur lors du chargement des communes : {e}")

def build_realtime_event(communes: list[dict], sent: int) -> dict:
    """
    TODO 4 — Événement JSON généré à la volée
    """
    c = random.choice(communes)
    candidates = load_candidates()
    cand = random.choice(candidates)
    
    signature_ok = True
    
    # On gère les deux cas : si le CSV utilise "candidate_id" ou juste "id"
    c_id = cand.get("candidate_id", cand.get("id", "C00"))
    
    # Injection d'erreurs (indispensable pour l'exercice 5 - Validation)
    rand_err = random.random()
    if rand_err < 0.03:
        signature_ok = False # 3% de signatures invalides
    elif rand_err < 0.05:
        c_id = "C99" # 2% de candidats inconnus

    now_utc = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    return {
        "vote_id": str(uuid.uuid4()),
        "election_id": "muni_2026",
        "event_time": now_utc,
        "city_code": c["code"],
        "city_name": c["nom"],
        "department_code": str(c.get("codeDepartement", "")),
        "region_code": str(c.get("codeRegion", "")),
        "polling_station_id": f"PS_{random.randint(1, 10):03d}",
        "candidate_id": c_id,
        "candidate_name": cand.get("name", cand.get("candidate_name", "")),
        "candidate_party": cand.get("party", cand.get("candidate_party", "")),
        "candidate_block": cand.get("political_block", cand.get("block", "")),
        "channel": random.choice(["booth", "mobile", "assist_terminal"]),
        "signature_ok": signature_ok,
        "voter_hash": str(uuid.uuid4().hex[:10]),
        "ingestion_ts": now_utc
    }

def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    sent = 0
    communes = load_communes()

    print("Démarrage du producteur Kafka...")

    while True:
        evt = build_realtime_event(communes, sent)

        # TODO 1 — Clé Kafka (partitionnement)
        key = str(evt.get("city_code", "UNKNOWN")).encode("utf-8")

        # TODO 2 — Valeur = JSON UTF-8
        value = json.dumps(evt, ensure_ascii=False).encode("utf-8")

        producer.produce(TOPIC, key=key, value=value)
        sent += 1

        # TODO 3 — Rythme
        if START_DELAY_MS > 0:
            time.sleep(START_DELAY_MS / 1000.0)

        # Affichage tous les 500 messages pour suivre l'activité sans polluer le terminal
        if sent % 500 == 0:
            print(f"Envoyé {sent} message(s) sur {TOPIC}...")

        if MAX_MESSAGES > 0 and sent >= MAX_MESSAGES:
            break

    producer.flush()
    print(f"Fin de l'envoi. Total : {sent} message(s) sur {TOPIC}.")

if __name__ == "__main__":
    main()