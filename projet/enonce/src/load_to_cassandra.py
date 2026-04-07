#!/usr/bin/env python3
"""Chargeur des agrégats Kafka vers Cassandra."""

from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import json
import uuid

BOOTSTRAP = "localhost:9092"
# Générer un UUID pour le groupe permet de relire depuis le début à chaque lancement
GROUP_ID = f"agg-cassandra-loader-{uuid.uuid4().hex[:8]}"

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "elections"


def main() -> None:
    # =========================================================================
    # TODO 1 — Connexion Cassandra + requêtes préparées
    # =========================================================================
    print("Connexion au cluster Cassandra...")
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(KEYSPACE)

    # Préparation des requêtes d'insertion (optimisation des performances)
    stmt_city = session.prepare(
        "INSERT INTO votes_by_city_minute (city_code, minute_bucket, candidate_id, votes_count) VALUES (?, ?, ?, ?)"
    )
    stmt_cand_city = session.prepare(
        "INSERT INTO votes_by_candidate_city (candidate_id, city_code, minute_bucket, votes_count) VALUES (?, ?, ?, ?)"
    )
    stmt_dept = session.prepare(
        "INSERT INTO votes_by_department_block (department_code, block, votes_count) VALUES (?, ?, ?)"
    )
    print("Requêtes préparées avec succès.")

    # Configuration du Consumer Kafka
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    # =========================================================================
    # TODO 2 — Topics Kafka (topics générés par ksqlDB)
    # =========================================================================
    # Par défaut, ksqlDB crée les topics en MAJUSCULES
    topics_to_consume = ["VOTE_COUNT_BY_CITY_MINUTE", "VOTE_COUNT_BY_DEPT_BLOCK"]
    consumer.subscribe(topics_to_consume)
    
    print(f"Chargeur Cassandra démarré… Abonné aux topics : {topics_to_consume}")
    print("Appuyez sur Ctrl+C pour arrêter.")

    inserted_count = 0

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur consumer: {msg.error()}")
            continue

        try:
            row = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"Erreur de parsing JSON: {e}")
            continue
            
        topic = msg.topic()

        # =====================================================================
        # TODO 3 & 4 — Mapping et INSERT
        # =====================================================================
        if topic == "VOTE_COUNT_BY_DEPT_BLOCK":
            department = row.get("DEPARTMENT_CODE_V", row.get("department_code_v", row.get("DEPARTMENT_CODE", row.get("department_code", ""))))
            block = row.get("BLOCK_V", row.get("block_v", row.get("CANDIDATE_BLOCK", row.get("candidate_block", "autre"))))
            votes = int(row.get("TOTAL_VOTES", row.get("total_votes", 0)))
            
            if department:
                session.execute(stmt_dept, (department, block, votes))
                inserted_count += 1

        elif topic == "VOTE_COUNT_BY_CITY_MINUTE":
            city = row.get("CITY_CODE_V", row.get("city_code_v", row.get("CITY_CODE", row.get("city_code", ""))))
            cand = row.get("CANDIDATE_ID_V", row.get("candidate_id_v", row.get("CANDIDATE_ID", row.get("candidate_id", ""))))
            minute_key = str(row.get("WINDOWSTART", row.get("WINDOW_START", row.get("window_start", ""))))
            votes = int(row.get("VOTES_IN_MINUTE", row.get("votes_in_minute", 0)))
            
            if city and cand and minute_key:
                # On insère dans les deux tables orientées requêtes
                session.execute(stmt_city, (city, minute_key, cand, votes))
                session.execute(stmt_cand_city, (cand, city, minute_key, votes))
                inserted_count += 2
                
        # Petit log pour le suivi
        if inserted_count > 0 and inserted_count % 100 == 0:
            print(f"Données insérées / mises à jour dans Cassandra : {inserted_count}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt chargeur Cassandra")