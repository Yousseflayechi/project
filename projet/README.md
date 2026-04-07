# Élections municipales en temps réel — Pipeline Kafka

Projet de data engineering complet : ingestion événementielle Kafka → validation en flux → agrégations ksqlDB → stockage Cassandra → tableau de bord Streamlit.

---

## Prérequis

- Docker + Docker Compose (droits suffisants)
- Python 3.10+
- **8 Go de RAM minimum** alloués à Docker

---

## Démarrage rapide

```bash
# 1. Environnement Python
python3 -m venv .venv && source .venv/bin/activate
pip install -r enonce/requirements.txt

# 2. Stack Docker
docker compose up -d
docker compose ps          # kafka, ksqldb, cassandra doivent être en cours

# 3. Données de référence
python enonce/src/generate_votes_data.py
ls data/                   # votes_municipales_sample.jsonl  candidates.csv  communes_fr.json

# 4. Topics Kafka
docker compose exec kafka bash -c 'kafka-topics --create --if-not-exists --topic vote_events_raw      --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092'
docker compose exec kafka bash -c 'kafka-topics --create --if-not-exists --topic vote_events_valid    --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092'
docker compose exec kafka bash -c 'kafka-topics --create --if-not-exists --topic vote_events_rejected --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092'
docker compose exec kafka bash -c 'kafka-topics --create --if-not-exists --topic vote_agg_city_minute --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092'
```

> **Windows** : utiliser WSL2 pour exécuter les commandes bash.

---

## Architecture du pipeline

```
Producteur Python
      │
      ▼
vote_events_raw          ← topic Kafka (6 partitions)
      │
      ▼
Validateur Python
   ├─► vote_events_valid    ← votes conformes
   └─► vote_events_rejected ← rejets avec error_reason
             │
             ▼
          ksqlDB            ← streams, fenêtres, agrégats matérialisés
      ┌────┴─────────────────────────┐
      │  VOTE_COUNT_BY_CITY_MINUTE   │
      │  VOTE_COUNT_BY_DEPT_BLOCK    │  ← créé par ksqlDB, pas à la main
      └────────────────┬─────────────┘
                       │
                       ▼
              Chargeur Python
                       │
                       ▼
              Cassandra elections.*
         ┌─────────────┴──────────────────┐
         │  votes_by_city_minute          │
         │  votes_by_candidate_city       │
         │  votes_by_department_block     │
         └─────────────┬──────────────────┘
                       │
                       ▼
              Streamlit (port 8501)
```

---

## Lancer la chaîne complète

Ouvrir **quatre terminaux** depuis la racine du projet :

| Terminal | Commande | Rôle |
|----------|----------|------|
| 1 | `python enonce/src/producer_votes.py` | Génère les votes en continu |
| 2 | `python enonce/src/validator_votes.py` | Valide / rejette, répartit sur les topics |
| 3 | `python enonce/src/load_to_cassandra.py` | Charge les agrégats ksqlDB → Cassandra |
| 4 | `streamlit run enonce/src/dashboard_streamlit.py` | Tableau de bord |

> Les terminaux 1, 2 et 3 restent ouverts (boucles actives). `Ctrl+C` pour arrêter.

Pour un run limité (tests) : `MAX_MESSAGES=500 python enonce/src/producer_votes.py`

---

## Exercices — fichiers à compléter

| # | Fichier | Contenu attendu |
|---|---------|-----------------|
| 1–3 | *(aucun fichier enonce/)* | Environnement, données, topics |
| 4 | `enonce/src/producer_votes.py` | TODO 0–5 : événement JSON, clé Kafka, rythme |
| 5 | `enonce/src/validator_votes.py` | TODO 1–3 : règles, réécriture valide/rejeté |
| 6 | `enonce/sql/ksqldb_queries.sql` | TODO 1–7 : streams, tables d'agrégats |
| 7 | `enonce/cql/schema.cql` + `load_to_cassandra.py` | Keyspace, tables, chargeur |
| 8 | `enonce/src/dashboard_streamlit.py` | TODO 1–7 : KPI, graphiques, carte |

---

## Commandes de contrôle utiles

```bash
# Vérifier les topics
docker compose exec kafka bash -c 'kafka-topics --bootstrap-server localhost:9092 --list'

# Lire les 5 derniers votes bruts
docker compose exec kafka bash -c \
  'kafka-console-consumer --bootstrap-server localhost:9092 --topic vote_events_raw --from-beginning --max-messages 5'

# Appliquer le schéma Cassandra
docker compose exec -T cassandra cqlsh < enonce/cql/schema.cql

# Appliquer les requêtes ksqlDB
docker compose exec -T ksqldb ksql http://localhost:8088 < enonce/sql/ksqldb_queries.sql

# Compter les lignes dans Cassandra
docker compose exec cassandra cqlsh -e "SELECT count(*) FROM elections.votes_by_city_minute;"
docker compose exec cassandra cqlsh -e "SELECT count(*) FROM elections.votes_by_department_block;"

# Session interactive ksqlDB
docker compose exec -it ksqldb ksql http://localhost:8088
# puis : SHOW TABLES; SHOW QUERIES;
```

---

## Accès aux interfaces

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| ksqlDB | http://localhost:8088 |
| Streamlit | http://localhost:8501 |

---

## Modèle Cassandra (tables dénormalisées)

Chaque table correspond à un **chemin d'accès** précis — pas de jointure en lecture.

```sql
-- Tendance par commune
SELECT * FROM elections.votes_by_city_minute WHERE city_code='75056' LIMIT 50;

-- Répartition par candidat
SELECT * FROM elections.votes_by_candidate_city WHERE candidate_id='C01' LIMIT 50;

-- Carte département × bloc
SELECT * FROM elections.votes_by_department_block WHERE department_code='75';
```

---

## Dépannage rapide

| Symptôme | Cause | Solution |
|----------|-------|----------|
| `topic already exists` | Relance après création | Utiliser `--if-not-exists`, ou ignorer |
| `SHOW TABLES` vide après SQL | Fichier entièrement commenté | Ajouter des `CREATE` non commentés dans le `.sql` |
| Tables Cassandra absentes | `schema.cql` sans instructions actives | Décommenter `CREATE KEYSPACE`, `USE`, `CREATE TABLE` |
| Cassandra : connection refused | Conteneur pas encore prêt | Attendre 1–2 min, relancer `docker compose ps` |
| Tableau de bord vide | Chargeur arrêté ou flux inactif | Vérifier les terminaux 1, 2, 3 |
| Producteur `RuntimeError` | TODO 1 ou 2 non renseignés | Renseigner la clé (bytes) et la valeur (JSON encodé) |
| `load_to_cassandra` ne rend pas la main | Comportement normal | `Ctrl+C` pour quitter |

---

## Scripts utilitaires (optionnels)

```bash
./lancer_tout.sh             # Lance Docker + venv + génération données + topics
./lancer_tout.sh --run-apps  # Lance toute la chaîne (bloqué si TODO critiques présents)
./arreter_tout.sh            # Arrêt complet + suppression des volumes
./arreter_tout.sh --keep-volumes  # Arrêt sans suppression
```

---

## Données de référence

| Fichier | Contenu |
|---------|---------|
| `data/votes_municipales_sample.jsonl` | Événements de vote (un JSON par ligne) |
| `data/candidates.csv` | Identifiants, nuance (`party`), bloc (`political_block`) |
| `data/communes_fr.json` | Codes INSEE, rattachement départemental |

Champs d'un événement : `vote_id`, `event_time`, `city_code`, `department_code`, `region_code`, `candidate_id`, `signature_ok`, `channel`, `ingestion_ts`.
