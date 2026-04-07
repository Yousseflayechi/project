#!/usr/bin/env python3
"""Tableau de bord Streamlit — lecture Cassandra."""

import streamlit as st
import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster
from pathlib import Path
import json

st.set_page_config(page_title="Municipales 2026", layout="wide")
st.title("Municipales 2026 - Dashboard France")

# =============================================================================
# TODO 1 — Connexion + lectures Cassandra
# =============================================================================
@st.cache_resource
def get_cassandra_session():
    cluster = Cluster(["127.0.0.1"])
    return cluster.connect("elections")

session = get_cassandra_session()

def fetch_data(query):
    rows = session.execute(query)
    return pd.DataFrame(list(rows))

# Récupération des données depuis Cassandra
df_city_minute = fetch_data("SELECT * FROM votes_by_city_minute")
df_dept_block = fetch_data("SELECT * FROM votes_by_department_block")

# Arrêt du script si Cassandra est vide pour éviter les erreurs d'affichage
if df_city_minute.empty:
    st.warning("En attente de données depuis Cassandra... Lancez le producteur, le validateur et le chargeur.")
    st.stop()

# =============================================================================
# TODO 2 — DataFrames & Nettoyage
# =============================================================================
df_city_minute["votes_count"] = pd.to_numeric(df_city_minute["votes_count"], errors="coerce").fillna(0)
if not df_dept_block.empty:
    df_dept_block["votes_count"] = pd.to_numeric(df_dept_block["votes_count"], errors="coerce").fillna(0)

# =============================================================================
# TODO 3 — Bloc / parti (candidates.csv)
# =============================================================================
# Chargement du fichier CSV des candidats
# =============================================================================
# TODO 3 — Bloc / parti (candidates.csv)
# =============================================================================
# Chargement du fichier CSV des candidats
csv_path = Path("data/candidates.csv")
if csv_path.exists():
    df_candidates = pd.read_csv(csv_path)
    
    # 1. Uniformisation des noms de colonnes (au cas où le CSV diffère)
    rename_dict = {
        "candidate_id": "id",
        "candidate_name": "name",
        "candidate_party": "party",
        "block": "political_block"
    }
    df_candidates = df_candidates.rename(columns=rename_dict)
    
    # 2. Sécurisation : on s'assure que les colonnes requises existent bien
    required_cols = ["id", "party", "political_block", "name"]
    for col in required_cols:
        if col not in df_candidates.columns:
            df_candidates[col] = "Inconnu"
            
    # 3. Jointure sécurisée avec le flux de votes
    df_city_minute = df_city_minute.merge(
        df_candidates[required_cols], 
        left_on="candidate_id", 
        right_on="id", 
        how="left"
    )
    
    # Si le merge ne trouve pas de correspondance, on affiche l'ID au lieu d'un blanc
    if "name" in df_city_minute.columns:
        df_city_minute["name"] = df_city_minute["name"].fillna(df_city_minute["candidate_id"])
else:
    st.error("Le fichier data/candidates.csv est introuvable. Le dashboard va manquer d'informations.")
    st.stop()

# Traitement du timestamp pour la série temporelle
# Traitement du timestamp pour la série temporelle
try:
    # 1. On force la conversion du texte vers un nombre (int/float)
    numeric_timestamps = pd.to_numeric(df_city_minute["minute_bucket"])
    # 2. On convertit ce nombre en date avec l'unité milliseconde
    df_city_minute["minute_bucket"] = pd.to_datetime(numeric_timestamps, unit="ms")
except Exception:
    # Solution de repli au cas où c'est déjà un format ISO "2026-04-07..."
    df_city_minute["minute_bucket"] = pd.to_datetime(df_city_minute["minute_bucket"], errors="coerce")

# =============================================================================
# TODO 4 & 4bis — Graphiques par Bloc et par Parti
# =============================================================================
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Votes par bloc politique")
    # Copie pour ne pas altérer le DF principal
    df_bloc = df_city_minute.copy()
    # Règle du sujet : "ecologiste" -> "gauche"
    df_bloc["political_block"] = df_bloc["political_block"].replace("ecologiste", "gauche")
    
    bloc_agg = df_bloc.groupby("political_block", as_index=False)["votes_count"].sum()
    
    fig_bloc = px.bar(
        bloc_agg, 
        x="political_block", 
        y="votes_count", 
        color="political_block",
        title="Répartition par bloc (Écologistes inclus dans Gauche)",
        labels={"political_block": "Bloc Politique", "votes_count": "Nombre de votes"}
    )
    st.plotly_chart(fig_bloc, use_container_width=True)

with col_chart2:
    st.subheader("Votes par parti (Nuance)")
    party_agg = df_city_minute.groupby("party", as_index=False)["votes_count"].sum()
    
    fig_party = px.bar(
        party_agg, 
        x="party", 
        y="votes_count", 
        color="party",
        title="Répartition détaillée par parti",
        labels={"party": "Parti", "votes_count": "Nombre de votes"}
    )
    st.plotly_chart(fig_party, use_container_width=True)

st.markdown("---")

# =============================================================================
# TODO 5 — Série temporelle
# =============================================================================
st.subheader("Dynamique des votes en temps réel")
time_agg = df_city_minute.groupby(["minute_bucket", "name"], as_index=False)["votes_count"].sum()

fig_time = px.line(
    time_agg, 
    x="minute_bucket", 
    y="votes_count", 
    color="name",
    title="Évolution des votes par minute et par candidat",
    labels={"minute_bucket": "Heure (Fenêtre d'une minute)", "votes_count": "Votes", "name": "Candidat"},
    markers=True
)
st.plotly_chart(fig_time, use_container_width=True)

st.markdown("---")

# =============================================================================
# TODO 6 & 7 — Géographie et Palmarès
# =============================================================================
col_geo, col_top = st.columns(2)

with col_geo:
    st.subheader("Cartographie (Top Départements)")
    if not df_dept_block.empty:
        dept_agg = df_dept_block.groupby("department_code", as_index=False)["votes_count"].sum().sort_values(by="votes_count", ascending=False).head(15)
        fig_geo = px.bar(
            dept_agg,
            x="department_code",
            y="votes_count",
            title="Les 15 départements les plus actifs",
            labels={"department_code": "Code Département", "votes_count": "Total des votes"}
        )
        st.plotly_chart(fig_geo, use_container_width=True)
    else:
        st.info("Les données départementales arrivent...")

with col_top:
    st.subheader("Palmarès")
    tab1, tab2 = st.tabs(["Top 10 Communes", "Classement Candidats"])
    
    with tab1:
        top_cities = df_city_minute.groupby("city_code", as_index=False)["votes_count"].sum()
        top_cities = top_cities.sort_values("votes_count", ascending=False).head(10).reset_index(drop=True)
        st.dataframe(top_cities, use_container_width=True)
        
    with tab2:
        top_cands = df_city_minute.groupby("name", as_index=False)["votes_count"].sum()
        top_cands = top_cands.sort_values("votes_count", ascending=False).reset_index(drop=True)
        st.dataframe(top_cands, use_container_width=True)