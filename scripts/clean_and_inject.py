"""
clean_and_inject.py
====================
Pipeline de nettoyage et d'injection en masse pour le projet e-commerce NoSQL.

Ce script fait 3 choses, dans l'ordre :
  1. NETTOYAGE : lit le CSV brut, détecte et journalise chaque anomalie
     (sans jamais faire un traitement ligne par ligne pour l'injection -
     seul le nettoyage pandas est "ligne par ligne" en mémoire, ce qui est normal).
  2. JOURNALISATION : écrit un rapport (logs/cleaning_report.json) et un CSV
     des lignes rejetées (logs/rejected_rows.csv), avec la raison du rejet.
  3. INJECTION EN MASSE :
       - MongoDB : bulk_write() avec InsertOne, par lots de BATCH_SIZE
       - Neo4j   : requêtes Cypher UNWIND, par lots de BATCH_SIZE

Usage :
    python clean_and_inject.py --csv chemin/vers/fichier.csv --dry-run
    python clean_and_inject.py --csv chemin/vers/fichier.csv
"""

import argparse
import json
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
BATCH_SIZE = 5000  # taille des lots pour les opérations bulk (Mongo / Neo4j)

MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
MONGO_DB = "ecommerce"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "motdepasse_a_changer"

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("pipeline")


# --------------------------------------------------------------------------
# 1. NETTOYAGE
# --------------------------------------------------------------------------
def clean_dataframe(df: pd.DataFrame):
    """
    Nettoie le DataFrame brut et retourne :
      - df_valid   : lignes propres, prêtes à être injectées
      - rejected   : liste de dicts {transaction_id, reason} pour le journal
      - stats      : compteurs pour le rapport
    """
    stats = {"total_lignes_lues": len(df)}
    rejected_rows = []  # accumulation des lignes rejetées avec leur raison

    # --- a) Doublons stricts (toutes colonnes identiques) -----------------
    dup_mask = df.duplicated(keep="first")
    stats["doublons_stricts"] = int(dup_mask.sum())
    for tx_id in df.loc[dup_mask, "transaction_id"]:
        rejected_rows.append({"transaction_id": tx_id, "reason": "doublon_strict"})
    df = df.loc[~dup_mask].copy()

    # --- b) Nettoyage du prix : enlever " CFA", convertir en float --------
    df["unit_price_clean"] = (
        df["unit_price"].astype(str).str.replace(r"\s*CFA\s*", "", regex=True)
    )
    df["unit_price_clean"] = pd.to_numeric(df["unit_price_clean"], errors="coerce")

    # --- c) Nettoyage de la quantité ---------------------------------------
    df["quantity_clean"] = pd.to_numeric(df["quantity"], errors="coerce")

    # --- d) Nettoyage de la date : ISO direct, puis format slash, puis rejet
    def parse_date(raw):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except (ValueError, TypeError):
                continue
        return None

    df["transaction_date_clean"] = df["transaction_date"].apply(parse_date)

    # --- e) Construction du masque de validité ------------------------------
    mask_qty_invalide = df["quantity_clean"].isna() | (df["quantity_clean"] <= 0)
    mask_prix_invalide = df["unit_price_clean"].isna() | (df["unit_price_clean"] <= 0)
    mask_date_invalide = df["transaction_date_clean"].isna()

    stats["quantite_incoherente"] = int(mask_qty_invalide.sum())
    stats["prix_corrompu_ou_negatif"] = int(mask_prix_invalide.sum())
    stats["date_invalide"] = int(mask_date_invalide.sum())

    for _, row in df.loc[mask_qty_invalide].iterrows():
        rejected_rows.append({"transaction_id": row["transaction_id"], "reason": "quantite_incoherente"})
    for _, row in df.loc[mask_prix_invalide & ~mask_qty_invalide].iterrows():
        rejected_rows.append({"transaction_id": row["transaction_id"], "reason": "prix_invalide"})
    for _, row in df.loc[mask_date_invalide & ~mask_qty_invalide & ~mask_prix_invalide].iterrows():
        rejected_rows.append({"transaction_id": row["transaction_id"], "reason": "date_invalide"})

    mask_rejet = mask_qty_invalide | mask_prix_invalide | mask_date_invalide
    df_valid = df.loc[~mask_rejet].copy()

    # --- f) customer_id manquant : PAS un rejet, juste un flag -------------
    df_valid["is_anonymous"] = (
        df_valid["customer_id"].isna() | (df_valid["customer_id"].astype(str).str.strip() == "")
    )
    stats["transactions_anonymes_conservees"] = int(df_valid["is_anonymous"].sum())

    stats["lignes_valides_finales"] = len(df_valid)
    stats["lignes_rejetees_total"] = len(rejected_rows)

    return df_valid, rejected_rows, stats


def write_reports(rejected_rows, stats):
    report_path = LOG_DIR / "cleaning_report.json"
    rejected_path = LOG_DIR / "rejected_rows.csv"

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    pd.DataFrame(rejected_rows).to_csv(rejected_path, index=False, encoding="utf-8")

    log.info("Rapport de nettoyage écrit dans %s", report_path)
    log.info("Lignes rejetées journalisées dans %s", rejected_path)
    for k, v in stats.items():
        log.info("  %-40s : %s", k, v)


# --------------------------------------------------------------------------
# 2. INJECTION MONGODB (bulk, par lots)
# --------------------------------------------------------------------------
def inject_mongodb(df_valid: pd.DataFrame, dry_run: bool = False):
    from pymongo import InsertOne, MongoClient
    from pymongo.errors import BulkWriteError

    if dry_run:
        log.info("[DRY-RUN] Injection MongoDB ignorée (%d documents prêts)", len(df_valid))
        return

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db["orders"]

    # Index unique sur transaction_id : une ré-exécution du script ne duplique
    # plus les données, les doublons sont rejetés silencieusement par Mongo.
    collection.create_index("transaction_id", unique=True)

    documents = df_valid.to_dict("records")
    total_inserted = 0

    for start in range(0, len(documents), BATCH_SIZE):
        batch = documents[start : start + BATCH_SIZE]
        operations = [
            InsertOne(
                {
                    "transaction_id": d["transaction_id"],
                    "customer_id": None if d["is_anonymous"] else d["customer_id"],
                    "product_id": d["product_id"],
                    "product_category": d["product_category"],
                    "transaction_date": d["transaction_date_clean"],
                    "quantity": int(d["quantity_clean"]),
                    "unit_price": float(d["unit_price_clean"]),
                    "total_amount": float(d["quantity_clean"]) * float(d["unit_price_clean"]),
                    "is_anonymous": bool(d["is_anonymous"]),
                }
            )
            for d in batch
        ]
        try:
            result = collection.bulk_write(operations, ordered=False)
            total_inserted += result.inserted_count
            log.info("MongoDB : lot %d-%d injecté (%d docs)", start, start + len(batch), result.inserted_count)
        except BulkWriteError as bwe:
            # ordered=False : les doublons (transaction_id déjà présent) sont
            # ignorés individuellement, le reste du lot est bien inséré.
            inserted = bwe.details.get("nInserted", 0)
            total_inserted += inserted
            nb_doublons = len(bwe.details.get("writeErrors", []))
            log.warning(
                "MongoDB : lot %d-%d - %d docs insérés, %d doublons ignorés (déjà présents)",
                start, start + len(batch), inserted, nb_doublons,
            )

    # Index utiles pour les agrégations et le rate-limiting métier
    collection.create_index("customer_id")
    collection.create_index("product_category")
    collection.create_index([("transaction_date", -1)])

    log.info("MongoDB : %d documents insérés au total", total_inserted)
    client.close()


# --------------------------------------------------------------------------
# 3. INJECTION NEO4J (UNWIND, par lots)
# --------------------------------------------------------------------------
def inject_neo4j(df_valid: pd.DataFrame, dry_run: bool = False):
    from neo4j import GraphDatabase

    # On exclut explicitement les transactions anonymes du graphe
    df_graph = df_valid.loc[~df_valid["is_anonymous"]].copy()

    if dry_run:
        log.info("[DRY-RUN] Injection Neo4j ignorée (%d relations prêtes)", len(df_graph))
        return

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    cypher_batch = """
    UNWIND $rows AS row
    MERGE (c:Customer {customer_id: row.customer_id})
    MERGE (p:Product {product_id: row.product_id})
      ON CREATE SET p.category = row.category
    MERGE (c)-[b:BOUGHT]->(p)
      ON CREATE SET b.count = 1, b.last_purchase = row.date
      ON MATCH  SET b.count = b.count + 1, b.last_purchase = row.date
    """

    with driver.session() as session:
        # Contraintes d'unicité (idempotent, à lancer une seule fois normalement)
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE")

        records = df_graph.to_dict("records")
        for start in range(0, len(records), BATCH_SIZE):
            batch = records[start : start + BATCH_SIZE]
            rows = [
                {
                    "customer_id": r["customer_id"],
                    "product_id": r["product_id"],
                    "category": r["product_category"],
                    "date": r["transaction_date_clean"].isoformat(),
                }
                for r in batch
            ]
            session.run(cypher_batch, rows=rows)
            log.info("Neo4j : lot %d-%d injecté (%d relations)", start, start + len(batch), len(batch))

    driver.close()
    log.info("Neo4j : injection terminée (%d relations Customer->Product)", len(df_graph))


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Nettoyage et injection en masse du dataset e-commerce")
    parser.add_argument("--csv", required=True, help="Chemin vers le CSV brut")
    parser.add_argument("--dry-run", action="store_true", help="Ne pas se connecter aux bases, juste nettoyer")
    parser.add_argument("--skip-mongo", action="store_true")
    parser.add_argument("--skip-neo4j", action="store_true")
    args = parser.parse_args()

    log.info("Lecture du fichier %s", args.csv)
    df_raw = pd.read_csv(args.csv, dtype=str)

    df_valid, rejected_rows, stats = clean_dataframe(df_raw)
    write_reports(rejected_rows, stats)

    if not args.skip_mongo:
        inject_mongodb(df_valid, dry_run=args.dry_run)
    if not args.skip_neo4j:
        inject_neo4j(df_valid, dry_run=args.dry_run)

    log.info("Pipeline terminé.")


if __name__ == "__main__":
    main()