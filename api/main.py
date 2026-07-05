"""
main.py - API REST de la plateforme e-commerce
================================================
Expose 3 familles d'endpoints, une par technologie NoSQL :

  1. /products/...        -> MongoDB (agrégations sur le catalogue/commandes)
  2. /recommendations/... -> Neo4j (moteur de recommandation, 2-3 sauts)
  3. /sales/top /sessions  -> Redis (top ventes temps réel + sessions)

+ Un middleware de Rate Limiting basé sur Redis (algorithme "fixed window").

Lancer avec :
    uvicorn main:app --reload --port 8000
"""

import time
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from neo4j import GraphDatabase
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Configuration (à externaliser en variables d'environnement en production)
# ---------------------------------------------------------------------------
MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
MONGO_DB = "ecommerce"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "motdepasse_a_changer"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

RATE_LIMIT_MAX_REQUESTS = 20   # requêtes max
RATE_LIMIT_WINDOW_SECONDS = 60  # par fenêtre de 60s


# ---------------------------------------------------------------------------
# Cycle de vie : ouverture/fermeture propre des connexions
# ---------------------------------------------------------------------------
clients = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    clients["mongo"] = MongoClient(MONGO_URI)
    clients["neo4j"] = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    clients["redis"] = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    yield
    clients["mongo"].close()
    clients["neo4j"].close()


app = FastAPI(title="E-commerce NoSQL API", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Middleware : Rate Limiting via Redis (fenêtre fixe, clé = IP + minute)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    r = clients["redis"]
    client_ip = request.client.host
    window = int(time.time() // RATE_LIMIT_WINDOW_SECONDS)
    key = f"ratelimit:{client_ip}:{window}"

    current = r.incr(key)
    if current == 1:
        r.expire(key, RATE_LIMIT_WINDOW_SECONDS)

    if current > RATE_LIMIT_MAX_REQUESTS:
        return JSONResponse(
            status_code=429,
            content={"detail": "Trop de requêtes, réessayez dans un instant."},
        )

    response = await call_next(request)
    response.headers["X-RateLimit-Remaining"] = str(max(0, RATE_LIMIT_MAX_REQUESTS - current))
    return response


# ---------------------------------------------------------------------------
# 1. MONGODB : agrégations complexes
# ---------------------------------------------------------------------------
@app.get("/products/top-selling")
def top_selling_products(category: str | None = None, limit: int = 10):
    """Top produits par chiffre d'affaires, avec filtre optionnel par catégorie."""
    db = clients["mongo"][MONGO_DB]
    match_stage = {"$match": {}}
    if category:
        match_stage["$match"]["product_category"] = category

    pipeline = [
        match_stage,
        {
            "$group": {
                "_id": "$product_id",
                "category": {"$first": "$product_category"},
                "total_revenue": {"$sum": "$total_amount"},
                "total_quantity": {"$sum": "$quantity"},
                "nb_transactions": {"$sum": 1},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
    ]
    results = list(db["orders"].aggregate(pipeline))
    return {"category_filter": category, "results": results}


@app.get("/customers/{customer_id}/stats")
def customer_stats(customer_id: str):
    """Statistiques d'achat consolidées pour un client (CA total, panier moyen, etc.)."""
    db = clients["mongo"][MONGO_DB]
    pipeline = [
        {"$match": {"customer_id": customer_id}},
        {
            "$group": {
                "_id": "$customer_id",
                "total_spent": {"$sum": "$total_amount"},
                "avg_basket": {"$avg": "$total_amount"},
                "nb_orders": {"$sum": 1},
                "categories": {"$addToSet": "$product_category"},
            }
        },
    ]
    result = list(db["orders"].aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="Client introuvable ou sans commande")
    return result[0]


# ---------------------------------------------------------------------------
# 2. NEO4J : recommandations à 2-3 niveaux de profondeur
# ---------------------------------------------------------------------------
@app.get("/recommendations/{customer_id}")
def recommend_products(customer_id: str, depth: int = 2, limit: int = 10):
    """
    Recommandation collaborative :
    "Les clients qui ont acheté les mêmes produits que vous ont aussi acheté..."

    depth=2 : Customer -> Product <- autres Customers -> autres Products
    depth=3 : ajoute un saut supplémentaire vers des produits de 2e degré
    """
    driver = clients["neo4j"]

    if depth == 2:
        query = """
        MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(p:Product)
              <-[:BOUGHT]-(other:Customer)-[:BOUGHT]->(rec:Product)
        WHERE NOT (c)-[:BOUGHT]->(rec)
        RETURN rec.product_id AS product_id, rec.category AS category,
               count(DISTINCT other) AS score
        ORDER BY score DESC
        LIMIT $limit
        """
    else:
        query = """
        MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(p:Product)
              <-[:BOUGHT]-(other:Customer)-[:BOUGHT]->(rec:Product)
              <-[:BOUGHT]-(other2:Customer)-[:BOUGHT]->(rec2:Product)
        WHERE NOT (c)-[:BOUGHT]->(rec2) AND rec2 <> p
        RETURN rec2.product_id AS product_id, rec2.category AS category,
               count(DISTINCT other2) AS score
        ORDER BY score DESC
        LIMIT $limit
        """

    with driver.session() as session:
        results = session.run(query, customer_id=customer_id, limit=limit)
        recommendations = [dict(record) for record in results]

    if not recommendations:
        raise HTTPException(status_code=404, detail="Aucune recommandation trouvée pour ce client")

    return {"customer_id": customer_id, "depth": depth, "recommendations": recommendations}


# ---------------------------------------------------------------------------
# 3. REDIS : top des ventes temps réel + sessions à expiration
# ---------------------------------------------------------------------------
@app.post("/sales/record/{product_id}")
def record_sale(product_id: str, quantity: int = 1):
    """
    Incrémente le score d'un produit dans un Sorted Set Redis (leaderboard temps réel).
    Bien plus rapide qu'une agrégation Mongo pour un "top des ventes en direct".
    """
    r = clients["redis"]
    new_score = r.zincrby("leaderboard:sales", quantity, product_id)
    return {"product_id": product_id, "total_units_sold": new_score}


@app.get("/sales/top")
def top_sales(limit: int = 10):
    """Retourne le top des ventes en temps réel depuis le Sorted Set Redis."""
    r = clients["redis"]
    top = r.zrevrange("leaderboard:sales", 0, limit - 1, withscores=True)
    return {"top_products": [{"product_id": pid, "units_sold": score} for pid, score in top]}


@app.post("/sessions/{customer_id}")
def create_session(customer_id: str, ttl_seconds: int = 1800):
    """Crée une session utilisateur avec expiration automatique (TTL Redis)."""
    r = clients["redis"]
    session_key = f"session:{customer_id}"
    r.set(session_key, value="active", ex=ttl_seconds)
    return {"customer_id": customer_id, "session_key": session_key, "expires_in": ttl_seconds}


@app.get("/sessions/{customer_id}")
def check_session(customer_id: str):
    """Vérifie si une session est encore active, et son temps restant."""
    r = clients["redis"]
    session_key = f"session:{customer_id}"
    ttl = r.ttl(session_key)
    if ttl < 0:
        raise HTTPException(status_code=404, detail="Session inexistante ou expirée")
    return {"customer_id": customer_id, "active": True, "expires_in_seconds": ttl}


@app.get("/health")
def health():
    """Vérifie que les 3 bases sont joignables - utile pour la démo de résilience."""
    status = {}
    try:
        clients["mongo"].admin.command("ping")
        status["mongodb"] = "ok"
    except Exception as e:
        status["mongodb"] = f"KO: {e}"

    try:
        clients["neo4j"].verify_connectivity()
        status["neo4j"] = "ok"
    except Exception as e:
        status["neo4j"] = f"KO: {e}"

    try:
        clients["redis"].ping()
        status["redis"] = "ok"
    except Exception as e:
        status["redis"] = f"KO: {e}"

    return status
