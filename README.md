# Projet E-commerce NoSQL — MongoDB / Neo4j / Redis

## 1. Architecture

```
                     ┌─────────────┐
                     │   FastAPI   │  (api/main.py)
                     └──────┬──────┘
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
 ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
 │  MongoDB    │     │   Neo4j     │     │   Redis     │
 │ Replica Set │     │  (graphe)   │     │(clé-valeur) │
 │ 3 nœuds     │     │             │     │             │
 └─────────────┘     └─────────────┘     └─────────────┘
   Catalogue &          Moteur de           Sessions &
   commandes            recommandation      top ventes
```

## 2. Lancer l'infrastructure

```bash
cd docker
docker compose up -d
docker compose ps        # vérifier que les 5 conteneurs sont "healthy" / "running"
```

Le conteneur `mongo-init` initialise automatiquement le replica set `rs0` (il se lance une
fois puis s'arrête, c'est normal - vérifiez ses logs avec `docker logs mongo-init` s'il y a
un doute). Pour vérifier manuellement l'état du replica set :

```bash
docker exec -it mongo1 mongosh --eval "rs.status()"
```

Vous devez voir `mongo1` en `PRIMARY` et `mongo2`/`mongo3` en `SECONDARY`.

## 3. Nettoyer et injecter les données

```bash
cd scripts
pip install -r ../api/requirements.txt
python clean_and_inject.py --csv /chemin/vers/ecommerce_raw_transactions_dirty.csv
```

Options utiles :
- `--dry-run` : nettoie et journalise sans se connecter aux bases (pour tester le nettoyage seul)
- `--skip-mongo` / `--skip-neo4j` : injecter dans une seule base à la fois

Les rapports sont écrits dans `logs/` :
- `cleaning_report.json` : compteurs de chaque anomalie
- `rejected_rows.csv` : chaque ligne rejetée + sa raison
- `pipeline.log` : journal complet d'exécution

## 4. Lancer l'API

```bash
cd api
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Endpoints principaux :

| Méthode | Route | Base | Description |
|---|---|---|---|
| GET | `/products/top-selling?category=Mode&limit=5` | MongoDB | Agrégation CA par produit |
| GET | `/customers/{id}/stats` | MongoDB | Statistiques d'achat d'un client |
| GET | `/recommendations/{id}?depth=2` | Neo4j | Recommandations collaboratives (2 ou 3 sauts) |
| POST | `/sales/record/{product_id}` | Redis | Enregistre une vente (leaderboard temps réel) |
| GET | `/sales/top` | Redis | Top des ventes en direct |
| POST | `/sessions/{customer_id}` | Redis | Crée une session avec TTL |
| GET | `/health` | Toutes | Vérifie la connectivité des 3 bases |

Toutes les routes sont protégées par un rate limiting Redis (20 requêtes/minute/IP,
code HTTP 429 au-delà).

## 5. Préparer la démo de résilience 


1. **Pourquoi ça marche** : le driver `pymongo`, quand on lui donne une URI avec
   `?replicaSet=rs0` et la liste des 3 hôtes, sait qu'il parle à un replica set. Si le
   nœud PRIMARY tombe, les 2 SECONDARY restants élisent automatiquement un nouveau
   PRIMARY en quelques secondes (élection Raft-like de MongoDB). Le driver détecte le
   changement de topologie et redirige les écritures vers le nouveau PRIMARY, sans
   qu'on ait à changer une ligne de code.
2. **Le paramètre qui protège une écriture en cours** : `retryWrites=true` (activé par
   défaut depuis les drivers MongoDB récents). Si une écriture est en vol pendant
   l'élection, le driver la rejoue automatiquement une fois le nouveau PRIMARY élu.
3. **Ce que vous devez montrer à l'oral** :
   ```bash
   # Terminal 1 : lancer une injection continue ou l'API
   # Terminal 2 : couper le PRIMARY pendant que ça tourne
   docker stop mongo1
   # Observer : l'API continue de répondre après une courte latence (2-10s)
   docker exec -it mongo2 mongosh --eval "rs.status()"   # mongo2 ou mongo3 est devenu PRIMARY
   docker start mongo1   # mongo1 revient comme SECONDARY, pas de conflit
   ```
4. **Limite** : il y a une courte fenêtre d'indisponibilité
   en écriture pendant l'élection (quelques secondes) - ce n'est pas de la haute
   disponibilité à zéro interruption, c'est de la tolérance aux pannes avec bascule
   automatique, ce qui est exactement ce que demande l'énoncé.

## 5. Structure des fichiers

```
nosql-project/
├── docker/
│   └── docker-compose.yml       # infra 5 conteneurs
├── scripts/
│   └── clean_and_inject.py      # nettoyage + injection bulk
├── api/
│   ├── main.py                  # API FastAPI
│   └── requirements.txt
├── logs/                        # généré après exécution du pipeline
└── README.md
```

