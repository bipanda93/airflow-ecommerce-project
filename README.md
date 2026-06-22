# 🛒 Pipeline ETL E-commerce — Jenkins + Airflow + MongoDB

> Plateforme automatisée d'industrialisation des ventes e-commerce avec CI/CD complet, orchestration de données et stockage des métriques.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.0-green)
![Jenkins](https://img.shields.io/badge/Jenkins-LTS-red)
![MongoDB](https://img.shields.io/badge/MongoDB-7-brightgreen)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## 📋 Contexte

Une entreprise de commerce électronique reçoit quotidiennement des fichiers CSV contenant ses ventes. Ce projet automatise entièrement le cycle de traitement :

- **Détection** automatique des fichiers à l'arrivée (FileSensor)
- **Validation** et contrôle qualité des données (règles métier)
- **Calcul** des KPI (CA, panier moyen, top produits, régions)
- **Stockage** des métriques dans MongoDB
- **Déploiement** automatisé via Jenkins CI/CD

---

## 🏗️ Architecture

```
Développeur
     │
     └── git push
              │
         ┌───▼───┐
         │Jenkins│  ← Tests + Validation + Déploiement DAG
         └───┬───┘
              │
         ┌───▼──────┐
         │  Airflow  │  ← Orchestration ETL
         └───┬──────┘
              │
    ┌─────────▼─────────┐
    │  FileSensor        │  → Détecte dataset.csv
    │  validate_file     │  → Vérifie existence + taille
    │  quality_check     │  → Règles métier (quantités, montants)
    │  check_data        │  → BranchPythonOperator
    │  load_data         │  → Calcul KPI
    │  analyse_*         │  → Dynamic Tasks par région
    │  generate_report   │  → Rapport CSV (TriggerRule.ALL_DONE)
    │  store_mongodb     │  → Insertion métriques
    └─────────┬─────────┘
              │
         ┌───▼──────┐
         │ MongoDB  │  ← ecommerce_analytics.sales_metrics
         └──────────┘
```

---

## 🚀 Stack Technique

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Airflow | 2.9.0 | Orchestration pipeline ETL |
| Jenkins | LTS | CI/CD — tests, validation, déploiement |
| MongoDB | 7 | Stockage des métriques et KPI |
| PostgreSQL | 15 | Metadata DB Airflow |
| Docker Compose | - | Conteneurisation |
| Python | 3.12 | Logique métier et tests unitaires |
| pytest | 8.4.1 | 10 tests unitaires (10/10 PASSED) |

---

## 📁 Structure du projet

```
airflow-ecommerce-project/
├── dags/
│   └── ecommerce_sales_pipeline.py    # DAG principal Airflow
├── tests/
│   └── test_pipeline.py               # 10 tests unitaires pytest
├── data/
│   └── dataset.csv                    # Données source e-commerce
├── scripts/
│   └── check_mongodb.py               # Script vérification MongoDB
├── docker/
├── Jenkinsfile                         # Pipeline CI/CD Jenkins
├── requirements.txt                    # Dépendances Python
├── docker-compose.yml                  # Stack Docker complète
└── README.md
```

---

## ⚡ Démarrage rapide

### Prérequis

- Docker Desktop
- Git
- Python 3.12+

### Installation

```bash
# Cloner le dépôt
git clone https://github.com/bipanda93/airflow-ecommerce-project.git
cd airflow-ecommerce-project

# Créer les dossiers nécessaires
mkdir -p dags logs data

# Démarrer le stack complet
docker-compose up -d

# Vérifier que tout tourne
docker ps
```

### Accès aux interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8183 | admin / admin |
| Jenkins UI | http://localhost:8080 | - |
| MongoDB | localhost:27018 | - |
| PostgreSQL | localhost:5442 | airflow / airflow |

### Configuration Airflow (première fois)

Créer la connexion filesystem :

```
Admin → Connections → + 
Connection Id : fs_default
Connection Type : File (path)
Path : /
```

---

## 🧪 Tests unitaires

```bash
python3 -m pytest tests/test_pipeline.py -v
```

**Résultats : 10/10 PASSED en 0.03s**

| Test | Description |
|------|-------------|
| test_file_exists | Vérifie la présence du fichier CSV |
| test_file_not_empty | Vérifie que le fichier n'est pas vide |
| test_file_has_required_columns | Valide les 6 colonnes obligatoires |
| test_quality_check_detects_invalid_rows | Détecte quantités négatives |
| test_quality_check_valid_rows | Identifie les lignes valides |
| test_chiffre_affaires_calcul | Calcule le CA correctement |
| test_panier_moyen_calcul | CA / nb_commandes = 18.18 EUR |
| test_montant_negatif_rejete | Rejette les montants négatifs |
| test_regions_extraction | Extrait les régions sans doublons |
| test_task_id_format | Format task_id dynamique valide |

---

## 🔧 Pipeline Jenkins — 6 stages

```
Checkout → Install deps → Run tests → Validate DAG → Deploy DAG → Verify MongoDB
```

| Stage | Durée | Résultat |
|-------|-------|---------|
| Checkout | ~378ms | DAG trouvé (328 lignes) |
| Install dependencies | ~381ms | OK |
| Run tests | ~355ms | 10/10 PASSED |
| Validate DAG | ~353ms | Syntaxe valide |
| Deploy DAG | ~350ms | Volume partagé confirmé |
| Verify MongoDB | ~429ms | Container accessible |

**Durée totale : ~3 secondes**

---

## 📊 Concepts Airflow implémentés

| Concept | Implémentation |
|---------|----------------|
| `FileSensor` | Détection CSV avec `mode='reschedule'` |
| `BranchPythonOperator` | Routing conditionnel selon qualité des données |
| `XComs` | Transmission KPI entre `quality_check`, `load_data`, `store_mongodb` |
| `Dynamic Tasks` | 1 tâche créée automatiquement par région détectée |
| `TriggerRule.ALL_DONE` | `generate_report` s'exécute même en cas d'erreur partielle |
| `default_args retries=2` | 2 tentatives avec 30s d'attente sur toutes les tâches |

---

## 📈 Résultats obtenus

```json
{
  "nb_commandes": 17,
  "nb_clients": 4,
  "chiffre_affaires": 309.06,
  "panier_moyen": 18.18,
  "top_produit": "CREAM CUPID HEARTS COAT HANGER",
  "region_n1": "France (112.14 EUR)",
  "lignes_valides": 18,
  "lignes_invalides": 2
}
```

---

## 🗄️ Document MongoDB généré

```json
{
  "execution_date": "2026-06-22T17:22:35",
  "dag_id": "ecommerce_sales_pipeline",
  "status": "success",
  "global_metrics": {
    "nb_commandes": 17,
    "chiffre_affaires": 309.06,
    "panier_moyen": 18.18
  },
  "top_products": [...],
  "region_metrics": [...],
  "quality": {
    "valid_rows": 18,
    "invalid_rows": 2
  }
}
```

---

## ⚠️ Limites et perspectives

Cette architecture est optimisée pour le développement. En production avec 10 000+ commandes/jour :

- **LocalExecutor** → migrer vers **CeleryExecutor** ou **KubernetesExecutor**
- **CSV en mémoire** → traitement par chunks avec `pandas read_csv(chunksize=1000)`
- **Fichiers locaux** → **MinIO / S3** comme Data Lake (Bronze/Silver/Gold)
- **Monitoring** → **Prometheus + Grafana** pour les alertes en temps réel
- **Qualité données** → **Great Expectations** pour des rapports automatisés

---

## 👤 Auteur

**Franck Ulrich BIPANDA**
Mastère Data Engineering — Digital School de Paris (Bac+5, RNCP Niveau 7)

[![GitHub](https://img.shields.io/badge/GitHub-bipanda93-black)](https://github.com/bipanda93)
