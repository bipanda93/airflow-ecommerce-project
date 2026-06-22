from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import csv
import os

# ============================================================
# CONFIGURATION DU DAG
# ============================================================
default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': lambda context: print(
        f"ALERTE : tâche {context['task_instance'].task_id} a échoué !"
    )
}

dag = DAG(
    dag_id='ecommerce_sales_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='Pipeline ETL e-commerce avec Jenkins, Airflow et MongoDB'
)

# ============================================================
# PARTIE 1 : FILESENSOR
# ============================================================
wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/opt/airflow/data/dataset.csv',
    poke_interval=15,
    timeout=300,
    mode='reschedule',
    dag=dag
)

# ============================================================
# PARTIE 2 : VALIDATION DU FICHIER
# ============================================================
def validate_file(**context):
    filepath = '/opt/airflow/data/dataset.csv'
    if not os.path.exists(filepath):
        raise Exception(f"Fichier introuvable : {filepath}")
    if os.path.getsize(filepath) == 0:
        raise Exception("Fichier vide !")
    with open(filepath) as f:
        reader = csv.reader(f)
        rows = list(reader)
        nb_lignes = len(rows) - 1
    print(f"Fichier valide : {nb_lignes} enregistrements trouvés")
    return nb_lignes

task_validate = PythonOperator(
    task_id='validate_file',
    python_callable=validate_file,
    provide_context=True,
    dag=dag
)

# ============================================================
# PARTIE 3 : CONTRÔLE QUALITÉ
# ============================================================
def quality_check(**context):
    filepath = '/opt/airflow/data/dataset.csv'
    valid_rows = []
    invalid_rows = []

    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            errors = []
            try:
                quantity = float(row['Quantity'])
                unit_price = float(row['UnitPrice'])
                montant = quantity * unit_price
                if quantity <= 0:
                    errors.append("Quantité nulle ou négative")
                if montant < 0:
                    errors.append("Montant négatif")
                if not row['InvoiceNo']:
                    errors.append("InvoiceNo manquant")
            except ValueError:
                errors.append("Valeur non numérique")

            if errors:
                row['errors'] = ' | '.join(errors)
                invalid_rows.append(row)
            else:
                valid_rows.append(row)

    error_file = '/opt/airflow/data/errors.csv'
    if invalid_rows:
        with open(error_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=invalid_rows[0].keys())
            writer.writeheader()
            writer.writerows(invalid_rows)

    print(f"Lignes valides : {len(valid_rows)}")
    print(f"Lignes invalides : {len(invalid_rows)}")

    return {
        'valid_rows': len(valid_rows),
        'invalid_rows': len(invalid_rows),
        'error_file': error_file if invalid_rows else None
    }

task_quality = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check,
    provide_context=True,
    dag=dag
)

# ============================================================
# PARTIE 4 : BRANCHPYTHONOPERATOR
# ============================================================
def check_data(**context):
    quality = context['ti'].xcom_pull(task_ids='quality_check')
    if quality['valid_rows'] > 0:
        return 'load_data'
    else:
        return 'stop_pipeline'

task_branch = BranchPythonOperator(
    task_id='check_data',
    python_callable=check_data,
    provide_context=True,
    dag=dag
)

task_stop = DummyOperator(
    task_id='stop_pipeline',
    dag=dag
)

# ============================================================
# PARTIE 5 : CALCUL DES KPI
# ============================================================
def load_data(**context):
    filepath = '/opt/airflow/data/dataset.csv'
    rows = []

    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                quantity = float(row['Quantity'])
                unit_price = float(row['UnitPrice'])
                if quantity > 0 and unit_price >= 0:
                    row['Montant'] = round(quantity * unit_price, 2)
                    rows.append(row)
            except ValueError:
                continue

    nb_commandes = len(set(r['InvoiceNo'] for r in rows))
    nb_clients = len(set(r['CustomerID'] for r in rows))
    chiffre_affaires = round(sum(r['Montant'] for r in rows), 2)
    panier_moyen = round(chiffre_affaires / nb_commandes, 2) if nb_commandes > 0 else 0

    print(f"Nb commandes       : {nb_commandes}")
    print(f"Nb clients         : {nb_clients}")
    print(f"Chiffre d'affaires : {chiffre_affaires} €")
    print(f"Panier moyen       : {panier_moyen} €")

    produits = {}
    for row in rows:
        p = row['Description']
        produits[p] = produits.get(p, 0) + row['Montant']
    top_products = sorted(produits.items(), key=lambda x: x[1], reverse=True)[:10]

    regions = {}
    for row in rows:
        r = row['Country']
        regions[r] = regions.get(r, 0) + row['Montant']

    return {
        'nb_commandes': nb_commandes,
        'nb_clients': nb_clients,
        'chiffre_affaires': chiffre_affaires,
        'panier_moyen': panier_moyen,
        'top_products': top_products,
        'region_metrics': regions
    }

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# ============================================================
# PARTIE 6 : DYNAMIC TASKS PAR RÉGION
# ============================================================
def analyse_region(region, **context):
    filepath = '/opt/airflow/data/dataset.csv'
    rows = []
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Country'] == region:
                try:
                    q = float(row['Quantity'])
                    p = float(row['UnitPrice'])
                    if q > 0:
                        rows.append(q * p)
                except ValueError:
                    continue
    ca = round(sum(rows), 2)
    print(f"{region} → {len(rows)} transactions, CA={ca} €")

def get_regions():
    filepath = '/opt/airflow/data/dataset.csv'
    if not os.path.exists(filepath):
        return ['Unknown']
    with open(filepath) as f:
        reader = csv.DictReader(f)
        return list(set(r['Country'] for r in reader))

taches_regions = []
for region in get_regions():
    t = PythonOperator(
        task_id=f'analyse_{region.lower().replace(" ", "_")}',
        python_callable=analyse_region,
        op_kwargs={'region': region},
        dag=dag
    )
    taches_regions.append(t)

# ============================================================
# PARTIE 7 : RAPPORT FINAL
# ============================================================
def generate_report(**context):
    kpi = context['ti'].xcom_pull(task_ids='load_data')
    quality = context['ti'].xcom_pull(task_ids='quality_check')

    print("=" * 50)
    print("RAPPORT FINAL E-COMMERCE")
    print("=" * 50)
    print(f"Nb commandes       : {kpi['nb_commandes']}")
    print(f"Nb clients         : {kpi['nb_clients']}")
    print(f"Chiffre d'affaires : {kpi['chiffre_affaires']} €")
    print(f"Panier moyen       : {kpi['panier_moyen']} €")
    print(f"Lignes valides     : {quality['valid_rows']}")
    print(f"Lignes invalides   : {quality['invalid_rows']}")
    print("=" * 50)

    with open('/opt/airflow/data/rapport_final.csv', 'w') as f:
        f.write("indicateur,valeur\n")
        f.write(f"nb_commandes,{kpi['nb_commandes']}\n")
        f.write(f"nb_clients,{kpi['nb_clients']}\n")
        f.write(f"chiffre_affaires,{kpi['chiffre_affaires']}\n")
        f.write(f"panier_moyen,{kpi['panier_moyen']}\n")
        f.write(f"lignes_valides,{quality['valid_rows']}\n")
        f.write(f"lignes_invalides,{quality['invalid_rows']}\n")

    print("Rapport sauvegardé dans /opt/airflow/data/rapport_final.csv")

task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# ============================================================
# PARTIE 8 : STOCKAGE MONGODB
# ============================================================
def store_mongodb(**context):
    from pymongo import MongoClient

    kpi = context['ti'].xcom_pull(task_ids='load_data')
    quality = context['ti'].xcom_pull(task_ids='quality_check')

    client = MongoClient('mongodb://ecommerce_mongodb:27017/')
    db = client['ecommerce_analytics']
    collection = db['sales_metrics']

    document = {
        'execution_date': str(context['execution_date']),
        'dag_id': context['dag'].dag_id,
        'dataset': 'online_retail',
        'source_file': 'dataset.csv',
        'status': 'success',
        'global_metrics': {
            'nb_commandes': kpi['nb_commandes'],
            'nb_clients': kpi['nb_clients'],
            'chiffre_affaires': kpi['chiffre_affaires'],
            'panier_moyen': kpi['panier_moyen']
        },
        'top_products': [
            {'product': p, 'revenue': r} for p, r in kpi['top_products']
        ],
        'region_metrics': [
            {'region': r, 'revenue': v} for r, v in kpi['region_metrics'].items()
        ],
        'quality': {
            'valid_rows': quality['valid_rows'],
            'invalid_rows': quality['invalid_rows'],
            'error_file': quality['error_file']
        }
    }

    collection.insert_one(document)
    print(f"Document inséré dans MongoDB : ecommerce_analytics.sales_metrics")
    client.close()

task_mongodb = PythonOperator(
    task_id='store_mongodb',
    python_callable=store_mongodb,
    provide_context=True,
    dag=dag
)

# ============================================================
# DÉPENDANCES
# ============================================================
wait_for_file >> task_validate >> task_quality >> task_branch
task_branch >> task_load
task_branch >> task_stop
task_load >> taches_regions
taches_regions >> task_report
task_report >> task_mongodb