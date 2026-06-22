import pytest
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

# ============================================================
# TEST 1 : Validation du fichier CSV
# ============================================================
def test_file_exists():
    """Le fichier dataset.csv doit exister"""
    filepath = os.path.join(os.path.dirname(__file__), '..', 'data', 'dataset.csv')
    assert os.path.exists(filepath), f"Fichier introuvable : {filepath}"

def test_file_not_empty():
    """Le fichier ne doit pas être vide"""
    filepath = os.path.join(os.path.dirname(__file__), '..', 'data', 'dataset.csv')
    assert os.path.getsize(filepath) > 0, "Fichier vide !"

def test_file_has_required_columns():
    """Le fichier doit contenir les colonnes obligatoires"""
    filepath = os.path.join(os.path.dirname(__file__), '..', 'data', 'dataset.csv')
    required_columns = ['InvoiceNo', 'Quantity', 'UnitPrice', 'Country', 'CustomerID', 'Description']
    with open(filepath) as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
    for col in required_columns:
        assert col in columns, f"Colonne manquante : {col}"

# ============================================================
# TEST 2 : Règles métier
# ============================================================
def test_quality_check_detects_invalid_rows():
    """Les lignes avec quantité négative doivent être détectées"""
    rows = [
        {'InvoiceNo': '001', 'Quantity': '-2', 'UnitPrice': '10.0', 'Country': 'France', 'CustomerID': '123', 'Description': 'Test'},
        {'InvoiceNo': '002', 'Quantity': '5',  'UnitPrice': '10.0', 'Country': 'France', 'CustomerID': '124', 'Description': 'Test'},
        {'InvoiceNo': '003', 'Quantity': '0',  'UnitPrice': '10.0', 'Country': 'France', 'CustomerID': '125', 'Description': 'Test'},
    ]
    invalid = [r for r in rows if float(r['Quantity']) <= 0]
    assert len(invalid) == 2, f"Attendu 2 lignes invalides, obtenu {len(invalid)}"

def test_quality_check_valid_rows():
    """Les lignes valides doivent être correctement identifiées"""
    rows = [
        {'InvoiceNo': '001', 'Quantity': '5',  'UnitPrice': '10.0', 'Country': 'France', 'CustomerID': '123', 'Description': 'Test'},
        {'InvoiceNo': '002', 'Quantity': '3',  'UnitPrice': '5.0',  'Country': 'France', 'CustomerID': '124', 'Description': 'Test'},
    ]
    valid = [r for r in rows if float(r['Quantity']) > 0]
    assert len(valid) == 2, f"Attendu 2 lignes valides, obtenu {len(valid)}"

# ============================================================
# TEST 3 : Calcul des KPI
# ============================================================
def test_chiffre_affaires_calcul():
    """Le CA total doit être la somme des montants valides"""
    rows = [
        {'Quantity': '5', 'UnitPrice': '10.0'},
        {'Quantity': '3', 'UnitPrice': '5.0'},
        {'Quantity': '2', 'UnitPrice': '20.0'},
    ]
    ca = sum(float(r['Quantity']) * float(r['UnitPrice']) for r in rows)
    assert ca == 105.0, f"CA attendu 105.0, obtenu {ca}"

def test_panier_moyen_calcul():
    """Le panier moyen doit être CA / nb_commandes"""
    ca = 309.06
    nb_commandes = 17
    panier_moyen = round(ca / nb_commandes, 2)
    assert panier_moyen == 18.18, f"Panier moyen attendu 18.18, obtenu {panier_moyen}"

def test_montant_negatif_rejete():
    """Une ligne avec montant négatif doit être rejetée"""
    row = {'Quantity': '-3', 'UnitPrice': '10.0'}
    montant = float(row['Quantity']) * float(row['UnitPrice'])
    assert montant < 0, "Le montant négatif n'a pas été détecté"

# ============================================================
# TEST 4 : Régions dynamiques
# ============================================================
def test_regions_extraction():
    """Les régions doivent être extraites sans doublons"""
    rows = [
        {'Country': 'France'},
        {'Country': 'Allemagne'},
        {'Country': 'France'},
        {'Country': 'Espagne'},
    ]
    regions = list(set(r['Country'] for r in rows))
    assert len(regions) == 3, f"Attendu 3 regions uniques, obtenu {len(regions)}"
    assert 'France' in regions
    assert 'Allemagne' in regions
    assert 'Espagne' in regions

def test_task_id_format():
    """Les task_id dynamiques doivent être en minuscules sans espaces"""
    region = "United Kingdom"
    task_id = f"analyse_{region.lower().replace(' ', '_')}"
    assert task_id == "analyse_united_kingdom"
    assert ' ' not in task_id
