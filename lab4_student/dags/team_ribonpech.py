from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
import os

# Remplacez 'votre_nom' par le vrai nom choisi pour votre équipe [cite: 441, 511]
TEAM_NAME = "ribonpech" 

default_args = {
    'owner': TEAM_NAME,
    'start_date': datetime(2026, 6, 1), # Aligné avec les dates du projet [cite: 4, 434]
    'retries': 1,
}

with DAG(
    dag_id=f"team_{TEAM_NAME}",
    default_args=default_args,
    schedule_interval=None, # Déclenché manuellement ou via l'UI pour les tests 
    catchup=False,
) as dag:

    # 1. Tâche Sensor : Attend le fichier CSV du jour [cite: 435, 450, 465]
    wait_for_vendor_file = FileSensor(
        task_id="wait_for_vendor_file",
        filepath=f"data/incoming/transactions_{{{{ ds }}}}.csv", # Injecte la date logique ds [cite: 435, 450]
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60,
    )

    # 2. Tâche Ingestion : Convertit le CSV (Bronze) en Parquet (Silver) [cite: 436, 457, 465]
    @task(task_id="ingest_csv_to_parquet")
    def ingest_csv_to_parquet(**context):
        ds = context['ds']
        input_path = f"data/incoming/transactions_{ds}.csv"
        output_dir = f"data/raw/dt={ds}/"
        
        print(f"📥 Ingestion du fichier : {input_path}")
        print(f"💾 Sauvegarde en Parquet Silver : {output_dir}")
        
        # Simulation de l'ingestion (DuckDB s'en charge en tâche de fond dans le kit) [cite: 457]
        os.makedirs(output_dir, exist_ok=True)
        # Ici votre code ou appel d'ingestion fourni par le kit [cite: 457]
        return output_dir

    # 3. Tâche de Validation (Votre 5ème tâche ajoutée pour la Track Q) [cite: 77, 187, 439, 463]
    @task(task_id="validate_silver_data")
    def validate_silver_data(**context):
        ds = context['ds']
        silver_path = f"data/raw/dt={ds}/"
        
        print(f"🔍 Vérification de la qualité des données Silver dans : {silver_path}")
        
        # C'est ici qu'on testera si les données contiennent des erreurs (comme le --corrupt) [cite: 187, 454]
        # Pour le squelette de la partie A, on valide simplement par un print [cite: 517]
        print("✅ Données Silver validées (Prêt pour Spark).")

    # 4. Tâche Spark : Lance les transformations PySpark (Gold) [cite: 77, 431, 437, 457, 465]
    @task(task_id="run_spark_kpi_job")
    def run_spark_kpi_job(**context):
        ds = context['ds']
        print(f"⚡ Initialisation de la SparkSession locale pour le jour : {ds} [cite: 131]")
        
        # On importe dynamiquement votre module Spark d'équipe [cite: 43, 77, 441]
        import importlib
        spark_module = importlib.import_module(f"include.team_{TEAM_NAME}_spark")
        
        # Appel de la fonction principale de votre script Spark [cite: 477]
        spark_module.run_daily(ds)

    # 5. Tâche Export : Génère le rapport final JSON pour le dashboard (Serve) [cite: 34, 438, 457, 465]
    @task(task_id="export_summary_dashboard")
    def export_summary_dashboard(**context):
        ds = context['ds']
        report_path = f"data/reports/dashboard_{ds}.json"
        
        print(f"📊 Génération du rapport de restitution final : {report_path}")
        
        # Code temporaire pour le squelette (génère un JSON simple requis) [cite: 438, 516]
        import json
        os.makedirs("data/reports/", exist_ok=True)
        summary_data = {
            "status": "success",
            "execution_date": ds,
            "team": TEAM_NAME,
            "msg": "KPIs mis à jour avec succès."
        }
        with open(report_path, "w") as f:
            json.dump(summary_data, f)

    # --- Ordonnancement des tâches (Le Squelette filaire) --- [cite: 517]
    wait_for_vendor_file >> ingest_csv_to_parquet() >> validate_silver_data() >> run_spark_kpi_job() >> export_summary_dashboard()