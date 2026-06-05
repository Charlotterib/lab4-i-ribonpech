from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum, count
import os

# 💡 Transformation 1 : Lecture et filtrage de base
def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Transformation 1 - Lecture des données Silver."""
    print(f"--- Spark Transformation 1 : Lecture du jour {logical_date} ---")
    input_path = f"data/raw/dt={logical_date}/"
    
    # Si le dossier existe, on lit le Parquet, sinon fallback de sécurité pour le squelette
    if os.path.exists(input_path):
        df = spark.read.parquet(input_path)
    else:
        df = spark.createDataFrame([("TX001", "Electro", "FR", 120.0)], ["tx_id", "category", "country", "amount_eur"])
    return df

# 💡 Transformation 2 : Enrichissement des données
def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """Transformation 2 - Enrichissement / Dérivations métiers."""
    print("--- Spark Transformation 2 : Enrichissement métier ---")
    # Pour le squelette initial, on retourne le DataFrame tel quel
    return df

# 💡 Transformation 3 : Agrégation des indicateurs finaux (Gold)
def transform_3(df: DataFrame) -> DataFrame:
    """Transformation 3 - Agrégations des KPIs demandés par le Retail ---."""
    print("--- Spark Transformation 3 : Agrégation KPI par Catégorie/Pays ---")
    # Groupement par catégorie et pays pour calculer le CA et le volume
    df_kpi = df.groupBy("category", "country") \
               .agg(
                   sum("amount_eur").alias("total_revenue"),
                   count("tx_id").alias("transaction_count")
               )
    return df_kpi

# 🚀 Fonction principale appelée depuis Airflow @task
def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Initialise la SparkSession, chaîne les transformations et écrit les outputs."""
    # Création de la session locale
    spark = SparkSession.builder \
        .appName(f"Spark_KPI_Job_{logical_date}") \
        .master("local[*]") \
        .getOrCreate()
        
    output_curated_dir = f"data/curated/dt={logical_date}/"
    
    try:
        # Exécution en chaîne des 3 transformations
        df_raw = transform_1(spark, logical_date)
        df_enriched = transform_2(spark, df_raw, logical_date)
        df_final = transform_3(df_enriched)
        
        # Sauvegarde du résultat en Parquet Gold (Idempotent grâce à 'overwrite')
        print(f"💾 Écriture des données Gold : {output_curated_dir}")
        df_final.write.mode("overwrite").parquet(output_curated_dir)
        
        # Retourne un dictionnaire de statut pour Airflow
        return {"status": "success", "logical_date": logical_date}
        
    finally:
        # On ferme TOUJOURS la session proprement, même en cas de crash
        spark.stop()