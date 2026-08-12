# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingesta Bronze desde openFDA (API REST)
# MAGIC
# MAGIC ## Objetivo
# MAGIC Ingerir datos farmacéuticos provenientes de la API pública openFDA y almacenarlos en la capa Bronze del Lakehouse.
# MAGIC
# MAGIC ## Fuente
# MAGIC - openFDA REST API
# MAGIC - Azure Data Factory
# MAGIC - Azure Data Lake Storage Gen2
# MAGIC
# MAGIC ## Proceso
# MAGIC 1. Lectura del JSON recibdo desde ADLS.
# MAGIC 2. Explosión del arreglo "results".
# MAGIC 3. Conservación de los datos con estructura cercana a la fuente.
# MAGIC 4. Escritura en Delta Lake.
# MAGIC 5. Registro de la tabla dentro de Unity Catalog.
# MAGIC
# MAGIC ## Salida
# MAGIC pharma_demo.bronze.openfda_drug_labels
# MAGIC
# MAGIC ## Capacidades demostradas
# MAGIC - Integración API > Azure > Databricks
# MAGIC - Manejo de JSON anidado
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - Unity Catalog
# MAGIC - Arquitectura Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Librerías

# COMMAND ----------

from pyspark.sql.functions import (
    explode,
    col,
    element_at,
    to_date,
    current_timestamp,
    expr
)

import pandas as pd
from pyspark.sql.functions import col, when, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lectura desde ADLS

# COMMAND ----------

bronze_path = "abfss://bronze@storagedbtest26.dfs.core.windows.net/openfda/"

df_raw = spark.read.json(bronze_path)

# display(df_raw.limit(10))

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------



df_drugs = (
    df_raw
    .select(explode(col("results")).alias("drug"))
)

print("Número de medicamentos:", df_drugs.count())

display(df_drugs)

# COMMAND ----------

df_drugs.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpieza de datos

# COMMAND ----------

df_products = (
    df_drugs
    .select(
        col("drug.id").alias("drug_id"),
        col("drug.set_id").alias("set_id"),

        # Safe parsing: malformed dates become NULL
        expr(
            "try_to_date(drug.effective_time, 'yyyyMMdd')"
        ).alias("effective_date"),

        element_at(
            col("drug.openfda.product_ndc"), 1
        ).alias("product_ndc"),

        element_at(
            col("drug.openfda.brand_name"), 1
        ).alias("brand_name"),

        element_at(
            col("drug.openfda.generic_name"), 1
        ).alias("generic_name"),

        element_at(
            col("drug.openfda.manufacturer_name"), 1
        ).alias("manufacturer"),

        element_at(
            col("drug.openfda.product_type"), 1
        ).alias("product_type"),

        element_at(
            col("drug.openfda.route"), 1
        ).alias("route"),

        element_at(
            col("drug.openfda.substance_name"), 1
        ).alias("substance_name"),

        current_timestamp().alias("processed_at")
    )
)
display(df_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validaciones

# COMMAND ----------

print("Total:", df_products.count())

print(
    "Sin NDC:",
    df_products.filter(col("product_ndc").isNull()).count()
)

print(
    "Sin brand:",
    df_products.filter(col("brand_name").isNull()).count()
)

print(
    "Sin manufacturer:",
    df_products.filter(col("manufacturer").isNull()).count()
)

# COMMAND ----------

df_quality = (
    df_products
    .withColumn(
        "data_quality_status",
        when(
            col("product_ndc").isNull() |
            col("brand_name").isNull() |
            col("manufacturer").isNull(),
            lit("REJECTED")
        ).otherwise(lit("VALID"))
    )
)

df_quality.groupBy("data_quality_status").count().show()

# COMMAND ----------

df_valid = (
    df_quality
    .filter(col("data_quality_status") == "VALID")
    .dropDuplicates(["product_ndc"])
)

df_rejected = (
    df_quality
    .filter(col("data_quality_status") == "REJECTED")
)

print("Valid products:", df_valid.count())
print("Rejected records:", df_rejected.count())

# COMMAND ----------

df_valid = (
    df_quality
    .filter(col("data_quality_status") == "VALID")
    .filter(
        col("product_type").isin(
            "HUMAN OTC DRUG",
            "HUMAN PRESCRIPTION DRUG"
        )
    )
    .dropDuplicates(["product_ndc"])
)

df_rejected = (
    df_quality
    .filter(col("data_quality_status") == "REJECTED")
)

print("Valid products after deduplication:", df_valid.count())
print("Rejected records:", df_rejected.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Creación de schemas en Catálogo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS pharma_demo.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS pharma_demo.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS pharma_demo.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS IN pharma_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escritura de tablas Delta

# COMMAND ----------

(
    df_drugs
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.bronze.openfda_drug_labels")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_records
# MAGIC FROM pharma_demo.bronze.openfda_drug_labels;

# COMMAND ----------

(
    df_valid
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.silver.dim_product")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pharma_demo.silver.dim_product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     product_ndc,
# MAGIC     brand_name,
# MAGIC     generic_name,
# MAGIC     manufacturer,
# MAGIC     product_type,
# MAGIC     route,
# MAGIC     substance_name,
# MAGIC     effective_date
# MAGIC FROM pharma_demo.silver.dim_product
# MAGIC LIMIT 20;

# COMMAND ----------

(
    df_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.silver.rejected_products")
)