# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Generación de inventarios - Capa Silver
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar los datos crudos provenientes de openFDA en un catálogo de productos confiable y preparado para consumo analítico.
# MAGIC
# MAGIC ## Principales transformaciones
# MAGIC - Flatten de estructuras JSON.
# MAGIC - Conversión segura de fechas.
# MAGIC - Selección de campos relevantes.
# MAGIC - Validaciones de valores nulos.
# MAGIC - Separación de registros válidos y rechazados.
# MAGIC - Deduplicación mediante "product_ndc".
# MAGIC
# MAGIC ## Resultados de calidad
# MAGIC - 261,646 registros procesados.
# MAGIC - 86,684 registros inicialmente válidos.
# MAGIC - 174,962 registros rechazados.
# MAGIC - 85,724 productos únicos válidos después de deduplicación.
# MAGIC
# MAGIC ## Salidas
# MAGIC - pharma_demo.silver.dim_product
# MAGIC - pharma_demo.silver.rejected_products
# MAGIC
# MAGIC ## Capacidades demostradas
# MAGIC - Data Quality
# MAGIC - Data cleansing
# MAGIC - Deduplicación
# MAGIC - Tratamiento de errores
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - Arquitectura Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Librerías

# COMMAND ----------

import pandas as pd
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    sequence, 
    explode, 
    to_date, 
    lit, 
    date_sub, 
    current_date
)
from pyspark.sql.functions import (
    explode,
    col,
    element_at,
    to_date,
    current_timestamp,
    expr,
    col,
    when,
    lit,
    row_number
)

from pyspark.sql.functions import (
    rand,
    round,
    when,
    floor,
    dayofweek,
    month,
    monotonically_increasing_id,
    avg
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Muestra de productos demo

# COMMAND ----------

df_products_demo = (
    spark.table("pharma_demo.silver.dim_product")
    .select(
        "product_ndc",
        "brand_name",
        "generic_name",
        "manufacturer",
        "product_type",
        "route"
    )
    .orderBy("product_ndc")
    .limit(1000)
)

display(df_products_demo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Window Function para agregar una clave de producto

# COMMAND ----------

w = Window.orderBy("product_ndc")

df_products_demo = (
    df_products_demo
    .withColumn(
        "product_key",
        row_number().over(w)
    )
)

display(df_products_demo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Creación de regiones

# COMMAND ----------

regions = [
    (1, "North"),
    (2, "South"),
    (3, "Central"),
    (4, "West"),
    (5, "East")
]

warehouses = [
    (1, "WH_NORTH", 1),
    (2, "WH_SOUTH", 2),
    (3, "WH_CENTRAL", 3),
    (4, "WH_WEST", 4),
    (5, "WH_EAST", 5)
]

df_regions = spark.createDataFrame(
    regions,
    ["region_key", "region_name"]
)

df_warehouses = spark.createDataFrame(
    warehouses,
    ["warehouse_key", "warehouse_name", "region_key"]
)

# COMMAND ----------

df_regions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("pharma_demo.silver.dim_region")

df_warehouses.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("pharma_demo.silver.dim_warehouse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Creación de calendario

# COMMAND ----------


df_dates = (
    spark.range(1)
    .select(
        explode(
            sequence(
                date_sub(current_date(), 179),
                current_date()
            )
        ).alias("date")
    )
)

display(df_dates)

# COMMAND ----------

df_dates.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("pharma_demo.silver.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Creación de tabla de ventas

# COMMAND ----------

df_sales_base = (
    df_products_demo
    .crossJoin(df_dates)
    .crossJoin(df_regions)
)

# COMMAND ----------

df_sales = (
    df_sales_base
    .withColumn(
        "demand_segment",
        when(col("product_key") <= 200, "HIGH")
        .when(col("product_key") <= 600, "MEDIUM")
        .otherwise("LOW")
    )
    .withColumn(
        "base_demand",
        when(col("demand_segment") == "HIGH", 400)
        .when(col("demand_segment") == "MEDIUM", 200)
        .otherwise(8)
    )
)

# COMMAND ----------

df_val = df_sales.groupBy("demand_segment").count()
display(df_val)

# COMMAND ----------

df_sales = (
    df_sales
    .withColumn(
        "weekday_factor",
        when(dayofweek("date").isin(1, 7), 0.8)
        .otherwise(1.0)
    )
    .withColumn(
        "region_factor",
        when(col("region_key") == 3, 1.25)
        .when(col("region_key") == 1, 1.15)
        .when(col("region_key") == 5, 1.05)
        .otherwise(0.9)
    )
    .withColumn(
        "random_factor",
        0.7 + rand(seed=42) * 0.6
    )
)

# COMMAND ----------

df_sales = (
    df_sales
    .withColumn(
        "quantity",
        floor(
            col("base_demand") *
            col("weekday_factor") *
            col("region_factor") *
            col("random_factor")
        ).cast("int")
    )
)

# COMMAND ----------

df_sales = (
    df_sales
    .withColumn(
        "unit_price",
        round(
            50 + (col("product_key") % 20) * 15 + rand(seed=7) * 20,
            2
        )
    )
    .withColumn(
        "revenue",
        round(col("quantity") * col("unit_price"), 2)
    )
)

# COMMAND ----------

df_sales = (
    df_sales
    .withColumn(
        "sale_id",
        monotonically_increasing_id()
    )
    .select(
        "sale_id",
        "date",
        "product_key",
        "product_ndc",
        "brand_name",
        "region_key",
        "quantity",
        "unit_price",
        "revenue",
        "demand_segment"
    )
)

# COMMAND ----------

display(df_sales)

# COMMAND ----------

print("Sales rows:", df_sales.count())

# COMMAND ----------

df_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("pharma_demo.silver.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Creación de demanda promedio

# COMMAND ----------

df_avg_demand = (
    df_sales
    .groupBy(
        "product_key",
        "product_ndc",
        "brand_name",
        "region_key"
    )
    .agg(
        avg("quantity").alias("avg_daily_demand")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Creación de inventario

# COMMAND ----------

df_inventory = (
    df_avg_demand
    .join(
        df_warehouses,
        on="region_key",
        how="inner"
    )
    .crossJoin(df_dates)
)

# COMMAND ----------

df_inventory = (
    df_inventory
    .withColumn(
        "inventory_units",
        floor(
            col("avg_daily_demand") *
            (
                3 + rand(seed=100) * 25
            )
        ).cast("int")
    )
)

# COMMAND ----------

df_inventory = (
    df_inventory
    .withColumn(
        "days_of_inventory",
        round(
            col("inventory_units") /
            col("avg_daily_demand"),
            2
        )
    )
    .withColumn(
        "stock_status",
        when(col("days_of_inventory") < 5, "HIGH_RISK")
        .when(col("days_of_inventory") < 10, "MEDIUM_RISK")
        .otherwise("HEALTHY")
    )
)

# COMMAND ----------

df_inventory = df_inventory.select(
    "date",
    "product_key",
    "product_ndc",
    "brand_name",
    "region_key",
    "warehouse_key",
    "warehouse_name",
    "inventory_units",
    "avg_daily_demand",
    "days_of_inventory",
    "stock_status"
)

# COMMAND ----------

display(df_inventory)

# COMMAND ----------

df_inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("pharma_demo.silver.fact_inventory")