# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Modelo comercial y capa Gold
# MAGIC
# MAGIC ## Objetivo
# MAGIC Construir un escenario de negocio de ventas e inventario utilizando productos reales provenientes de openFDA y datos comerciales sintéticos.
# MAGIC
# MAGIC ## Alcance
# MAGIC Se seleccionan 1,000 productos para simular:
# MAGIC - ventas diarias
# MAGIC - comportamiento regional
# MAGIC - demanda
# MAGIC - inventario
# MAGIC - almacenes
# MAGIC - riesgo de desabasto
# MAGIC
# MAGIC ## Lógica de simulación
# MAGIC La demanda utiliza factores controlados como:
# MAGIC - segmento de demanda
# MAGIC - demanda base
# MAGIC - día de la semana
# MAGIC - región
# MAGIC - variabilidad aleatoria
# MAGIC
# MAGIC El inventario se relaciona con la demanda diaria promedio para calcular:
# MAGIC - unidades disponibles
# MAGIC - días de inventario
# MAGIC - estado de stock
# MAGIC - prioridad de reabastecimiento
# MAGIC
# MAGIC ## Salidas Gold
# MAGIC - sales_daily
# MAGIC - current_inventory
# MAGIC - stock_risk
# MAGIC - product_performance
# MAGIC
# MAGIC ## Capacidades demostradas
# MAGIC - Modelado dimensional
# MAGIC - Transformaciones Spark
# MAGIC - Agregaciones
# MAGIC - Métricas de negocio
# MAGIC - Data serving
# MAGIC - Integración con Power BI

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Librerías

# COMMAND ----------

from pyspark.sql.functions import (
    sum,
    avg,
    count,
    round,
    col,
    max,
    when
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga de tablas silver

# COMMAND ----------

df_sales = spark.table("pharma_demo.silver.fact_sales")
df_inventory = spark.table("pharma_demo.silver.fact_inventory")
df_products = spark.table("pharma_demo.silver.dim_product")
df_regions = spark.table("pharma_demo.silver.dim_region")
df_warehouses = spark.table("pharma_demo.silver.dim_warehouse")

# COMMAND ----------

print("Sales:", df_sales.count())
print("Inventory:", df_inventory.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ventas diarias

# COMMAND ----------

df_sales_daily = (
    df_sales
    .groupBy(
        "date",
        "product_key",
        "product_ndc",
        "brand_name",
        "region_key"
    )
    .agg(
        sum("quantity").alias("units_sold"),
        round(sum("revenue"), 2).alias("revenue"),
        round(avg("unit_price"), 2).alias("avg_unit_price")
    )
)

display(df_sales_daily)

# COMMAND ----------

(
    df_sales_daily
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.gold.sales_daily")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inventario Actual

# COMMAND ----------

latest_date = (
    df_inventory
    .agg(max("date").alias("latest_date"))
    .collect()[0]["latest_date"]
)

print("Latest inventory date:", latest_date)

# COMMAND ----------

df_current_inventory = (
    df_inventory
    .filter(col("date") == latest_date)
)

display(df_current_inventory)

# COMMAND ----------

(
    df_current_inventory
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.gold.current_inventory")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Riesgo de desabasto

# COMMAND ----------

df_stock_risk = (
    df_current_inventory
    .withColumn(
        "replenishment_priority",
        when(col("days_of_inventory") < 3, "CRITICAL")
        .when(col("days_of_inventory") < 5, "HIGH")
        .when(col("days_of_inventory") < 10, "MEDIUM")
        .otherwise("LOW")
    )
)

display(df_stock_risk)

# COMMAND ----------

(
    df_stock_risk
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.gold.stock_risk")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     brand_name,
# MAGIC     warehouse_name,
# MAGIC     inventory_units,
# MAGIC     avg_daily_demand,
# MAGIC     days_of_inventory,
# MAGIC     replenishment_priority
# MAGIC FROM pharma_demo.gold.stock_risk
# MAGIC WHERE replenishment_priority IN ('CRITICAL', 'HIGH')
# MAGIC ORDER BY days_of_inventory ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Performance por Producto

# COMMAND ----------

df_product_sales = (
    df_sales
    .groupBy(
        "product_key",
        "product_ndc",
        "brand_name"
    )
    .agg(
        sum("quantity").alias("total_units_sold"),
        round(sum("revenue"), 2).alias("total_revenue"),
        round(avg("quantity"), 2).alias("avg_daily_sales")
    )
)

# COMMAND ----------

df_product_inventory = (
    df_current_inventory
    .groupBy(
        "product_key",
        "product_ndc",
        "brand_name"
    )
    .agg(
        sum("inventory_units").alias("current_inventory"),
        round(avg("days_of_inventory"), 2).alias("avg_days_inventory")
    )
)

# COMMAND ----------

df_product_performance = (
    df_product_sales
    .join(
        df_product_inventory,
        ["product_key", "product_ndc", "brand_name"],
        "inner"
    )
)

# COMMAND ----------

df_product_performance = (
    df_product_performance
    .withColumn(
        "inventory_health",
        when(col("avg_days_inventory") < 5, "HIGH_RISK")
        .when(col("avg_days_inventory") < 10, "MEDIUM_RISK")
        .otherwise("HEALTHY")
    )
)

# COMMAND ----------

display(df_product_performance)

# COMMAND ----------

(
    df_product_performance
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("pharma_demo.gold.product_performance")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Creación de vistas para Power BI

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW pharma_demo.gold.dim_product_bi AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     product_key,
# MAGIC     product_ndc,
# MAGIC     brand_name
# MAGIC FROM pharma_demo.gold.sales_daily;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     brand_name,
# MAGIC     total_units_sold,
# MAGIC     total_revenue,
# MAGIC     current_inventory,
# MAGIC     avg_days_inventory
# MAGIC FROM pharma_demo.gold.product_performance
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     brand_name,
# MAGIC     warehouse_name,
# MAGIC     inventory_units,
# MAGIC     avg_daily_demand,
# MAGIC     days_of_inventory,
# MAGIC     replenishment_priority
# MAGIC FROM pharma_demo.gold.stock_risk
# MAGIC WHERE replenishment_priority = 'CRITICAL'
# MAGIC ORDER BY days_of_inventory;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     r.region_name,
# MAGIC     ROUND(SUM(s.revenue), 2) AS revenue,
# MAGIC     SUM(s.quantity) AS units_sold
# MAGIC FROM pharma_demo.silver.fact_sales s
# MAGIC
# MAGIC JOIN pharma_demo.silver.dim_region r
# MAGIC ON s.region_key = r.region_key
# MAGIC
# MAGIC GROUP BY r.region_name
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT replenishment_priority, COUNT(*) AS products
# MAGIC FROM pharma_demo.gold.stock_risk
# MAGIC GROUP BY replenishment_priority
# MAGIC ORDER BY products DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW pharma_demo.gold.dim_product AS
# MAGIC SELECT
# MAGIC     product_ndc,
# MAGIC     brand_name,
# MAGIC     generic_name,
# MAGIC     manufacturer,
# MAGIC     product_type,
# MAGIC     route,
# MAGIC     substance_name
# MAGIC FROM pharma_demo.silver.dim_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW pharma_demo.gold.dim_region AS
# MAGIC SELECT *
# MAGIC FROM pharma_demo.silver.dim_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW pharma_demo.gold.dim_warehouse AS
# MAGIC SELECT *
# MAGIC FROM pharma_demo.silver.dim_warehouse;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW pharma_demo.gold.dim_date AS
# MAGIC SELECT *
# MAGIC FROM pharma_demo.silver.dim_date;