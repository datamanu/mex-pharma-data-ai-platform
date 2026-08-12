# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Agente de Ventas e Inventarios
# MAGIC
# MAGIC ## Objetivo
# MAGIC Permitir que usuarios de negocio consulten información de ventas e inventario utilizando lenguaje natural.
# MAGIC
# MAGIC ## Arquitectura
# MAGIC Usuario
# MAGIC > LLM
# MAGIC > LangChain Agent
# MAGIC > Tool Calling
# MAGIC > Databricks SQL Warehouse
# MAGIC > Unity Catalog
# MAGIC > respuesta
# MAGIC
# MAGIC ## Herramientas del agente
# MAGIC - top_products_tool
# MAGIC - inventory_status_tool
# MAGIC - replenishment_risk_tool
# MAGIC - regional_sales_tool
# MAGIC
# MAGIC ## Diseño de seguridad
# MAGIC El modelo no recibe acceso SQL irrestricto.
# MAGIC
# MAGIC En su lugar, utiliza herramientas controladas que encapsulan consultas específicas sobre las tablas Gold.
# MAGIC
# MAGIC ## Ejemplos de preguntas
# MAGIC - Which products require the most urgent replenishment?
# MAGIC - Which critical products are in the East region?
# MAGIC - What are the top 5 products by revenue?
# MAGIC - Which region generates the most revenue?
# MAGIC
# MAGIC ## Deployment
# MAGIC El agente se despliega como una Databricks App utilizando Streamlit.
# MAGIC
# MAGIC ## Capacidades demostradas
# MAGIC - LangChain
# MAGIC - Tool Calling
# MAGIC - OpenAI
# MAGIC - Databricks SQL
# MAGIC - Unity Catalog
# MAGIC - Databricks Apps
# MAGIC - Streamlit
# MAGIC - Gestión segura de secretos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Librerías

# COMMAND ----------

from pyspark.sql import functions as F
from langchain.agents import create_agent
import os
from langchain_openai import ChatOpenAI
from langchain.tools import tool
from langchain.agents import create_agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creación de funciones delimitantes del contexto

# COMMAND ----------

def get_replenishment_risk(
    priority="CRITICAL",
    region=None,
    limit=10
):

    stock = spark.table("pharma_demo.gold.stock_risk")
    regions = spark.table("pharma_demo.gold.dim_region")

    df = (
        stock
        .join(
            regions,
            on="region_key",
            how="left"
        )
    )

    if priority:
        df = df.filter(
            F.upper(F.col("replenishment_priority")) == priority.upper()
        )

    if region:
        df = df.filter(
            F.lower(F.col("region_name")) == region.lower()
        )

    result = (
        df
        .orderBy(F.col("days_of_inventory").asc())
        .limit(limit)
        .select(
            "brand_name",
            "warehouse_name",
            "region_name",
            "inventory_units",
            "avg_daily_demand",
            "days_of_inventory",
            "replenishment_priority"
        )
    )

    return [row.asDict() for row in result.collect()]

# COMMAND ----------

# get_replenishment_risk("CRITICAL", 5)

# COMMAND ----------

def get_top_products(limit=10, region=None):

    sales = spark.table("pharma_demo.gold.sales_daily")
    regions = spark.table("pharma_demo.gold.dim_region")

    df = (
        sales
        .join(
            regions,
            on="region_key",
            how="left"
        )
    )

    if region:
        df = df.filter(
            F.lower(F.col("region_name")) == region.lower()
        )

    result = (
        df
        .groupBy(
            "product_key",
            "brand_name"
        )
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.sum("units_sold").alias("units_sold")
        )
        .orderBy(F.col("total_revenue").desc())
        .limit(limit)
    )

    return [row.asDict() for row in result.collect()]

# COMMAND ----------

# get_top_products(5)

# COMMAND ----------

# get_top_products(5, "Central")

# COMMAND ----------

def get_inventory_status(brand=None, region=None, limit=10):

    inventory = spark.table("pharma_demo.gold.current_inventory")
    regions = spark.table("pharma_demo.gold.dim_region")

    df = (
        inventory
        .join(
            regions,
            on="region_key",
            how="left"
        )
    )

    if brand:
        df = df.filter(
            F.lower(F.col("brand_name")).contains(brand.lower())
        )

    if region:
        df = df.filter(
            F.lower(F.col("region_name")) == region.lower()
        )

    result = (
        df
        .orderBy(F.col("inventory_units").asc())
        .limit(limit)
        .select(
            "brand_name",
            "warehouse_name",
            "region_name",
            "inventory_units",
            "avg_daily_demand",
            "days_of_inventory",
            "stock_status"
        )
    )

    return [row.asDict() for row in result.collect()]

# COMMAND ----------

# get_inventory_status(region="East", limit=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Secreto

# COMMAND ----------

openai_key = dbutils.secrets.get(
    catalog="pharma_demo",
    schema="gold",
    key="openai_api_key"
)

os.environ["OPENAI_API_KEY"] = openai_key

print("OpenAI API key loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. LLM

# COMMAND ----------

llm = ChatOpenAI(
    model="gpt-5-mini",
    temperature=0
)

response = llm.invoke(
    "Reply only with: CONNECTION SUCCESSFUL"
)

print(response.content)

# COMMAND ----------

# %pip install -U \
#   langchain==1.2.0 \
#   langchain-core==1.2.5 \
#   langchain-openai==1.1.6 \
#   langgraph==1.0.5 \
#   langgraph-prebuilt==1.0.5 \
#   langgraph-checkpoint==3.0.1 \
#   langgraph-sdk==0.3.1

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. LangChain Tools

# COMMAND ----------

@tool
def replenishment_risk_tool(
    priority: str = "CRITICAL",
    region: str | None = None,
    limit: int = 10
) -> list:
    """
    Find products with inventory replenishment risk.

    Use this tool for questions about:
    - critical inventory
    - stockout risk
    - replenishment priority
    - products that need restocking
    - low days of inventory
    """
    return get_replenishment_risk(
        priority=priority,
        region=region,
        limit=limit
    )

# COMMAND ----------

@tool
def top_products_tool(
    limit: int = 10,
    region: str | None = None
) -> list:
    """
    Find the top selling products ranked by revenue.

    Use this tool for questions about:
    - best selling products
    - highest revenue products
    - product sales rankings
    - top products by region
    """
    return get_top_products(
        limit=limit,
        region=region
    )

# COMMAND ----------

@tool
def inventory_status_tool(
    brand: str | None = None,
    region: str | None = None,
    limit: int = 10
) -> list:
    """
    Get current inventory information.

    Use this tool for questions about:
    - current stock
    - inventory levels
    - days of inventory
    - stock status
    - inventory for a particular brand or region
    """
    return get_inventory_status(
        brand=brand,
        region=region,
        limit=limit
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Creación del Agente

# COMMAND ----------

tools = [
    replenishment_risk_tool,
    top_products_tool,
    inventory_status_tool
]

agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt="""
You are a Sales & Inventory AI Assistant for a pharmaceutical company.

You answer business questions using the available tools.

Rules:
- Use tools whenever the user asks about sales, products, inventory,
  replenishment, stock levels or stockout risk.
- Never invent numerical values.
- Base answers only on tool results.
- Clearly mention region and warehouse when relevant.
- For inventory risk, prioritize products with fewer days of inventory.
- Keep answers concise and business-oriented.
- If data is not available, say so.
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pruebas

# COMMAND ----------

result = agent.invoke({
    "messages": [
        {
            "role": "user",
            "content": "Which 5 products require the most urgent replenishment?"
        }
    ]
})

print(result["messages"][-1].content)

# COMMAND ----------

questions = [
    "What are the top 5 products by revenue in the Central region?",
    "Which critical inventory products are in the East region?",
    "Show me the 5 products with the lowest inventory in the North region."
]

for question in questions:

    result = agent.invoke({
        "messages": [
            {
                "role": "user",
                "content": question
            }
        ]
    })

    print("\nQUESTION:", question)
    print("ANSWER:", result["messages"][-1].content)