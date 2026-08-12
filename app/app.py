import os
import json
import streamlit as st

from databricks import sql
from databricks.sdk.core import Config
from openai import OpenAI


# ============================================================
# 1. PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="MEX PHARMA AI Assistant",
    page_icon="💊",
    layout="wide"
)

st.title("💊 MEX PHARMA")
st.subheader("Sales & Inventory AI Assistant")

st.caption(
    "AI-powered assistant for pharmaceutical sales, "
    "inventory and replenishment analysis."
)

st.divider()


# ============================================================
# 2. DATABRICKS AUTHENTICATION
# ============================================================

# Databricks Apps provides authentication automatically
# through the application's service principal.

cfg = Config()


def get_connection():

    server_hostname = cfg.host

    if server_hostname.startswith("https://"):
        server_hostname = server_hostname.replace("https://", "")

    return sql.connect(
        server_hostname=server_hostname,
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )


# ============================================================
# 3. GENERIC SQL EXECUTOR
# ============================================================

def execute_query(query):

    connection = get_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(query)

        columns = [
            column[0]
            for column in cursor.description
        ]

        rows = cursor.fetchall()

        return [
            dict(zip(columns, row))
            for row in rows
        ]

    finally:

        cursor.close()
        connection.close()


# ============================================================
# 4. SALES TOOL
# ============================================================

def get_top_products(limit=10, region=None):

    region_filter = ""

    if region:

        safe_region = region.replace("'", "''")

        region_filter = f"""
        WHERE LOWER(r.region_name) = LOWER('{safe_region}')
        """

    query = f"""
        SELECT
            s.brand_name,
            SUM(s.units_sold) AS units_sold,
            ROUND(SUM(s.revenue), 2) AS total_revenue

        FROM pharma_demo.gold.sales_daily s

        LEFT JOIN pharma_demo.gold.dim_region r
            ON s.region_key = r.region_key

        {region_filter}

        GROUP BY
            s.brand_name

        ORDER BY
            total_revenue DESC

        LIMIT {int(limit)}
    """

    return execute_query(query)


# ============================================================
# 5. INVENTORY TOOL
# ============================================================

def get_inventory_status(
    brand=None,
    region=None,
    limit=10
):

    filters = []

    if brand:

        safe_brand = brand.replace("'", "''")

        filters.append(
            f"LOWER(i.brand_name) LIKE LOWER('%{safe_brand}%')"
        )

    if region:

        safe_region = region.replace("'", "''")

        filters.append(
            f"LOWER(r.region_name) = LOWER('{safe_region}')"
        )

    where_clause = ""

    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    query = f"""
        SELECT
            i.brand_name,
            i.warehouse_name,
            r.region_name,
            i.inventory_units,
            i.avg_daily_demand,
            i.days_of_inventory,
            i.stock_status

        FROM pharma_demo.gold.current_inventory i

        LEFT JOIN pharma_demo.gold.dim_region r
            ON i.region_key = r.region_key

        {where_clause}

        ORDER BY
            i.inventory_units ASC

        LIMIT {int(limit)}
    """

    return execute_query(query)


# ============================================================
# 6. REPLENISHMENT RISK TOOL
# ============================================================

def get_replenishment_risk(
    priority="CRITICAL",
    region=None,
    limit=10
):

    safe_priority = priority.replace("'", "''")

    filters = [
        f"""
        UPPER(s.replenishment_priority)
        =
        UPPER('{safe_priority}')
        """
    ]

    if region:

        safe_region = region.replace("'", "''")

        filters.append(
            f"LOWER(r.region_name) = LOWER('{safe_region}')"
        )

    where_clause = (
        "WHERE " +
        " AND ".join(filters)
    )

    query = f"""
        SELECT
            s.brand_name,
            s.warehouse_name,
            r.region_name,
            s.inventory_units,
            s.avg_daily_demand,
            s.days_of_inventory,
            s.replenishment_priority

        FROM pharma_demo.gold.stock_risk s

        LEFT JOIN pharma_demo.gold.dim_region r
            ON s.region_key = r.region_key

        {where_clause}

        ORDER BY
            s.days_of_inventory ASC

        LIMIT {int(limit)}
    """

    return execute_query(query)


# ============================================================
# 7. SALES BY REGION
# ============================================================

def get_sales_by_region():

    query = """
        SELECT
            r.region_name,
            ROUND(SUM(s.revenue), 2) AS total_revenue,
            SUM(s.units_sold) AS units_sold

        FROM pharma_demo.gold.sales_daily s

        LEFT JOIN pharma_demo.gold.dim_region r
            ON s.region_key = r.region_key

        GROUP BY
            r.region_name

        ORDER BY
            total_revenue DESC
    """

    return execute_query(query)


# ============================================================
# 8. OPENAI CLIENT
# ============================================================

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)


# ============================================================
# 9. SIMPLE AGENT ROUTER
# ============================================================

def ask_agent(question):

    question_lower = question.lower()

    # --------------------------------------------------------
    # INVENTORY / REPLENISHMENT QUESTIONS
    # --------------------------------------------------------

    if (
        "critical" in question_lower
        or "replenish" in question_lower
        or "replenishment" in question_lower
        or "restock" in question_lower
        or "stockout" in question_lower
        or "inventory risk" in question_lower
    ):

        data = get_replenishment_risk(
            priority="CRITICAL",
            limit=10
        )

        tool_used = "Replenishment Risk Tool"

    # --------------------------------------------------------
    # TOP PRODUCTS / SALES
    # --------------------------------------------------------

    elif (
        "top product" in question_lower
        or "best selling" in question_lower
        or "best-selling" in question_lower
        or "highest revenue product" in question_lower
    ):

        data = get_top_products(
            limit=10
        )

        tool_used = "Sales Performance Tool"

    # --------------------------------------------------------
    # REGIONAL SALES
    # --------------------------------------------------------

    elif (
        "region" in question_lower
        or "regional" in question_lower
    ):

        data = get_sales_by_region()

        tool_used = "Regional Sales Tool"

    # --------------------------------------------------------
    # GENERAL INVENTORY
    # --------------------------------------------------------

    elif (
        "inventory" in question_lower
        or "stock" in question_lower
    ):

        data = get_inventory_status(
            limit=10
        )

        tool_used = "Inventory Status Tool"

    else:

        return """
I can currently help you analyze:

- Sales performance
- Top-selling products
- Regional sales
- Current inventory
- Stockout risk
- Replenishment priorities

Try asking:

**Which products require the most urgent replenishment?**
"""

    # --------------------------------------------------------
    # SEND TOOL RESULT TO LLM
    # --------------------------------------------------------

    context = {
        "tool_used": tool_used,
        "data": data
    }

    prompt = f"""
You are the MEX PHARMA Sales & Inventory AI Assistant.

You help business users understand pharmaceutical sales,
inventory and replenishment information.

Answer the user's question using ONLY the supplied analytical data.

RULES:

1. Never invent numerical values.
2. Only use information contained in the supplied data.
3. Be concise and business-oriented.
4. Mention product, warehouse and region when relevant.
5. For inventory risk, prioritize products with fewer
   days of inventory.
6. Clearly explain why an item requires attention.
7. If the supplied data cannot answer the question,
   clearly say so.

USER QUESTION:

{question}

ANALYTICAL DATA:

{json.dumps(context, default=str)}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",

        messages=[
            {
                "role": "system",
                "content": (
                    "You are a pharmaceutical sales and "
                    "inventory analytics assistant."
                )
            },
            {
                "role": "user",
                "content": prompt
            }
        ],

        temperature=0
    )

    return response.choices[0].message.content


# ============================================================
# 10. CHAT SESSION
# ============================================================

if "messages" not in st.session_state:

    st.session_state.messages = []


# ============================================================
# 11. DISPLAY PREVIOUS MESSAGES
# ============================================================

for message in st.session_state.messages:

    with st.chat_message(message["role"]):

        st.markdown(
            message["content"]
        )


# ============================================================
# 12. SUGGESTED QUESTIONS
# ============================================================

st.markdown("### Suggested questions")

col1, col2, col3 = st.columns(3)

with col1:

    st.info(
        "Which products require the most urgent replenishment?"
    )

with col2:

    st.info(
        "What are the top-selling products?"
    )

with col3:

    st.info(
        "Which region generates the most revenue?"
    )


# ============================================================
# 13. CHAT INPUT
# ============================================================

question = st.chat_input(
    "Ask about sales, inventory or replenishment..."
)


if question:

    # Save user message

    st.session_state.messages.append(
        {
            "role": "user",
            "content": question
        }
    )

    with st.chat_message("user"):

        st.markdown(question)


    # Generate assistant response

    with st.chat_message("assistant"):

        with st.spinner(
            "Analyzing MEX PHARMA data..."
        ):

            try:

                answer = ask_agent(question)

                st.markdown(answer)

            except Exception as e:

                answer = (
                    "I couldn't complete the analysis. "
                    f"Application error: {str(e)}"
                )

                st.error(answer)


    # Save assistant response

    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": answer
        }
    )