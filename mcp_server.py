from fastmcp import FastMCP
import requests

BASE_URL = "http://localhost:8000"

mcp = FastMCP(
    "Keynes QA Advertiser Intelligence"
)

# =========================================================
# CURRENT DATE
# =========================================================

@mcp.tool()
def get_current_date():

    response = requests.get(
        f"{BASE_URL}/current-date"
    )

    return response.json()

# =========================================================
# DATASETS
# =========================================================

@mcp.tool()
def get_available_datasets():

    response = requests.get(
        f"{BASE_URL}/datasets"
    )

    return response.json()

# =========================================================
# TABLE SCHEMA
# =========================================================

@mcp.tool()
def get_table_schema(table_name: str):

    """
    Returns actual Athena schema
    for a dataset.

    Use before querying if unsure
    about column names.
    """

    response = requests.get(
        f"{BASE_URL}/schema/{table_name}"
    )

    return response.json()

# =========================================================
# QUERY ATHENA
# =========================================================

@mcp.tool()
def query_athena(sql: str):

    print("\n[MCP TOOL] query_athena called")
    print("\nGenerated SQL:")
    print(sql)

    response = requests.post(

        f"{BASE_URL}/query",

        json={
            "sql": sql
        }

    )

    return response.json()

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    print(
        "\nStarting Keynes QA MCP Server..."
    )

    mcp.run(

        transport="sse",

        host="0.0.0.0",

        port=9000

    )
