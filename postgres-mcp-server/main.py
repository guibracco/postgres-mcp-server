from typing import List, Dict, Any
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initializes your MCP server instance. It's used to register your tools.
mcp = FastMCP("postgres-server")

# Database connection configuration from environment variables
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "practice_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# TODO: Implement a second MCP tool called `execute_sql`
# This function should:
#  - Take a SQL query as input (string)
#  - Run the query against the Postgres database
#  - Return the rows as a list of dictionaries (column_name → value)
# Hint: Use the same psycopg2 connection pattern shown in `get_schema`.
@mcp.tool()
async def execute_sql(query: str) -> List[Dict[str, Any]]:
    """Execute a SQL query and return rows as a list of dictionaries."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return [dict(row) for row in rows]


# TODO: Implement a third MCP tool called `list_tables`
# This function should:
#  - Take no inputs
#  - Return the list of table names available in the current database
# Hint: Query `information_schema.tables` and filter for `table_schema = 'public'`.
@mcp.tool()
async def list_tables() -> List[str]:
    """Return the list of tables in the public schema."""
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [row[0] for row in rows]

@mcp.tool()
async def get_schema(table: str) -> List[Dict]:
    """Return column names and types for a given table."""
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (table,))
            rows = [{"column": r[0], "type": r[1]} for r in cur.fetchall()]
    return rows

def main():
    # Run MCP server using stdio transport for AI assistant integration
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
