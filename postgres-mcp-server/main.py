from typing import List, Dict, Any, Optional
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

# Basic statistics tool: row count, column count, and estimated size of a table
@mcp.tool()
async def get_table_stats(table: str) -> Dict[str, Any]:
    """Get basic statistics about a table: row count, column count, and estimated size."""
    sql_count = f"SELECT COUNT(*) FROM {table}"
    sql_cols = """
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = 'public'
    """
    sql_size = """
        SELECT pg_size_pretty(pg_total_relation_size(%s)) as size
    """
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_count)
            row_count = cur.fetchone()[0]
            
            cur.execute(sql_cols, (table,))
            col_count = cur.fetchone()[0]
            
            cur.execute(sql_size, (table,))
            size = cur.fetchone()[0]
    
    return {
        "table": table,
        "row_count": row_count,
        "column_count": col_count,
        "size": size
    }

# Check for null values in each column of a table
@mcp.tool()
async def check_null_values(table: str) -> List[Dict[str, Any]]:
    """Check for null values in each column of a table."""
    # First get all columns
    sql_cols = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_cols, (table,))
            columns = [row[0] for row in cur.fetchall()]
            
            # Get total row count
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cur.fetchone()[0]
            
            results = []
            for col in columns:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                null_count = cur.fetchone()[0]
                null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
                
                results.append({
                    "column": col,
                    "null_count": null_count,
                    "null_percentage": round(null_pct, 2),
                    "non_null_count": total_rows - null_count
                })
    
    return results

# Get statistics for a specific column in a table
@mcp.tool()
async def get_column_stats(table: str, column: str) -> Dict[str, Any]:
    """Get statistics for a specific column: min, max, avg (for numeric), and distinct count."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Get column data type
            cur.execute("""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (table, column))
            data_type = cur.fetchone()[0]
            
            # Get distinct count
            cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table}")
            distinct_count = cur.fetchone()[0]
            
            stats = {
                "column": column,
                "data_type": data_type,
                "distinct_count": distinct_count
            }
            
            # If numeric, get min, max, avg
            if data_type in ('integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision', 'decimal'):
                cur.execute(f"SELECT MIN({column}), MAX({column}), AVG({column}) FROM {table}")
                min_val, max_val, avg_val = cur.fetchone()
                stats.update({
                    "min": min_val,
                    "max": max_val,
                    "avg": round(float(avg_val), 2) if avg_val else None
                })
            
    return stats

# Preview the first N rows of a table (default 5)
@mcp.tool()
async def preview_data(table: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Preview the first N rows of a table (default 5)."""
    sql = f"SELECT * FROM {table} LIMIT %s"
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
    return [dict(row) for row in rows]

# Check for duplicate rows in a table
@mcp.tool()
async def check_duplicate_rows(table: str, columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """Count duplicate rows for a table, optionally scoped to specific columns."""
    columns = columns or []
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cur.fetchone()[0]

            if columns:
                column_list = ", ".join(columns)
                distinct_expr = f"({column_list})" if len(columns) > 1 else columns[0]
                cur.execute(f"SELECT COUNT(DISTINCT {distinct_expr}) FROM {table}")
                distinct_rows = cur.fetchone()[0]
            else:
                cur.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table}) AS distinct_rows")
                distinct_rows = cur.fetchone()[0]

    duplicate_rows = total_rows - distinct_rows
    duplicate_pct = (duplicate_rows / total_rows * 100) if total_rows > 0 else 0

    return {
        "table": table,
        "columns": columns if columns else None,
        "total_rows": total_rows,
        "distinct_rows": distinct_rows,
        "duplicate_rows": duplicate_rows,
        "duplicate_percentage": round(duplicate_pct, 2)
    }

# Return top values and counts for a column
@mcp.tool()
async def column_value_counts(table: str, column: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Return top values and counts for a column."""
    sql = f"""
        SELECT {column} AS value, COUNT(*) AS count
        FROM {table}
        GROUP BY {column}
        ORDER BY count DESC
        LIMIT %s
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
    return [dict(row) for row in rows]

# Check for empty string values in a text column
@mcp.tool()
async def check_empty_strings(table: str, column: str) -> Dict[str, Any]:
    """Check for empty string values in a text column."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s AND table_schema = 'public'
            """, (table, column))
            row = cur.fetchone()
            data_type = row[0] if row else None

            cur.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cur.fetchone()[0]

            if data_type in ("text", "character varying", "character"):
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} = ''")
                empty_count = cur.fetchone()[0]
                empty_pct = (empty_count / total_rows * 100) if total_rows > 0 else 0
                note = None
            else:
                empty_count = 0
                empty_pct = 0
                note = "column is not a text type"

    return {
        "table": table,
        "column": column,
        "data_type": data_type,
        "empty_string_count": empty_count,
        "empty_string_percentage": round(empty_pct, 2),
        "note": note
    }

def main():
    # Run MCP server using stdio transport for AI assistant integration
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
