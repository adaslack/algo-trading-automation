"""
Unified Database Adapter (V7 Migration - DuckDB/PostgreSQL Upgrade)
====================================================================
Dynamically routes queries to PostgreSQL or DuckDB based on the environment.
Stripped of legacy SQLite code to enforce elite systematic research storage.
"""
import os
import duckdb
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.duckdb')

def get_connection():
    """Returns an active DuckDB or PostgreSQL connection based on the environment."""
    if os.getenv("DB_HOST"):
        import db_postgres
        return db_postgres.get_connection()
    else:
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
        return duckdb.connect(DUCKDB_PATH)

def execute_query(query: str, params: tuple = None, fetch: bool = False, commit: bool = True):
    """
    Executes a query dynamically translating placeholders if PostgreSQL is active,
    routing local queries strictly through high-performance DuckDB.
    """
    use_postgres = bool(os.getenv("DB_HOST"))
    
    if use_postgres:
        import db_postgres
        # Dynamically translate ? to %s for PostgreSQL psycopg2 parameter binding
        translated_query = query.replace('?', '%s')
        if "INSERT OR IGNORE" in translated_query:
            translated_query = translated_query.replace("INSERT OR IGNORE", "INSERT")
            if "ON CONFLICT" not in translated_query:
                translated_query += " ON CONFLICT DO NOTHING"
        return db_postgres.execute_query(translated_query, params, fetch, commit)
    else:
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
        conn = duckdb.connect(DUCKDB_PATH)
        translated_query = query
        if "INSERT OR IGNORE" in translated_query:
            translated_query = translated_query.replace("INSERT OR IGNORE", "INSERT")
            if "ON CONFLICT" not in translated_query:
                translated_query += " ON CONFLICT DO NOTHING"
                
        cursor = conn.cursor()
        try:
            cursor.execute(translated_query, params or ())
            result = None
            if fetch:
                result = cursor.fetchall()
            return result
        except Exception as e:
            raise e
        finally:
            cursor.close()
            conn.close()

