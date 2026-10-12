"""
PostgreSQL Connection Pool & Management Engine (V7 Upgrade)
============================================================
Handles thread-safe PostgreSQL connection pooling. 
Provides clean abstractions for connection retrieval, release, and cursor operations.
"""
import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from logger import get_logger

# Load environment configs
load_dotenv()
log = get_logger("DB_Postgres")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "trading_brain")
DB_USER = os.getenv("DB_USER", "quant_operator")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Global pool instance
_pool_instance = None

def init_pool():
    """Initializes the thread-safe connection pool."""
    global _pool_instance
    if _pool_instance is None:
        try:
            _pool_instance = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=20,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            log.info("PostgreSQL thread-safe connection pool initialized successfully.")
        except Exception as e:
            log.critical(f"Failed to initialize PostgreSQL connection pool: {e}")
            raise

class PooledConnectionProxy:
    """Proxy that automatically returns a connection to the pool when close() is called."""
    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool
        
    def __getattr__(self, name):
        return getattr(self._conn, name)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def close(self):
        if self._pool and self._conn:
            try:
                self._pool.putconn(self._conn)
            except Exception as e:
                log.error(f"Error returning connection to pool: {e}")
            self._conn = None
            self._pool = None

def get_connection():
    """Get a connection from the connection pool, wrapped in a proxy."""
    global _pool_instance
    if _pool_instance is None:
        init_pool()
    conn = _pool_instance.getconn()
    return PooledConnectionProxy(conn, _pool_instance)

def release_connection(conn):
    """Release a connection back to the pool."""
    global _pool_instance
    if _pool_instance and conn:
        try:
            # If it's a proxy, extract the underlying connection
            raw_conn = conn._conn if isinstance(conn, PooledConnectionProxy) else conn
            if raw_conn:
                _pool_instance.putconn(raw_conn)
        except Exception as e:
            log.error(f"Error returning connection to pool: {e}")

def execute_query(query: str, params: tuple = None, fetch: bool = False, commit: bool = True):
    """
    Executes a query safely using the connection pool.
    Auto-commits transactions and auto-releases connection back to the pool.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        result = None
        if fetch:
            result = cursor.fetchall()
            
        if commit:
            conn.commit()
            
        return result
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"PostgreSQL Execution Error: {e} | Query: {query[:100]}...")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)
