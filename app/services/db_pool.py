import os
import psycopg2
from psycopg2 import pool
import logging

logger = logging.getLogger(__name__)

class DatabasePool:
    """
    PostgreSQL connection pool for efficient connection management.
    
    Task 7: Implements connection pooling to support 8-12 concurrent uploads
    without exhausting database connections.
    
    Configuration:
    - Min connections: 2 (always available)
    - Max connections: 20 (limit concurrent DB access)
    - Connection timeout: 30 seconds
    - Auto-close on return to pool
    """
    
    _pool = None
    _initialized = False
    
    @classmethod
    def initialize(cls):
        """Initialize the connection pool (called once at app startup)"""
        if cls._initialized:
            logger.warning("Database pool already initialized")
            return
        
        try:
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not set")
            
            cls._pool = psycopg2.pool.SimpleConnectionPool(
                minconn=2,  # Minimum connections (always available)
                maxconn=20,  # Maximum connections (limit concurrent access)
                dsn=database_url,
                connect_timeout=30  # 30 second timeout for new connections
            )
            
            cls._initialized = True
            logger.info("✓ Database connection pool initialized: min=2, max=20, timeout=30s")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}", exc_info=True)
            raise
    
    @classmethod
    def get_connection(cls):
        """
        Get a connection from the pool.
        
        Returns:
            psycopg2 connection object
        
        Raises:
            psycopg2.pool.PoolError: If no connections available (pool exhausted)
            RuntimeError: If pool not initialized
        """
        if not cls._initialized or cls._pool is None:
            raise RuntimeError(
                "Database pool not initialized. Call DatabasePool.initialize() first."
            )
        
        try:
            conn = cls._pool.getconn()
            if conn is None:
                raise psycopg2.pool.PoolError("Connection pool exhausted (20 connections in use)")
            
            return conn
            
        except psycopg2.pool.PoolError as e:
            logger.error(f"Connection pool exhausted: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting connection from pool: {e}", exc_info=True)
            raise
    
    @classmethod
    def return_connection(cls, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: psycopg2 connection to return
        """
        if not cls._initialized or cls._pool is None:
            logger.warning("Attempting to return connection but pool not initialized")
            if conn:
                try:
                    conn.close()
                except:
                    pass
            return
        
        try:
            if conn:
                cls._pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}", exc_info=True)
    
    @classmethod
    def close_all(cls):
        """Close all connections in the pool (called at shutdown)"""
        if cls._pool:
            try:
                cls._pool.closeall()
                logger.info("✓ Database connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}", exc_info=True)
            finally:
                cls._pool = None
                cls._initialized = False
