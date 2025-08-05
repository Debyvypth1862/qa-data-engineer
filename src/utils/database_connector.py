"""
Database Connector

This module provides database connection management for various database types
including PostgreSQL, MySQL, SQLite, and other SQL databases.
"""

import asyncio
from typing import Any, Dict, Optional, Union, List
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import structlog
import pandas as pd
from datetime import datetime

logger = structlog.get_logger()

Base = declarative_base()


class DatabaseConnector:
    """
    Database connector for managing connections to various database types.
    
    This class provides functionality to connect to different database types,
    execute queries, and manage database operations for testing purposes.
    """
    
    def __init__(self, config_manager):
        """
        Initialize the database connector.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.connections: Dict[str, Any] = {}
        self.engines: Dict[str, Any] = {}
        
        logger.info("Database connector initialized")
    
    async def get_connection(self, database_config: Dict[str, Any]) -> Any:
        """
        Get database connection based on configuration.
        
        Args:
            database_config: Database configuration dictionary
            
        Returns:
            SQLAlchemy engine instance
        """
        try:
            # Generate connection key
            connection_key = self._generate_connection_key(database_config)
            
            # Check if connection already exists
            if connection_key in self.engines:
                return self.engines[connection_key]
            
            # Create new connection
            engine = await self._create_connection(database_config)
            
            # Store connection
            self.engines[connection_key] = engine
            
            logger.info("Database connection created", connection_key=connection_key)
            
            return engine
            
        except Exception as e:
            logger.error("Failed to get database connection", error=str(e))
            raise
    
    async def _create_connection(self, database_config: Dict[str, Any]) -> Any:
        """
        Create database connection based on configuration.
        
        Args:
            database_config: Database configuration dictionary
            
        Returns:
            SQLAlchemy engine instance
        """
        db_type = database_config.get("type", "postgresql").lower()
        
        if db_type == "postgresql":
            return self._create_postgresql_connection(database_config)
        elif db_type == "mysql":
            return self._create_mysql_connection(database_config)
        elif db_type == "sqlite":
            return self._create_sqlite_connection(database_config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _create_postgresql_connection(self, config: Dict[str, Any]) -> Any:
        """Create PostgreSQL connection."""
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        database = config.get("name", "test_db")
        username = config.get("user", "postgres")
        password = config.get("password", "")
        
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        return create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=config.get("echo", False)
        )
    
    def _create_mysql_connection(self, config: Dict[str, Any]) -> Any:
        """Create MySQL connection."""
        host = config.get("host", "localhost")
        port = config.get("port", 3306)
        database = config.get("name", "test_db")
        username = config.get("user", "root")
        password = config.get("password", "")
        
        connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        
        return create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=config.get("echo", False)
        )
    
    def _create_sqlite_connection(self, config: Dict[str, Any]) -> Any:
        """Create SQLite connection."""
        database_path = config.get("path", ":memory:")
        
        connection_string = f"sqlite:///{database_path}"
        
        return create_engine(
            connection_string,
            echo=config.get("echo", False)
        )
    
    def _generate_connection_key(self, config: Dict[str, Any]) -> str:
        """Generate unique connection key."""
        db_type = config.get("type", "postgresql")
        host = config.get("host", "localhost")
        port = config.get("port", "")
        database = config.get("name", "")
        
        return f"{db_type}_{host}_{port}_{database}"
    
    async def test_connection(self, database_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test database connection.
        
        Args:
            database_config: Database configuration dictionary
            
        Returns:
            Connection test results
        """
        try:
            engine = await self.get_connection(database_config)
            
            # Test connection with simple query
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                result.fetchone()
            
            return {
                "success": True,
                "message": "Database connection successful",
                "database_type": database_config.get("type", "unknown")
            }
            
        except Exception as e:
            logger.error("Database connection test failed", error=str(e))
            return {
                "success": False,
                "error": str(e),
                "database_type": database_config.get("type", "unknown")
            }
    
    async def execute_query(self, database_config: Dict[str, Any], query: str, 
                           params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a database query.
        
        Args:
            database_config: Database configuration dictionary
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query execution results
        """
        try:
            engine = await self.get_connection(database_config)
            
            with engine.connect() as connection:
                if params:
                    result = connection.execute(text(query), params)
                else:
                    result = connection.execute(text(query))
                
                # Fetch results
                if result.returns_rows:
                    rows = result.fetchall()
                    columns = result.keys()
                    
                    return {
                        "success": True,
                        "rows": [dict(zip(columns, row)) for row in rows],
                        "row_count": len(rows),
                        "columns": list(columns)
                    }
                else:
                    return {
                        "success": True,
                        "affected_rows": result.rowcount,
                        "message": "Query executed successfully"
                    }
                    
        except Exception as e:
            logger.error("Query execution failed", error=str(e), query=query)
            return {
                "success": False,
                "error": str(e),
                "query": query
            }
    
    async def execute_query_pandas(self, database_config: Dict[str, Any], query: str, 
                                  params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a database query and return results as pandas DataFrame.
        
        Args:
            database_config: Database configuration dictionary
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Pandas DataFrame with query results
        """
        try:
            engine = await self.get_connection(database_config)
            
            if params:
                df = pd.read_sql(query, engine, params=params)
            else:
                df = pd.read_sql(query, engine)
            
            return df
            
        except Exception as e:
            logger.error("Pandas query execution failed", error=str(e), query=query)
            raise
    
    async def get_table_info(self, database_config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Get table information.
        
        Args:
            database_config: Database configuration dictionary
            table_name: Name of the table
            
        Returns:
            Table information
        """
        try:
            engine = await self.get_connection(database_config)
            
            # Get table metadata
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            
            # Get column information
            columns = []
            for column in table.columns:
                columns.append({
                    "name": column.name,
                    "type": str(column.type),
                    "nullable": column.nullable,
                    "primary_key": column.primary_key,
                    "default": str(column.default) if column.default else None
                })
            
            # Get row count
            with engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
            
            return {
                "success": True,
                "table_name": table_name,
                "columns": columns,
                "row_count": row_count,
                "column_count": len(columns)
            }
            
        except Exception as e:
            logger.error("Failed to get table info", error=str(e), table_name=table_name)
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name
            }
    
    async def create_test_table(self, database_config: Dict[str, Any], table_name: str, 
                               schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a test table with specified schema.
        
        Args:
            database_config: Database configuration dictionary
            table_name: Name of the table to create
            schema: Table schema definition
            
        Returns:
            Table creation results
        """
        try:
            engine = await self.get_connection(database_config)
            
            # Create table definition
            columns = []
            for column_name, column_def in schema.items():
                column_type = column_def.get("type", "String")
                nullable = column_def.get("nullable", True)
                primary_key = column_def.get("primary_key", False)
                
                if column_type.lower() == "integer":
                    col = Column(column_name, Integer, nullable=nullable, primary_key=primary_key)
                elif column_type.lower() == "datetime":
                    col = Column(column_name, DateTime, nullable=nullable, primary_key=primary_key)
                else:
                    col = Column(column_name, String, nullable=nullable, primary_key=primary_key)
                
                columns.append(col)
            
            # Create table
            table = Table(table_name, MetaData(), *columns)
            table.create(engine)
            
            logger.info("Test table created", table_name=table_name)
            
            return {
                "success": True,
                "table_name": table_name,
                "message": "Test table created successfully"
            }
            
        except Exception as e:
            logger.error("Failed to create test table", error=str(e), table_name=table_name)
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name
            }
    
    async def insert_test_data(self, database_config: Dict[str, Any], table_name: str, 
                              data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Insert test data into a table.
        
        Args:
            database_config: Database configuration dictionary
            table_name: Name of the table
            data: List of data dictionaries to insert
            
        Returns:
            Insert operation results
        """
        try:
            engine = await self.get_connection(database_config)
            
            # Convert data to DataFrame and insert
            df = pd.DataFrame(data)
            df.to_sql(table_name, engine, if_exists='append', index=False)
            
            logger.info("Test data inserted", table_name=table_name, row_count=len(data))
            
            return {
                "success": True,
                "table_name": table_name,
                "inserted_rows": len(data),
                "message": "Test data inserted successfully"
            }
            
        except Exception as e:
            logger.error("Failed to insert test data", error=str(e), table_name=table_name)
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name
            }
    
    async def cleanup_test_data(self, database_config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Clean up test data from a table.
        
        Args:
            database_config: Database configuration dictionary
            table_name: Name of the table to clean up
            
        Returns:
            Cleanup operation results
        """
        try:
            engine = await self.get_connection(database_config)
            
            # Delete all data from table
            with engine.connect() as connection:
                result = connection.execute(text(f"DELETE FROM {table_name}"))
                deleted_rows = result.rowcount
            
            logger.info("Test data cleaned up", table_name=table_name, deleted_rows=deleted_rows)
            
            return {
                "success": True,
                "table_name": table_name,
                "deleted_rows": deleted_rows,
                "message": "Test data cleaned up successfully"
            }
            
        except Exception as e:
            logger.error("Failed to cleanup test data", error=str(e), table_name=table_name)
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name
            }
    
    async def drop_test_table(self, database_config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Drop a test table.
        
        Args:
            database_config: Database configuration dictionary
            table_name: Name of the table to drop
            
        Returns:
            Drop operation results
        """
        try:
            engine = await self.get_connection(database_config)
            
            # Drop table
            with engine.connect() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                connection.commit()
            
            logger.info("Test table dropped", table_name=table_name)
            
            return {
                "success": True,
                "table_name": table_name,
                "message": "Test table dropped successfully"
            }
            
        except Exception as e:
            logger.error("Failed to drop test table", error=str(e), table_name=table_name)
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name
            }
    
    async def get_database_stats(self, database_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Args:
            database_config: Database configuration dictionary
            
        Returns:
            Database statistics
        """
        try:
            engine = await self.get_connection(database_config)
            db_type = database_config.get("type", "postgresql").lower()
            
            stats = {
                "database_type": db_type,
                "connection_pool_size": engine.pool.size(),
                "connection_pool_checked_in": engine.pool.checkedin(),
                "connection_pool_checked_out": engine.pool.checkedout(),
                "connection_pool_overflow": engine.pool.overflow()
            }
            
            # Get database-specific statistics
            if db_type == "postgresql":
                stats.update(await self._get_postgresql_stats(engine))
            elif db_type == "mysql":
                stats.update(await self._get_mysql_stats(engine))
            
            return {
                "success": True,
                "stats": stats
            }
            
        except Exception as e:
            logger.error("Failed to get database stats", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _get_postgresql_stats(self, engine) -> Dict[str, Any]:
        """Get PostgreSQL-specific statistics."""
        try:
            with engine.connect() as connection:
                # Get database size
                size_result = connection.execute(text("""
                    SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
                """))
                db_size = size_result.scalar()
                
                # Get table count
                table_result = connection.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """))
                table_count = table_result.scalar()
                
                return {
                    "database_size": db_size,
                    "table_count": table_count
                }
        except Exception as e:
            logger.error("Failed to get PostgreSQL stats", error=str(e))
            return {}
    
    async def _get_mysql_stats(self, engine) -> Dict[str, Any]:
        """Get MySQL-specific statistics."""
        try:
            with engine.connect() as connection:
                # Get database size
                size_result = connection.execute(text("""
                    SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS db_size_mb
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                """))
                db_size = size_result.scalar()
                
                # Get table count
                table_result = connection.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                """))
                table_count = table_result.scalar()
                
                return {
                    "database_size_mb": db_size,
                    "table_count": table_count
                }
        except Exception as e:
            logger.error("Failed to get MySQL stats", error=str(e))
            return {}
    
    def close_all_connections(self) -> None:
        """Close all database connections."""
        try:
            for engine in self.engines.values():
                engine.dispose()
            
            self.engines.clear()
            self.connections.clear()
            
            logger.info("All database connections closed")
            
        except Exception as e:
            logger.error("Failed to close database connections", error=str(e)) 