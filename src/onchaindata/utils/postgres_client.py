from typing import Optional, Any, Dict
import os
from contextlib import contextmanager

import psycopg
from sqlalchemy import create_engine
import dlt

from .base_client import BaseDatabaseClient


class PostgresClient(BaseDatabaseClient):
    """Object-oriented PostgreSQL client for database operations."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
    ):
        """
        Initialize PostgresDestination with database configuration.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        super().__init__()

    @classmethod
    def from_env(cls) -> "PostgresClient":
        """Create from environment variables"""
        return cls(
            host=cls._get_env_var("POSTGRES_HOST"),
            port=int(cls._get_env_var("POSTGRES_PORT", "5432")),
            database=cls._get_env_var("POSTGRES_DB"),
            user=cls._get_env_var("POSTGRES_USER"),
            password=cls._get_env_var("POSTGRES_PASSWORD"),
        )

    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from instance variables."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }

    @contextmanager
    def get_connection(self):
        """Context manager for PostgreSQL connections."""
        conn = None
        try:
            conn = psycopg.connect(**self.connection_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def get_dlt_destination(self) -> Any:
        """Return DLT destination for pipeline operations."""
        params = self.connection_params
        connection_url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['dbname']}"
        return dlt.destinations.postgres(connection_url)
