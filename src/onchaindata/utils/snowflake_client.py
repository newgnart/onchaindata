import snowflake.connector
import os
from contextlib import contextmanager
from typing import Optional, Dict, Any
import dlt

from .base_client import BaseDatabaseClient


class SnowflakeClient(BaseDatabaseClient):
    """Reusable Snowflake client with connection management."""

    def __init__(
        self,
        account: str = None,
        user: str = None,
        authenticator: str = None,
        private_key_file: str = None,
        warehouse: str = None,
        database: str = None,
        role: str = None,
    ):
        """Initialize Snowflake client with environment configuration."""
        self.account = account
        self.user = user
        self.authenticator = authenticator
        self.private_key_file = private_key_file
        self.warehouse = warehouse
        self.database = database
        self.role = role
        super().__init__()

    @classmethod
    def from_env(cls) -> "SnowflakeClient":
        """Create from environment variables"""
        return cls(
            account=cls._get_env_var("SNOWFLAKE_ACCOUNT"),
            user=cls._get_env_var("SNOWFLAKE_USER"),
            authenticator=cls._get_env_var("SNOWFLAKE_AUTHENTICATOR"),
            private_key_file=cls._get_env_var("SNOWFLAKE_PRIVATE_KEY_FILE"),
            warehouse=cls._get_env_var("SNOWFLAKE_WAREHOUSE"),
            database=cls._get_env_var("SNOWFLAKE_DATABASE"),
            role=cls._get_env_var("SNOWFLAKE_ROLE"),
        )

    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment variables."""
        return {
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
            "private_key_file": self.private_key_file,
            "warehouse": self.warehouse,
            "database": self.database,
            "role": self.role,
        }

    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake connections."""
        conn = None
        try:
            conn = snowflake.connector.connect(**self.connection_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def get_dlt_destination(self):
        """Get DLT destination configuration for Snowflake."""

        # Read private key file and convert to private_key for DLT
        private_key_path = os.path.expanduser(
            self.connection_params["private_key_file"]
        )

        try:
            with open(private_key_path, "r") as key_file:
                private_key_data = key_file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Private key file not found: {private_key_path}")

        # Create credentials object that DLT expects
        credentials = {
            "username": self.connection_params["user"],
            "host": self.connection_params["account"],
            "warehouse": self.connection_params["warehouse"],
            "database": self.connection_params["database"],
            "role": self.connection_params["role"],
            "private_key": private_key_data,
        }

        # Only add private key password if it exists
        if self.connection_params.get("private_key_file_pwd"):
            credentials["private_key_passphrase"] = self.connection_params[
                "private_key_file_pwd"
            ]

        return dlt.destinations.snowflake(credentials=credentials)
