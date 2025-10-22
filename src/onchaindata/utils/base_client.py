from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Optional, Any, Dict, List
import os


class BaseDatabaseClient(ABC):
    """Abstract base class for database clients with common patterns."""

    def __init__(self):
        """Initialize with connection parameters."""
        self.connection_params = self._build_connection_params()
        self._engine = None

    @abstractmethod
    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment or config."""
        pass

    @abstractmethod
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        pass

    @abstractmethod
    def get_dlt_destination(self):
        """Get DLT destination for this database."""
        pass

    @staticmethod
    def _get_env_var(key: str, default: str = None) -> str:
        """Helper method to get environment variables."""
        return os.getenv(key, default)
