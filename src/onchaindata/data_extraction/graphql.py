#!/usr/bin/env python3
"""
GraphQL data extractor with batch and streaming modes.

This module extracts data from a GraphQL endpoint and either:
1. Saves to Parquet file (batch mode)
2. Pushes directly to database (streaming mode)
"""

import time, logging
from typing import Optional, Dict, Any, List

import requests
import polars as pl

from onchaindata.data_pipeline import Loader

logger = logging.getLogger(__name__)


class GraphQLBatch:
    """Fetches data from GraphQL endpoint with streaming and batch modes."""

    def __init__(
        self,
        endpoint: str,
        query: str,
    ):
        """
        Initialize GraphQL fetcher.

        Args:
            endpoint: GraphQL endpoint URL
            query: GraphQL query string
        """
        self.endpoint = endpoint
        self.query = query
        self.session = requests.Session()

    def extract(self) -> Dict[str, Any]:
        """
        Execute GraphQL query and return results.

        Returns:
            GraphQL response data
        """
        payload = {
            "query": self.query,
        }

        response = self.session.post(
            self.endpoint,
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

        data = response.json()
        if "errors" in data:
            raise ValueError(f"GraphQL errors: {data['errors']}")

        return data.get("data", {})

    def extract_to_dataframe(self, table_name: str) -> pl.DataFrame:
        """
        Extract data and convert to Polars DataFrame.

        Args:
            table_name: Name of the table/query result to extract

        Returns:
            Polars DataFrame with fetched data
        """
        data = self.extract()
        if table_name not in data:
            raise ValueError(
                f"Table '{table_name}' not found in response. Available: {list(data.keys())}"
            )

        records = data[table_name]
        if not records:
            return pl.DataFrame()

        return pl.DataFrame(records)


class GraphQLStream:
    """Fetches data from GraphQL endpoint in streaming mode with polling."""

    def __init__(
        self,
        endpoint: str,
        table_name: str,
        fields: List[str],
        poll_interval: int = 5,
    ):
        """
        Initialize streaming fetcher.

        Args:
            endpoint: GraphQL endpoint URL
            table_name: Name of the table/query to fetch
            fields: List of fields to fetch
            poll_interval: Seconds to wait between polls
        """
        self.endpoint = endpoint
        self.table_name = table_name
        self.fields = fields
        self.poll_interval = poll_interval
        self.last_seen_block_number: Optional[int] = None

    def _build_query(self, where_clause: Optional[str] = None) -> str:
        """
        Build GraphQL query dynamically.

        Args:
            where_clause: Optional WHERE clause filter

        Returns:
            GraphQL query string
        """
        fields_str = "\n    ".join(self.fields)
        where_str = f", where: {{{where_clause}}}" if where_clause else ""

        query = f"""
            query {{
            {self.table_name}(
                order_by: {{blockNumber: desc}}
                {where_str}
            ) {{
                {fields_str}
            }}
            }}
        """.strip()
        return query

    def _get_last_block_number_from_db(
        self, loader: Loader, schema: str, table_name: str
    ) -> Optional[int]:
        """
        Query database to get the last maximum block number.

        Args:
            loader: Loader instance for database operations
            schema: Target schema name
            table_name: Target table name

        Returns:
            Maximum block number, or None if table is empty or doesn't exist
        """
        try:
            # Get database connection from loader's client
            with loader.client.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                    SELECT MAX(block_number)::INTEGER
                    FROM {schema}.{table_name}
                    """
                    cur.execute(query)
                    result = cur.fetchone()
                    return result[0] if result and result[0] is not None else None
        except Exception as e:
            return None

    def stream(self, loader: Loader, schema: str, table_name: str):
        """
        Stream data from GraphQL endpoint to database.

        Automatically resumes from the last record in the database based on block number.

        Args:
            loader: Loader instance for database operations
            schema: Target schema name
            table_name: Target table name
        """
        logger.info(f"Starting streaming mode: {self.endpoint}")
        logger.info(f"Target: {schema}.{table_name}")

        self.last_seen_block_number = self._get_last_block_number_from_db(
            loader, schema, table_name
        )
        logger.info(f"Last seen block number: {self.last_seen_block_number}")
        if self.last_seen_block_number is not None:
            logger.info(f"Resuming from block number = {self.last_seen_block_number}")
        else:
            logger.info("No existing data found, starting fresh")

        poll_count = 1
        total_records = 0

        try:
            while True:
                # Build query with current state
                where_clause = None
                if self.last_seen_block_number is not None:
                    # Build WHERE clause for incremental fetch
                    where_clause = (
                        f"blockNumber: {{_gt: {self.last_seen_block_number}}}"
                    )

                query = self._build_query(where_clause)

                extractor = GraphQLBatch(
                    endpoint=self.endpoint,
                    query=query,
                )

                # Fetch data
                df = extractor.extract_to_dataframe(self.table_name)
                if not df.is_empty():
                    records_count = len(df)
                    total_records += records_count

                    # Load DataFrame directly to database
                    loader.load_dataframe(
                        df=df,
                        schema=schema,
                        table_name=table_name,
                        write_disposition="append",
                    )

                    # Update last seen value
                    if "blockNumber" in df.columns:
                        self.last_seen_block_number = df["blockNumber"].max()
                        logger.info(
                            f"[Poll {poll_count}] - {records_count} new records, new max block number in the database: {self.last_seen_block_number}"
                        )

                    poll_count += 1
                else:
                    logger.info(
                        f"[Poll {poll_count}] - No records fetched, waiting for next poll..."
                    )
                    poll_count += 1

                # Wait before next poll
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info(f"\n\nStreaming stopped by user.")
            logger.info(f"Total polls: {poll_count}")
            logger.info(f"Total records: {total_records}")
