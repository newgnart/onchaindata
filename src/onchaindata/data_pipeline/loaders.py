#!/usr/bin/env python3

"""Unified loader for loading Parquet files to various destinations."""

import os
from pathlib import Path
from typing import Union

import dlt
from dlt.sources.filesystem import filesystem, read_parquet
import polars as pl

from ..utils import PostgresClient, SnowflakeClient


class Loader:
    """Unified loader class for loading data to different destinations."""

    def __init__(
        self,
        client: Union[PostgresClient, SnowflakeClient],
    ):
        """
        Initialize the Loader.

        Args:
            client: Database client instance (PostgresClient or SnowflakeClient)
                   If not provided, will be created from environment variables
        """
        self.client = client

    def load_parquet(
        self,
        file_path: Union[str, Path],
        schema: str,
        table_name: str,
        write_disposition: str = "append",
    ):
        """
        Load Parquet file to the configured destination using DLT.

        Args:
            file_path: Path to the Parquet file
            schema: Target schema name
            table_name: Target table name
            write_disposition: How to handle existing data ("append", "replace", "merge")

        Returns:
            DLT pipeline run result
        """
        # Convert Path to string if needed
        if isinstance(file_path, Path):
            file_path = file_path.as_posix()

        # Create filesystem source
        fs_source = filesystem(bucket_url=".", file_glob=file_path)

        # Read parquet with special handling for logs table
        parquet_resource = fs_source | read_parquet()
        if table_name == "logs":
            parquet_resource.apply_hints(
                columns={"topics": {"data_type": "json", "nullable": True}}
            )

        # Create pipeline with destination-specific configuration
        pipeline = dlt.pipeline(
            pipeline_name="parquet_loader",
            destination=self.client.get_dlt_destination(),
            dataset_name=schema,
        )

        # Load data
        result = pipeline.run(
            parquet_resource,
            table_name=table_name,
            write_disposition=write_disposition,
        )

        return result

    def load_dataframe(
        self,
        df: pl.DataFrame,
        schema: str,
        table_name: str,
        write_disposition: str = "append",
    ):
        """
        Load Polars DataFrame directly to the database using DLT.

        Args:
            df: Polars DataFrame to load
            schema: Target schema name
            table_name: Target table name
            write_disposition: How to handle existing data ("append", "replace", "merge")

        Returns:
            DLT pipeline run result
        """
        # Convert DataFrame to list of dicts for DLT
        data = df.to_dicts()

        # Create a DLT resource from the data
        resource = dlt.resource(data, name=table_name)

        # Apply special hints for logs table
        if table_name == "logs":
            resource.apply_hints(
                columns={"topics": {"data_type": "json", "nullable": True}}
            )

        # Create pipeline with destination-specific configuration
        pipeline = dlt.pipeline(
            pipeline_name="dataframe_loader",
            destination=self.client.get_dlt_destination(),
            dataset_name=schema,
        )

        # Load data
        result = pipeline.run(
            resource,
            table_name=table_name,
            write_disposition=write_disposition,
        )

        return result
