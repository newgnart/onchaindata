"""Etherscan API client implementation."""

import os, json, csv, logging

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal, Tuple
from dataclasses import dataclass

import polars as pl
import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client import paginators

from .base import BaseAPIClient, BaseSource, APIConfig
from .base import APIError
from ..utils.chain import chainid_data

logger = logging.getLogger(__name__)


@dataclass
class APIs:
    """API-specific settings."""

    etherscan_api_key: Optional[str] = None
    coingecko_api_key: Optional[str] = None

    # Rate limits (requests per second)
    etherscan_rate_limit: float = 5.0
    coingecko_rate_limit: float = 5.0
    defillama_rate_limit: float = 10.0

    def __post_init__(self):
        # Load from environment if not provided
        if self.etherscan_api_key is None:
            self.etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")


class APIUrls:
    """API endpoint URLs."""

    ETHERSCAN = "https://api.etherscan.io/v2/api"


class EtherscanClient(BaseAPIClient):
    """Etherscan API client implementation."""

    @classmethod
    def _load_chainid_mapping(cls) -> Dict[str, int]:
        """Load chain name to chainid mapping from utils.chain."""
        return chainid_data

    def __init__(
        self,
        chainid: Optional[int] = None,
        chain: Optional[str] = None,
        api_key: Optional[str] = None,
        calls_per_second: float = 5.0,
    ):
        # Validate that exactly one of chainid or chain is provided
        if chainid is not None and chain is not None:
            raise ValueError(
                "Cannot specify both 'chainid' and 'chain' parameters. Use only one."
            )
        if chainid is None and chain is None:
            raise ValueError("Must specify either 'chainid' or 'chain' parameter.")

        # Resolve chainid from chain name if needed
        chainid_mapping = self._load_chainid_mapping()
        if chain is not None:
            if chain not in chainid_mapping:
                available_chains = ", ".join(sorted(chainid_mapping.keys()))
                raise ValueError(
                    f"Unknown chain '{chain}'. Available chains: {available_chains}"
                )
            chainid = chainid_mapping[chain]

        self.chainid = chainid
        chain_name_mapping = {v: k for k, v in chainid_mapping.items()}
        self.chain = chain_name_mapping.get(chainid, "unknown")

        # Create APIs instance to load environment variables
        apis = APIs()
        config = APIConfig(
            base_url=APIUrls.ETHERSCAN,
            api_key=api_key or apis.etherscan_api_key,
            rate_limit=calls_per_second,
        )
        super().__init__(config)

    def _build_request_params(self, **kwargs) -> Dict[str, Any]:
        """Build request parameters with chain ID and API key."""
        return {"chainid": self.chainid, "apikey": self.config.api_key, **kwargs}

    def _handle_response(self, response) -> Any:
        """Handle Etherscan API response."""
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "0":
            message = data.get("message", "Etherscan API error")
            if "rate limit" in message.lower():
                raise APIError(f"Rate limit exceeded: {message}")
            raise APIError(f"API error: {message}")

        return data["result"]

    def get_latest_block(
        self, timestamp: Optional[int] = None, closest: str = "before"
    ) -> int:
        """Get the latest block number or block closest to timestamp."""
        if timestamp is None:
            timestamp = int(datetime.now().timestamp())

        pass  # Getting latest block

        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": timestamp,
            "closest": closest,
        }
        result = self.make_request("", params)

        latest_block = int(result)
        pass  # Latest block retrieved
        return latest_block

    def get_contract_abi(
        self, address: str, save: bool = True, save_dir: str = "data/abi"
    ) -> Dict[str, Any]:
        """Get contract ABI and optionally save to file."""
        # Get contract metadata to check for proxy
        try:
            contract_metadata = self.get_contract_metadata(address)
        except Exception as e:
            logger.warning(f"Could not get metadata for {address}: {e}")
            contract_metadata = {}

        # Fetch main contract ABI
        params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
        }
        result = self.make_request("", params)
        abi = json.loads(result)

        # Check if it's a proxy and fetch implementation ABI
        implementation_abi = None
        implementation_address = None
        if contract_metadata.get("Proxy"):
            implementation_address = contract_metadata.get("Implementation")
            if implementation_address:
                pass  # Contract is a proxy, fetching implementation ABI
                try:
                    impl_params = {
                        "module": "contract",
                        "action": "getabi",
                        "address": implementation_address,
                    }
                    impl_result = self.make_request("", impl_params)
                    implementation_abi = json.loads(impl_result)
                except Exception as e:
                    logger.warning(
                        f"Could not fetch implementation ABI for {implementation_address}: {e}"
                    )

        if save:
            self._save_abi(
                address, abi, implementation_address, implementation_abi, save_dir
            )

        return abi, implementation_abi

    def get_contract_metadata(self, address: str) -> Dict[str, Any]:
        """Get contract metadata including proxy status."""
        pass  # Fetching metadata for contract

        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
        }
        result = self.make_request("", params)

        source_data = result[0] if isinstance(result, list) else result
        if not source_data:
            raise ValueError(f"No source code found for contract {address}")

        return {
            "ContractName": source_data.get("ContractName"),
            "Proxy": source_data.get("Proxy") == "1",
            "Implementation": source_data.get("Implementation"),
        }

    def get_contract_creation_block_number(self, address: str) -> int:
        """Get contract creation block number for given address."""
        return int(self.get_contract_creation_info(address)["blockNumber"])

    def get_transaction_receipt(
        self, txhash: str, save: bool = True, save_dir: str = "data/receipts"
    ) -> Dict[str, Any]:
        """Get transaction receipt for given transaction hash."""
        # Ensure txhash has 0x prefix
        if not txhash.startswith("0x"):
            txhash = "0x" + txhash

        pass  # Getting transaction receipt

        params = {
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": txhash,
        }

        result = self.make_request("", params)

        if result is None:
            raise APIError(f"Transaction receipt not found for {txhash}")

        if save:
            self._save_receipt(txhash, result, save_dir)

        return result

    def get_contract_creation_info(
        self, contract_addresses: List[str]
    ) -> Dict[str, Any]:
        """Get contract creation information for one or more addresses."""
        if isinstance(contract_addresses, str):
            contract_addresses = [contract_addresses]

        pass  # Getting creation info for contracts

        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": ",".join(contract_addresses),
        }
        result = self.make_request("", params)

        if len(contract_addresses) == 1:
            return result[0] if isinstance(result, list) else result
        return result

    def _save_abi(
        self,
        address: str,
        abi: Dict[str, Any],
        implementation_address: Optional[str],
        implementation_abi: Optional[Dict[str, Any]],
        save_dir: str,
    ):
        """Save ABI(s) to file."""
        os.makedirs(save_dir, exist_ok=True)
        # create a csv file with the following columns: address, implementation_address
        csv_path = Path(save_dir) / "implementation.csv"

        # Check if file exists to determine whether to write headers
        if not csv_path.exists():
            # Create new file with headers
            with csv_path.open("w") as f:
                f.write("address,implementation_address\n")

        with csv_path.open("a") as f:
            f.write(f"{address},{implementation_address}\n")
        df = pl.read_csv(csv_path).unique()
        df.write_csv(csv_path, separator=",", include_header=True)

        # Save main ABI
        main_path = Path(save_dir) / f"{address}.json"
        with open(main_path, "w") as f:
            json.dump(abi, f, indent=2)
        pass  # ABI saved

        # Save implementation ABI if available
        if implementation_abi:
            impl_path = Path(save_dir) / f"{implementation_address}.json"
            with open(impl_path, "w") as f:
                json.dump(implementation_abi, f, indent=2)
            pass  # Implementation ABI saved

    def _save_receipt(self, txhash: str, receipt: Dict[str, Any], save_dir: str):
        """Save transaction receipt to file."""
        os.makedirs(save_dir, exist_ok=True)

        receipt_path = Path(save_dir) / f"{txhash}.json"
        with open(receipt_path, "w") as f:
            json.dump(receipt, f, indent=2)
        pass  # Receipt saved

    def get_block_number_by_timestamp(self, timestamp: int) -> int:
        """Get block number by timestamp."""
        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": timestamp,
            "closest": "before",
        }
        return int(self.make_request("", params))


class EtherscanSource(BaseSource):
    """Creating DLT source for Etherscan data."""

    def __init__(self, client: EtherscanClient):
        super().__init__(client)

    def get_available_sources(self) -> List[str]:
        """Return list of available source names."""
        return ["logs", "transactions"]

    def create_dlt_source(self, **kwargs):
        """Create DLT source for Etherscan API."""
        session = self.client._session
        return rest_api_source(
            {
                "client": {
                    "base_url": self.client.config.base_url,
                    "paginator": paginators.PageNumberPaginator(
                        base_page=1, total_path=None, page_param="page"
                    ),
                    "session": session,
                },
                "resources": [
                    {
                        "name": "",  # Etherscan result is not nested
                        "endpoint": {"params": kwargs},
                    },
                ],
            }
        )

    def logs(
        self,
        address: str,
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000,
    ):
        """Get event logs for a given address."""

        def _fetch():
            params = {
                "module": "logs",
                "action": "getLogs",
                "address": address,
                "fromBlock": from_block,
                "toBlock": to_block,
                "offset": offset,
                "chainid": self.client.chainid,
                "apikey": self.client.config.api_key,
            }

            pass  # Fetching logs for address

            source = self.create_dlt_source(**params)
            for item in source:
                item["chain"] = self.client.chain
                yield item

        return dlt.resource(
            _fetch,
            columns={
                "topics": {"data_type": "json"},
                "time_stamp": {"data_type": "bigint"},
                "block_number": {"data_type": "bigint"},
                "log_index": {"data_type": "bigint"},
                "transaction_index": {"data_type": "bigint"},
                "gas_price": {"data_type": "bigint"},
                "gas_used": {"data_type": "bigint"},
            },
        )

    def transactions(
        self,
        address: str,
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000,
        sort: str = "asc",
    ):
        """Get transactions for a given address."""

        def _fetch():
            params = {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": from_block,
                "endblock": to_block,
                "offset": offset,
                "sort": sort,
                "chainid": self.client.chainid,
                "apikey": self.client.config.api_key,
            }

            pass  # Fetching transactions for address

            source = self.create_dlt_source(**params)
            for item in source:
                item["chain"] = self.client.chain
                yield item

        return dlt.resource(
            _fetch,
            columns={
                "time_stamp": {"data_type": "bigint"},
            },
        )


class EtherscanExtractor:
    """Extracts historical blockchain data and saves to Parquet files.

    Example:
        extractor = EtherscanExtractor(etherscan_client)
    """

    def __init__(
        self,
        client: EtherscanClient,
    ):
        self.client = client

    def to_parquet(
        self,
        address: str,
        from_block: int,
        to_block: str,
        chain: str,
        table: str,
        output_path: Path,
        offset: int = 1000,
    ):
        """
        Core building block function to extract blockchain data to Parquet files.

        Args:
            address: Contract address to extract data for
            chain: Blockchain network (default: "ethereum")
            table: logs, transactions
            from_block: Starting block number
            to_block: Ending block number or "latest"
            offset: Number of records per API call

        Returns:
            Path to the created Parquet file, or None if no data extracted
        """
        source = EtherscanSource(self.client)
        data = []

        try:
            if table == "logs":
                resource = source.logs(
                    address=address,
                    from_block=from_block,
                    to_block=to_block,
                    offset=offset,
                )

                for record in resource:
                    record = self._process_hex_fields(record)
                    data.append(record)

            elif table == "transactions":
                resource = source.transactions(
                    address=address,
                    from_block=from_block,
                    to_block=to_block,
                    offset=offset,
                )

                for record in resource:
                    record = self._process_hex_fields(record)
                    data.append(record)

            if len(data) == 0:
                logger.info(
                    f"{chain} - {address} - {table} - {from_block}-{to_block}, ✅ no data extracted"
                )
            elif len(data) >= 10_000:
                """
                when >10000 records, potential missing data due to too many records, so no saving to parquet but
                log to logging/extract_error and will retry with smaller chunk size
                """
                logger.warning(
                    f"{chain} - {address} - {table} - {from_block}-{to_block}, ⚠️ {len(data)} records"
                )

                _log_error_to_csv(
                    address=address,
                    chain=chain,
                    table=table,
                    from_block=from_block,
                    to_block=to_block,
                    block_chunk_size=to_block - from_block,
                )
            else:
                self._save_to_parquet(
                    chain, address, table, from_block, to_block, data, output_path
                )
        except:
            """
            this is unlikely to happen, but if it does, treat it as a potential missing data and
            log to logging/extract_error and will retry with smaller chunk size
            """
            logger.error(
                f"{chain} - {address} - {table} - {from_block}-{to_block}, 🚨 unexpected error"
            )

            _log_error_to_csv(
                address=address,
                chain=chain,
                table=table,
                from_block=from_block,
                to_block=to_block,
                block_chunk_size=to_block - from_block,
            )

    def _process_hex_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert numeric string fields to integers (handles both hex and decimal formats)."""
        numeric_fields = {
            "blockNumber",
            "timeStamp",
            "logIndex",
            "transactionIndex",
            "gasPrice",
            "gasUsed",
            "nonce",
            "value",
            "gas",
            "cumulativeGasUsed",
            "confirmations",
        }

        for field in numeric_fields:
            if field in record and isinstance(record[field], str):
                str_value = record[field].strip()
                if str_value and str_value != "0x":
                    try:
                        # Auto-detect format based on prefix
                        if str_value.startswith("0x"):
                            # Hex format (logs API)
                            record[field] = int(str_value, 16)
                        else:
                            # Decimal format (transactions API)
                            record[field] = int(str_value, 10)
                    except ValueError:
                        logger.warning(
                            f"Could not convert {field} value '{str_value}' to int"
                        )
                        record[field] = None
                else:
                    record[field] = None

        return record

    def _save_to_parquet(
        self,
        chain: str,
        address: str,
        table: str,
        from_block: int,
        to_block: int,
        data: List[Dict[str, Any]],
        output_path: Path,
    ) -> str:
        """Save data to Parquet file organized by chain_address_table_from_block_to_block."""
        try:
            # Create Polars DataFrame
            new_lf = pl.LazyFrame(data)

            # Save to Parquet (append if file exists)
            if output_path.exists():
                # Use scan_parquet for memory efficiency, then concatenate and collect
                existing_lf = pl.scan_parquet(output_path)

                # Ensure column order matches between existing and new data
                existing_columns = existing_lf.collect_schema().names()
                new_lf = new_lf.select(existing_columns)

                combined_lf = pl.concat([existing_lf, new_lf]).unique()
                combined_lf.collect().write_parquet(output_path)

                logger.info(
                    f"{chain} - {address} - {table} - {from_block}-{to_block}: {len(data)} saved"
                )

            else:
                # Write new file
                new_lf.collect().write_parquet(output_path)
                logger.info(
                    f"{chain} - {address} - {table} - {from_block}-{to_block}: {len(data)} saved"
                )

            return output_path

        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise


def etherscan_to_parquet(
    address: str,
    etherscan_client: EtherscanClient,
    from_block: int,
    to_block: int,
    output_path: Path,
    table,
    block_chunk_size: int = 10_000,
) -> Path:
    """Backfill blockchain data from Etherscan to protocol-grouped Parquet files in chunks.

    This function efficiently extracts historical data and saves to Parquet files:
    - logs of a specific contract address
    - transactions to a specific contract address (optional)

    Args:
        address: Ethereum address to fetch data for (case-insensitive)
        etherscan_client: Configured Etherscan API client for data retrieval
        from_block: Starting block number (uses contract creation block if None)
        to_block: Ending block number (uses latest block if None)
        block_chunk_size: Number of blocks to process per chunk (default: 50,000)
        data_dir: Path for parquet file output
        table: logs, transactions
    Returns:
        Path to the parquet file
    """

    extractor = EtherscanExtractor(etherscan_client)
    address = address.lower()
    chain = etherscan_client.chain

    end_block = to_block

    assert from_block < to_block, "from_block must be less than to_block"
    assert (
        block_chunk_size < to_block - from_block
    ), "block_chunk_size must be less than to_block - from_block"

    for chunk_start in range(
        from_block, end_block - block_chunk_size + 1, block_chunk_size
    ):
        chunk_end = min(chunk_start + block_chunk_size, end_block)

        extractor.to_parquet(
            address=address,
            chain=chain,
            table=table,
            from_block=chunk_start,
            to_block=chunk_end,
            offset=1000,
            output_path=output_path,
        )
    if chunk_end < end_block:
        extractor.to_parquet(
            address=address,
            chain=chain,
            table=table,
            from_block=chunk_end,
            to_block=end_block,
            offset=1000,
            output_path=output_path,
        )
    return output_path


def _log_error_to_csv(
    address: str,
    chain: str,
    table: str,
    from_block: int,
    to_block: int,
    block_chunk_size: int,
):
    """Log an error to CSV file."""
    error_file = Path(f"logging/extract_error/{chain}_{address}_{table}.csv")
    error_file.parent.mkdir(parents=True, exist_ok=True)

    # CSV headers
    csv_headers = [
        "timestamp",
        "address",
        "chain",
        "from_block",
        "to_block",
        "block_chunk_size",
    ]

    # Check if file exists to determine if we need to write headers
    file_exists = error_file.exists()

    # Append error to CSV file immediately
    with error_file.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write headers if this is a new file
        if not file_exists:
            writer.writerow(csv_headers)

        timestamp = datetime.now().isoformat()

        writer.writerow(
            [
                timestamp,
                address,
                chain,
                from_block,
                to_block,
                block_chunk_size,
            ]
        )
