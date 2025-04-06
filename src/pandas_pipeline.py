import pandas as pd
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler

# Constants
NOT_DELIVERED = "Not Delivered"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
REQUIRED_FIELDS = frozenset(
    {"order_id", "order_status", "order_purchase_timestamp", "order_delivered_customer_date"})
CHUNK_SIZE = 10000  # Default chunk size for scalability

# Column name constants
ORDER_ID = "order_id"
ORDER_STATUS = "order_status"
PURCHASE_TIMESTAMP = "order_purchase_timestamp"
DELIVERED_TIMESTAMP = "order_delivered_customer_date"
DELIVERY_TIME_DAYS = "delivery_time_days"


def configure_logging(log_file: str = "pandas_pipeline.log") -> logging.Logger:
    """Configure logging with rotation for pipeline monitoring.

    Args:
        log_file: Path to the log file (default: 'pandas_pipeline.log').

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("EcommercePipeline")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(
        log_file, maxBytes=1_048_576, backupCount=3)  # 1 MB, 3 backups
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.handlers = []  # Clear existing handlers to avoid duplicates
    logger.addHandler(handler)
    return logger


logger = configure_logging()


class EcommercePipeline:
    """A scalable, robust pipeline to process e-commerce data using Pandas.

    Attributes:
        input_path (Path): Resolved path to input CSV.
        output_path (Path): Resolved path to output CSV.
        timestamp_format (str): Format for timestamp parsing.
        chunk_size (int): Rows per chunk for large datasets.
        data (pd.DataFrame): Processed dataset in memory.
    """

    def __init__(
        self,
        input_path: str,
        output_path: str,
        timestamp_format: str = TIMESTAMP_FORMAT,
        chunk_size: int = CHUNK_SIZE
    ) -> None:
        """Initialize pipeline with configurable parameters.

        Args:
            input_path: Path to input CSV file.
            output_path: Path to output CSV file.
            timestamp_format: Format for timestamps (default: '%Y-%m-%d %H:%M:%S').
            chunk_size: Rows to process per chunk (default: 10000).
        """
        self.input_path = Path(input_path).resolve()
        self.output_path = Path(output_path).resolve()
        self.timestamp_format = timestamp_format
        self.chunk_size = chunk_size
        self.data = pd.DataFrame()
        self._validate_paths()

    def _validate_paths(self) -> None:
        """Validate input and output paths, creating output directory if needed."""
        if not self.input_path.is_file():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(
            f"Validated paths: input={self.input_path}, output={self.output_path}")

    def load_data(self) -> None:
        """Load CSV data into memory with validation."""
        try:
            self.data = pd.read_csv(
                self.input_path,
                usecols=lambda col: col in REQUIRED_FIELDS,
                dtype={ORDER_ID: str, ORDER_STATUS: str},
            )
            if self.data.empty:
                raise ValueError(f"No data found in {self.input_path}")
            missing_fields = REQUIRED_FIELDS - set(self.data.columns)
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            logger.info(f"Loaded {len(self.data)} rows from {self.input_path}")
        except Exception as e:
            logger.error(f"Failed to load data from {self.input_path}: {e}")
            raise

    def clean_data(self) -> None:
        """Clean data: remove duplicates, normalize delivery dates, filter delivered orders."""
        initial_count = len(self.data)
        self.data.drop_duplicates(
            subset=[ORDER_ID], inplace=True, keep="first")
        self.data[DELIVERED_TIMESTAMP] = self.data[DELIVERED_TIMESTAMP].fillna(
            NOT_DELIVERED)
        self.data = self.data.query(f"{ORDER_STATUS} == 'delivered'").copy()
        removed = initial_count - len(self.data)
        logger.info(
            f"Cleaned data: {len(self.data)} rows remain (removed {removed} rows)")
        if removed > 0:
            logger.debug(
                f"Removed {removed} rows: duplicates and non-delivered orders")

    def calculate_delivery_time(self) -> None:
        """Calculate delivery time in days with vectorized operations."""
        start_time = datetime.now()

        # Parse timestamps only here
        self.data[PURCHASE_TIMESTAMP] = pd.to_datetime(
            self.data[PURCHASE_TIMESTAMP], format=self.timestamp_format, errors="coerce"
        )
        self.data[DELIVERED_TIMESTAMP] = pd.to_datetime(
            self.data[DELIVERED_TIMESTAMP], format=self.timestamp_format, errors="coerce"
        )

        # Initialize delivery time column
        self.data[DELIVERY_TIME_DAYS] = pd.NA
        na_mask = (self.data[DELIVERED_TIMESTAMP] ==
                   NOT_DELIVERED) | self.data[DELIVERED_TIMESTAMP].isna()

        # Vectorized calculation for valid timestamps
        valid_rows = ~na_mask & self.data[PURCHASE_TIMESTAMP].notna()
        if valid_rows.any():
            time_diff = (
                self.data.loc[valid_rows, DELIVERED_TIMESTAMP] -
                self.data.loc[valid_rows, PURCHASE_TIMESTAMP]
            )
            self.data.loc[valid_rows,
                          DELIVERY_TIME_DAYS] = time_diff.dt.days.astype(str)

        # Set N/A for invalid/missing cases
        self.data.loc[na_mask, DELIVERY_TIME_DAYS] = "N/A"
        na_count = self.data[DELIVERY_TIME_DAYS].eq("N/A").sum()
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Calculated delivery times: {len(self.data) - na_count} with times, {na_count} N/A, in {duration:.2f} seconds")

    def save_data(self) -> None:
        """Save processed data to CSV with error handling."""
        if self.data.empty:
            logger.warning("No data to save")
            return
        try:
            self.data.to_csv(self.output_path, index=False)
            logger.info(f"Saved {len(self.data)} rows to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to save data to {self.output_path}: {e}")
            raise

    def process_chunks(self) -> None:
        """Process large datasets in chunks for memory efficiency."""
        chunks = pd.read_csv(
            self.input_path,
            usecols=lambda col: col in REQUIRED_FIELDS,
            dtype={ORDER_ID: str, ORDER_STATUS: str},
            chunksize=self.chunk_size,
        )
        processed_chunks = []
        total_rows = 0
        for i, chunk in enumerate(chunks):
            initial_count = len(chunk)
            chunk.drop_duplicates(
                subset=[ORDER_ID], inplace=True, keep="first")
            chunk[DELIVERED_TIMESTAMP] = chunk[DELIVERED_TIMESTAMP].fillna(
                NOT_DELIVERED)
            chunk = chunk.query(f"{ORDER_STATUS} == 'delivered'").copy()

            # Parse timestamps
            chunk[PURCHASE_TIMESTAMP] = pd.to_datetime(
                chunk[PURCHASE_TIMESTAMP], format=self.timestamp_format, errors="coerce"
            )
            chunk[DELIVERED_TIMESTAMP] = pd.to_datetime(
                chunk[DELIVERED_TIMESTAMP], format=self.timestamp_format, errors="coerce"
            )

            na_mask = (chunk[DELIVERED_TIMESTAMP] ==
                       NOT_DELIVERED) | chunk[DELIVERED_TIMESTAMP].isna()
            chunk[DELIVERY_TIME_DAYS] = pd.NA
            valid_rows = ~na_mask & chunk[PURCHASE_TIMESTAMP].notna()
            if valid_rows.any():
                chunk.loc[valid_rows, DELIVERY_TIME_DAYS] = (
                    (chunk.loc[valid_rows, DELIVERED_TIMESTAMP] -
                     chunk.loc[valid_rows, PURCHASE_TIMESTAMP]).dt.days.astype(str)
                )
            chunk[DELIVERY_TIME_DAYS] = chunk[DELIVERY_TIME_DAYS].fillna("N/A")
            processed_chunks.append(chunk)
            total_rows += len(chunk)
            logger.debug(f"Processed chunk {i + 1}: {len(chunk)} rows")

        if not processed_chunks:
            logger.warning("No data processed from chunks")
            return
        self.data = pd.concat(processed_chunks, ignore_index=True)
        logger.info(f"Processed {total_rows} rows from chunks")

    def run(self, use_chunks: bool = False) -> None:
        """Execute the full pipeline with optional chunking.

        Args:
            use_chunks: If True, process data in chunks (default: False).
        """
        logger.info(f"Starting EcommercePipeline (chunked={use_chunks})")
        start_time = datetime.now()
        try:
            if use_chunks:
                self.process_chunks()
            else:
                self.load_data()
                self.clean_data()
                self.calculate_delivery_time()
            self.save_data()
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"Pipeline completed successfully in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    # Hardcoded paths for simplicity
    input_file = "data/raw/olist_orders_dataset.csv"
    # Updated to match your log
    output_file = "data/processed/pandas_processed_orders.csv"

    # Uncomment to enable debug logging
    # logger.setLevel(logging.DEBUG)

    pipeline = EcommercePipeline(input_file, output_file)
    pipeline.run(use_chunks=False)  # Set to True for large datasets
