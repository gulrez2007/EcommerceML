import csv
import logging
import os
from datetime import datetime
from typing import List, Dict, Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="pipeline.log"
)

NOT_DELIVERED = "Not Delivered"  # Constant for missing delivery dates


class EcommercePipeline:
    """Pipeline to process e-commerce customer purchase data."""
    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
    REQUIRED_FIELDS = {"order_id", "order_status",
                       "order_purchase_timestamp", "order_delivered_customer_date"}

    def __init__(self, input_path: str, output_path: str, timestamp_format: str = TIMESTAMP_FORMAT):
        """Initialize with file paths and optional timestamp format."""
        self.input_path = input_path
        self.output_path = output_path
        self.timestamp_format = timestamp_format
        self.data: List[Dict[str, str]] = []

    def _safe_execute(self, action: Callable[[], None], error_msg: str) -> None:
        """Execute an action, logging any errors and re-raising exceptions."""
        try:
            action()
        except Exception as e:
            logging.error(f"{error_msg}: {e}")
            raise

    def load_data(self) -> None:
        """Load CSV data into memory."""
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        def load():
            with open(self.input_path, "r") as file:
                reader = csv.DictReader(file)
                self.data = [row for row in reader]
                if not self.data:
                    raise ValueError(f"No data found in {self.input_path}")
                if not self.REQUIRED_FIELDS.issubset(self.data[0].keys()):
                    raise ValueError(
                        f"Missing required fields: {self.REQUIRED_FIELDS - set(self.data[0].keys())}")
            logging.info(
                f"Loaded {len(self.data)} rows from {self.input_path}")

        self._safe_execute(load, f"Error loading data from {self.input_path}")

    def clean_data(self) -> None:
        """Remove duplicates and handle missing delivery dates."""
        seen_orders = set()
        cleaned_data = []
        duplicates = 0

        for row in self.data:
            order_id = row.get("order_id")
            if not order_id:
                logging.warning("Skipping row with missing order_id")
                continue
            if order_id in seen_orders:
                duplicates += 1
                continue
            seen_orders.add(order_id)

            row["order_delivered_customer_date"] = self._normalize_delivery_date(
                row.get("order_delivered_customer_date"))
            if row.get("order_status") != "delivered":
                continue
            cleaned_data.append(row)

        self.data = cleaned_data
        if duplicates == 0:
            logging.debug("No duplicate order_ids detected")
        logging.info(
            f"Cleaned data: {len(self.data)} rows remain (removed {duplicates} duplicates)")

    def _normalize_delivery_date(self, delivery_date: str) -> str:
        """Normalize delivery date, replacing empty/missing with NOT_DELIVERED."""
        return NOT_DELIVERED if not delivery_date or delivery_date.strip() == "" else delivery_date

    def calculate_delivery_time(self) -> None:
        """Add delivery time in days to each row."""
        na_count = 0
        for i, row in enumerate(self.data):
            if i % 10000 == 0:
                logging.debug(f"Processed {i} rows in calculate_delivery_time")
            try:
                purchase = self._parse_timestamp(
                    row["order_purchase_timestamp"], row["order_id"])
                delivery = row["order_delivered_customer_date"]

                if delivery == NOT_DELIVERED:
                    row["delivery_time_days"] = "N/A"
                    na_count += 1
                else:
                    delivery_time = self._parse_timestamp(
                        delivery, row["order_id"])
                    row["delivery_time_days"] = str(
                        (delivery_time - purchase).days)
            except ValueError as e:
                logging.warning(
                    f"Timestamp error in row {row.get('order_id', 'Unknown')}: {e}")
                row["delivery_time_days"] = "N/A"
                na_count += 1

        logging.info(
            f"Calculated delivery times: {len(self.data) - na_count} with times, {na_count} N/A")

    def _parse_timestamp(self, timestamp: str, order_id: str) -> datetime:
        """Parse a timestamp with error handling."""
        if not timestamp or timestamp.strip() == "":
            raise ValueError(f"Empty timestamp for order {order_id}")
        return datetime.strptime(timestamp, self.timestamp_format)

    def save_data(self) -> None:
        """Save processed data to CSV."""
        if not self.data:
            logging.warning("No data to save")
            return

        def save():
            headers = self.data[0].keys()
            with open(self.output_path, "w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(self.data)
            logging.info(f"Saved {len(self.data)} rows to {self.output_path}")

        self._safe_execute(save, f"Error saving data to {self.output_path}")

    def run(self) -> None:
        """Execute the full pipeline."""
        logging.info("Starting EcommercePipeline")
        start_time = datetime.now()
        self.load_data()
        if not self.data:
            logging.info("No data to process, exiting pipeline")
            return
        self.clean_data()
        self.calculate_delivery_time()
        self.save_data()
        duration = (datetime.now() - start_time).total_seconds()
        logging.info(
            f"Pipeline completed successfully in {duration:.2f} seconds")


if __name__ == "__main__":
    input_file = "data/raw/olist_orders_dataset.csv"
    output_file = "data/processed/processed_orders.csv"
    pipeline = EcommercePipeline(input_file, output_file)
    pipeline.run()
