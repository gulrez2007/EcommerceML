import csv
import logging
from datetime import datetime
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="pipeline.log"
)


class EcommercePipeline:
    """Pipeline to process e-commerce customer purchase data."""
    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, input_path: str, output_path: str):
        """Initialize with file paths."""
        self.input_path = input_path
        self.output_path = output_path
        self.data: List[Dict[str, str]] = []

    def load_data(self) -> None:
        """Load CSV data into memory."""
        try:
            with open(self.input_path, "r") as file:
                reader = csv.DictReader(file)
                self.data = [row for row in reader]
            logging.info(
                f"Loaded {len(self.data)} rows from {self.input_path}")
        except FileNotFoundError:
            logging.error(f"File not found: {self.input_path}")
            raise
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            raise

    def clean_data(self) -> None:
        """Remove duplicates and handle missing delivery dates."""
        seen_orders = set()
        cleaned_data = []

        for row in self.data:
            order_id = row["order_id"]
            if order_id in seen_orders:
                continue
            seen_orders.add(order_id)

            # Explicitly handle empty or missing delivery dates
            delivery_date = row.get(
                "order_delivered_customer_date", "Not Delivered")
            if not delivery_date or delivery_date.strip() == "":
                row["order_delivered_customer_date"] = "Not Delivered"
            if row["order_status"] == "delivered":
                cleaned_data.append(row)

        self.data = cleaned_data
        logging.info(f"Cleaned data: {len(self.data)} rows remain")

    def calculate_delivery_time(self) -> None:
        """Add delivery time in days to each row."""
        for row in self.data:
            try:
                purchase = datetime.strptime(
                    row["order_purchase_timestamp"], self.TIMESTAMP_FORMAT)
                delivery = row["order_delivered_customer_date"]

                # Skip parsing if delivery date is invalid or "Not Delivered"
                if not delivery or delivery == "Not Delivered" or delivery.strip() == "":
                    row["delivery_time_days"] = "N/A"
                else:
                    delivery = datetime.strptime(
                        delivery, self.TIMESTAMP_FORMAT)
                    row["delivery_time_days"] = str((delivery - purchase).days)
            except ValueError as e:
                logging.warning(
                    f"Invalid timestamp in row {row['order_id']}: {e}")
                row["delivery_time_days"] = "N/A"

        logging.info("Calculated delivery times for all rows")

    def save_data(self) -> None:
        """Save processed data to CSV."""
        try:
            headers = self.data[0].keys()
            with open(self.output_path, "w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(self.data)
            logging.info(f"Saved {len(self.data)} rows to {self.output_path}")
        except Exception as e:
            logging.error(f"Error saving data: {e}")
            raise

    def run(self) -> None:
        """Execute the full pipeline."""
        logging.info("Starting EcommercePipeline")
        try:
            self.load_data()
            self.clean_data()
            self.calculate_delivery_time()
            self.save_data()
            logging.info("Pipeline completed successfully")
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    input_file = "data/raw/olist_orders_dataset.csv"
    output_file = "data/processed/processed_orders.csv"
    pipeline = EcommercePipeline(input_file, output_file)
    pipeline.run()
