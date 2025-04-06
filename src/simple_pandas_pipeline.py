import pandas as pd
from pathlib import Path
from datetime import datetime  # Added for duration check

# Constants (values that don't change)
NOT_DELIVERED = "Not Delivered"  # What we use for missing delivery dates
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"  # How dates look in the CSV
CHUNK_SIZE = 10000  # How many rows to process at a time for big files

# Column names (makes it easier to read the code)
ORDER_ID = "order_id"
ORDER_STATUS = "order_status"
PURCHASE_TIMESTAMP = "order_purchase_timestamp"
DELIVERED_TIMESTAMP = "order_delivered_customer_date"
DELIVERY_TIME_DAYS = "delivery_time_days"


class EcommercePipeline:
    def __init__(self, input_path, output_path):
        # Set up the file paths and an empty table to hold our data
        self.input_path = Path(input_path)  # Where to find the input CSV
        self.output_path = Path(output_path)  # Where to save the output CSV
        self.data = pd.DataFrame()  # Our table starts empty

    def load_data(self):
        # Read the CSV file into our table, only taking the columns we need
        self.data = pd.read_csv(
            self.input_path,
            usecols=[ORDER_ID, ORDER_STATUS,
                     PURCHASE_TIMESTAMP, DELIVERED_TIMESTAMP],
            dtype={ORDER_ID: str, ORDER_STATUS: str}  # Keep these as text
        )

    def clean_data(self):
        # Remove duplicate orders based on order_id
        self.data = self.data.drop_duplicates(subset=[ORDER_ID])

        # Fill in missing delivery dates with "Not Delivered"
        self.data[DELIVERED_TIMESTAMP] = self.data[DELIVERED_TIMESTAMP].fillna(
            NOT_DELIVERED)

        # Keep only orders that are "delivered"
        self.data = self.data[self.data[ORDER_STATUS] == "delivered"]

    def calculate_delivery_time(self):
        # Convert the timestamp columns to proper dates, ignore errors
        self.data[PURCHASE_TIMESTAMP] = pd.to_datetime(
            self.data[PURCHASE_TIMESTAMP], format=TIMESTAMP_FORMAT, errors="coerce"
        )
        self.data[DELIVERED_TIMESTAMP] = pd.to_datetime(
            self.data[DELIVERED_TIMESTAMP], format=TIMESTAMP_FORMAT, errors="coerce"
        )

        # Add a new column for delivery time in days
        self.data[DELIVERY_TIME_DAYS] = "N/A"  # Start with "N/A" everywhere

        # Find rows where delivery date is valid (not "Not Delivered" or missing)
        valid_rows = (self.data[DELIVERED_TIMESTAMP] !=
                      NOT_DELIVERED) & self.data[DELIVERED_TIMESTAMP].notna()

        # Calculate days between purchase and delivery for valid rows
        self.data.loc[valid_rows, DELIVERY_TIME_DAYS] = (
            (self.data[DELIVERED_TIMESTAMP] - self.data[PURCHASE_TIMESTAMP])
            .dt.days.astype(str)
        )

    def save_data(self):
        # Save the table to a new CSV file
        self.data.to_csv(self.output_path, index=False)

    def process_chunks(self):
        # Read the CSV in chunks for big files
        chunks = pd.read_csv(
            self.input_path,
            usecols=[ORDER_ID, ORDER_STATUS,
                     PURCHASE_TIMESTAMP, DELIVERED_TIMESTAMP],
            dtype={ORDER_ID: str, ORDER_STATUS: str},
            chunksize=CHUNK_SIZE
        )

        # List to hold processed chunks
        all_chunks = []

        # Process each chunk one by one
        for chunk in chunks:
            # Clean the chunk
            chunk = chunk.drop_duplicates(subset=[ORDER_ID])
            chunk[DELIVERED_TIMESTAMP] = chunk[DELIVERED_TIMESTAMP].fillna(
                NOT_DELIVERED)
            chunk = chunk[chunk[ORDER_STATUS] == "delivered"]

            # Calculate delivery times for the chunk
            chunk[PURCHASE_TIMESTAMP] = pd.to_datetime(
                chunk[PURCHASE_TIMESTAMP], format=TIMESTAMP_FORMAT, errors="coerce"
            )
            chunk[DELIVERED_TIMESTAMP] = pd.to_datetime(
                chunk[DELIVERED_TIMESTAMP], format=TIMESTAMP_FORMAT, errors="coerce"
            )
            chunk[DELIVERY_TIME_DAYS] = "N/A"
            valid_rows = (chunk[DELIVERED_TIMESTAMP] !=
                          NOT_DELIVERED) & chunk[DELIVERED_TIMESTAMP].notna()
            chunk.loc[valid_rows, DELIVERY_TIME_DAYS] = (
                (chunk[DELIVERED_TIMESTAMP] - chunk[PURCHASE_TIMESTAMP])
                .dt.days.astype(str)
            )

            # Add the processed chunk to our list
            all_chunks.append(chunk)

        # Combine all chunks into one table
        self.data = pd.concat(all_chunks, ignore_index=True)

    def run(self, use_chunks=False):
        # Start the timer
        start_time = datetime.now()

        # Run the pipeline: either all at once or in chunks
        if use_chunks:
            self.process_chunks()
        else:
            self.load_data()
            self.clean_data()
            self.calculate_delivery_time()
        self.save_data()

        # Calculate and print how long it took
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Pipeline took {duration:.2f} seconds to run")


if __name__ == "__main__":
    input_file = "data/raw/olist_orders_dataset.csv"
    output_file = "data/processed/simple_pandas_processed_orders.csv"
    pipeline = EcommercePipeline(input_file, output_file)
    pipeline.run(use_chunks=False)
