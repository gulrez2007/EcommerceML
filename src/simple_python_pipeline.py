import csv
from datetime import datetime

# Constants (values that don't change)
NOT_DELIVERED = "Not Delivered"  # What we use for missing delivery dates
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"  # How dates look in the CSV


class EcommercePipeline:
    def __init__(self, input_path, output_path):
        # Set up the file paths and an empty list to hold our data
        self.input_path = input_path  # Where to find the input CSV
        self.output_path = output_path  # Where to save the output CSV
        self.data = []  # Our list starts empty

    def load_data(self):
        # Read the CSV file into our list
        with open(self.input_path, "r") as file:
            reader = csv.DictReader(file)
            # Load all rows as dictionaries
            self.data = [row for row in reader]

    def clean_data(self):
        # Remove duplicates and keep only delivered orders
        seen_orders = set()  # Track order IDs we’ve seen
        cleaned_data = []  # New list for cleaned rows

        for row in self.data:
            order_id = row["order_id"]
            if order_id in seen_orders:  # Skip if we’ve seen this order before
                continue
            seen_orders.add(order_id)

            # Fill missing delivery dates with "Not Delivered"
            if not row["order_delivered_customer_date"]:
                row["order_delivered_customer_date"] = NOT_DELIVERED

            # Only keep "delivered" orders
            if row["order_status"] == "delivered":
                cleaned_data.append(row)

        self.data = cleaned_data  # Update our data with the cleaned list

    def calculate_delivery_time(self):
        # Add delivery time in days to each row
        for row in self.data:
            purchase = datetime.strptime(
                row["order_purchase_timestamp"], TIMESTAMP_FORMAT)
            delivery = row["order_delivered_customer_date"]

            if delivery == NOT_DELIVERED:
                # No delivery time if not delivered
                row["delivery_time_days"] = "N/A"
            else:
                delivery_time = datetime.strptime(delivery, TIMESTAMP_FORMAT)
                # Calculate days difference
                days = (delivery_time - purchase).days
                row["delivery_time_days"] = str(days)  # Add as a string

    def save_data(self):
        # Save the data to a new CSV file
        headers = self.data[0].keys()  # Get column names from the first row
        with open(self.output_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()  # Write the column names
            writer.writerows(self.data)  # Write all rows

    def run(self):
        # Start the timer
        start_time = datetime.now()

        # Run the pipeline
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
    output_file = "data/processed/simple_python_processed_orders.csv"  # Updated output file
    pipeline = EcommercePipeline(input_file, output_file)
    pipeline.run()
