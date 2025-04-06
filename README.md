# EcommerceML
Customer Retention Toolkit - Churn Prediction and API

This project leverages the **Brazilian E-Commerce Public Dataset by Olist** (100k orders with product, customer, and review data) to build a data pipeline for e-commerce analytics. It includes code to load and process `olist_orders_dataset.csv`, setting the foundation for churn prediction and a deployable API.

## Features
- Loads and processes e-commerce transaction data.
- Prepares dataset for ML models (e.g., churn prediction).
- Future: Flask API for real-time predictions.

## Dataset
- Source: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- File: `olist_orders_dataset.csv` (not included)
- License: CC BY-NC-SA 4.0

## Setup - For python only pipeline
1. Clone: `git clone https://github.com/gulrez2007/EcommerceML.git`
2. **Requirements**: Python 3.8+, Pandas (`pip install pandas`)
3. **Run Pure Python**: `python src/python_only_pipeline.py`
4. **Run Pandas**: `python src/pandas_pipeline.py`

## Processing
- **Pure Python Pipeline** (`src/python_only_pipeline.py`):
  - Loads 99,441 rows, cleans to 96,478 "delivered" orders (no duplicates).
  - Adds `delivery_time_days`: 96,470 with calculated times, 8 N/A due to missing delivery data.
  - Runtime: ~6.6 seconds on a standard machine.
  - Output: `data/processed/python_only_processed_orders.csv`.

- **Optimized Pandas Pipeline** (`src/pandas_pipeline.py`):
  - Same processing as above, leveraging Pandas’ vectorized operations.
  - Runtime: ~2.1s (3x faster than pure Python) on a standard machine (HDD).
  - Features: Scalable chunk processing, rotating logs (`pandas_pipeline.log`), robust error handling.
  - Output: `data/processed/pandas_processed_orders.csv`.

## Results
- **Pure Python**: Processes 99,441 rows in ~6.6s, suitable for learning basic Python.
- **Pandas**: Processes the same in ~2.1s, optimized for speed and scalability.

## Troubleshooting
- Enable detailed logs by uncommenting `logger.setLevel(logging.DEBUG)` in `src/pandas_pipeline.py`.
- Check `pandas_pipeline.log` for runtime breakdowns and errors.

*Work in Progress - 
- EDA on `pandas_processed_orders.csv` to explore delivery times and churn predictors.
- ML model for churn prediction and API deployment.
