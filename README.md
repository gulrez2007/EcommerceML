# EcommerceML
Customer Retention Toolkit - Churn Prediction and API

This project leverages the **Brazilian E-Commerce Public Dataset by Olist** (100k orders with product, customer, and review data) to build a data pipeline for e-commerce analytics. It includes code to load and process `olist_orders_dataset.csv`, setting the foundation for churn prediction and a deployable API.

## Features
- Loads and processes e-commerce transaction data.
- Prepares dataset for ML models (e.g., churn prediction).
- Future: Flask API for real-time predictions.

## Dataset
- Source: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- File: `olist_orders_dataset.csv`

## Setup
1. Clone: `git clone https://github.com/gulrez2007/EcommerceML.git`
2. Install: `pip install -r requirements.txt`
3. Run: `python src/load_data.py`

*Work in Progress - Next: Churn Prediction Model*