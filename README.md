# Data226: Building a Stock Price Prediction Analytics Using Snowflake & Airflow

## Lab 1: Building a Finance Data Analytics System
This lab project is part of a group assignment (2-person team) aimed at analyzing finance data using the yfinance API. The objective of this lab is to build a stock price prediction system by leveraging various financial analyses.

---

### Objective
The project’s goal is to create a system that predicts the future stock prices of a company based on the last 180 days of stock price data. Additionally, we will utilize Snowflake for data storage and Airflow to automate data pipelines.

### Data Source: yfinance API
The yfinance API provides comprehensive stock market data. This includes details about stock prices such as:
- Open
- High
- Low
- Close
- Volume

With this dataset, we can perform the following analyses:

#### 1. **Stock Price Prediction**
   - **Objective:** Predict future stock prices based on historical data.
   - **Data Used:** Historical stock prices and technical indicators.
   - **Example Analysis:** Develop models using machine learning techniques like LSTM, ARIMA to predict short-term or long-term stock price forecasts.

---

### Project Details

#### **Step-by-Step Process**
1. **Choose at least two companies** (e.g., NVDA and AAPL).
2. **Get historical stock prices** for the last 180 days using the yfinance API and store the data in a Snowflake table.
   - **Table Schema:**
     - Stock Symbol
     - Date
     - Open, Close, Min, Max, Volume
3. **Python Codes:**
   - Set up an **Airflow DAG** to run the data extraction and loading (ETL) process daily.
4. **SQL Queries:**
   - Set up ML forecasting tasks in Snowflake to predict stock prices for the next 7 days.
5. **Airflow Data Pipelines:**
   - Create Airflow DAGs to manage both the ETL process and the forecasting process.
   - Ensure that the final table is populated using SQL transactions.

#### **Table Structure in Snowflake:**
| Field       | Type    | Description                    |
|-------------|---------|--------------------------------|
| Stock Symbol| VARCHAR | Stock ticker symbol (e.g., NVDA, AAPL) |
| Date        | DATE    | Date of the stock data         |
| Open        | FLOAT   | Opening price                  |
| Close       | FLOAT   | Closing price                  |
| Min         | FLOAT   | Minimum price                  |
| Max         | FLOAT   | Maximum price                  |
| Volume      | INT     | Volume of stocks traded        |

---

# Airflow Pinecone Integration - Homework Submission #8

This project demonstrates a complete end-to-end pipeline using Apache Airflow and Pinecone for vector search.

## Project Structure

- `dags/medium_to_pinecone_dag.py` - Main DAG that:
  - Loads and processes CSV data
  - Generates embeddings using `sentence-transformers`
  - Sets up Pinecone index
  - Ingests data into Pinecone
  - Performs a search query and logs the top results

- `data/medium_data.csv` - Raw input data file used for vectorization.

- `docker-compose.yaml` - Defines services and includes necessary packages like:
  - `sentence-transformers`
  - `pinecone-client`

- `Dockerfile.airflow` - Custom Dockerfile to extend the base image if needed.

- `logs/` - Stores logs of DAG execution.

- `config/`, `plugins/` - Reserved for future enhancements or custom Airflow plugins.

## Completed Steps

- Docker setup with required packages
- Airflow variable configuration for Pinecone API key
- Data preprocessing and embedding generation
- Pinecone index creation and data ingestion
- Search functionality tested and working

---

Run the DAG manually from the Airflow UI to see the results.



