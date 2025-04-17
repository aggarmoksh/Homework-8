

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



