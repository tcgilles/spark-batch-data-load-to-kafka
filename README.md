# Spark Batch Data Load to Kafka (SBDL)

## Overview
This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using PySpark to process batch data and load it into a Kafka topic. The pipeline is designed for flexibility across different environments (local, QA, production) and includes robust configuration, transformation, and testing modules.

## Features
- **Configurable ETL Pipeline:** Easily switch between environments using configuration files.
- **Data Sources:** Loads sample data from CSV files (accounts, parties, addresses) or Hive tables.
- **Transformations:** Cleans, structures, and joins data into a unified schema suitable for downstream consumption.
- **Kafka Integration:** Publishes processed records to a Kafka topic in a key-value format.
- **Logging:** Uses Log4j for Spark job logging.
- **Testing:** Includes unit tests using `pytest` and `chispa` for DataFrame equality.

## Project Structure
```
├── main.py                  # Entry point for the ETL job
├── lib/
│   ├── __init__.py
│   ├── config_loader.py     # Loads environment and Spark configs
│   ├── data_generation.py   # (Optional) Data generation utilities
│   ├── logger.py            # Log4j logger integration
│   ├── transformations.py   # All ETL transformation logic
│   └── utils.py             # Spark session utilities
├── conf/
│   ├── sbdl.conf            # Environment-specific configs
│   └── spark.conf           # Spark-specific configs
├── test_data/               # Sample input and expected output data
│   ├── accounts/
│   ├── parties/
│   ├── party_address/
│   └── results/
├── test_pytest_sbdl.py      # Unit tests for the pipeline
├── Pipfile / Pipfile.lock   # Python dependencies
├── log4j.properties         # Log4j configuration for Spark
└── LICENSE
```

## Getting Started

### Prerequisites
- Python 3.11+
- Java 8+
- Apache Spark 3.5.x
- Kafka cluster (for production/QA)
- [pipenv](https://pipenv.pypa.io/) for dependency management

### Installation
1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd spark-project
   ```
2. **Install dependencies:**
   ```sh
   pipenv install
   ```
3. **Set up environment variables:**
   - Create a `.env` file with your Kafka credentials:
     ```ini
     KAFKA_API_KEY=your_kafka_api_key
     KAFKA_API_SECRET=your_kafka_api_secret
     ```
4. **Configure environments:**
   - Edit `conf/sbdl.conf` and `conf/spark.conf` for your environment settings.

### Running the ETL Job
Run the ETL pipeline with:
```sh
pipenv run python main.py <ENV> <LOAD_DATE>
```
- `<ENV>`: `local`, `qa`, or `prod`
- `<LOAD_DATE>`: Date string (e.g., `2025-09-28`)

Example:
```sh
pipenv run python main.py local 2025-09-28
```

### Running Tests
Run all unit tests with:
```sh
pipenv run pytest
```

## Configuration
- **Kafka and Hive settings** are managed in `conf/sbdl.conf`.
- **Spark settings** are managed in `conf/spark.conf`.
- **Logging** is configured via `log4j.properties`.

## Data
Sample input data is provided in the `test_data/` directory. Expected output DataFrames for tests are in `test_data/results/`.

## License
This project is licensed under the MIT License.
