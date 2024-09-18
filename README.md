

Table of Contents
Project Overview
Installation
Usage
Configuration
Testing
Design Choices
Scaling Considerations
Contributing
License

Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark to process car sales data. The pipeline extracts raw data from CSV files, transforms it into meaningful features required by data scientists, and loads the processed data into Parquet files for downstream analytics.

Installation
1. Clone the repository:
    sh git clone <repository_url> cd <repository_directory>

2. **Set up the environment**:
    - Install Java (JDK 1.8.0_241 or later)
    - Install Apache Spark (version 3.5.2 or later)
    - Install Python (version 3.12 or later)

3. **Set environment variables**:

sh export JAVA_HOME="C:\Program Files\Java\jdk1.8.0_241" export SPARK_HOME="C:\Spark\spark-3.5.2-bin-hadoop3" export PYSPARK_PYTHON="C:\Python312\python.exe"

4. **Install Python dependencies**:

sh pip install -r requirements.txt

## Usage
1. **Run the ETL pipeline**:

sh python orchestrator.py

## Configuration
The configuration file `config/config.json` contains paths to the input CSV files and other settings. Update this file as needed

json { "accounts_file_path": "data/accounts.csv", "skus_file_path": "data/skus.csv", "invoices_file_path": "data/invoices.csv", "invoice_line_items_file_path": "data/invoice_line_items.csv" }

## Testing
1. **Run all tests**:

sh pytest

2. **Test specific functionality**:
    - Unit tests for individual functions
    - Integration tests for the entire ETL pipeline

    ## Design Choices
- **Intermediate Data Models**: Parquet files are used for intermediate storage due to their efficiency and support for complex data types.
- **Modular Design**: The ETL process is divided into separate scripts (`extract.py`, `transform.py`, `load.py`) for better maintainability and testing.
- **Error Handling**: The pipeline includes error handling to manage issues during data extraction, transformation, and loading.

## Scaling Considerations
- **Distributed Processing**: Apache Spark is used to handle large datasets efficiently by distributing the workload across multiple nodes.
- **Configuration Tuning**: Spark configurations can be adjusted to optimize performance based on the data volume and cluster resources.

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit them (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a pull request.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.