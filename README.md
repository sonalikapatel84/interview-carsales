## Table of Contents
- [Carsales ETL Pipeline with Apache Spark.](#project-overview)
- [Installation steps and executing this job on Spark in Local Mode](#installation)
- [Run the ETL pipeline](#run-pipeline)
- [Configuration](#config)
- [Testing](#testing)
- [Design Choices](#design)
- [Scaling Considerations](#scaling)
- [Contributing](#contribute)
- [License](#license)

## Carsales ETL Pipeline with Apache Spark.
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark to process car sales data. The pipeline extracts raw data from CSV files, transforms it into meaningful features required by data scientists, and loads the processed data into Parquet files for downstream analytics.

## Prerequisites
- Python 3.12 or later
- Java JDK 1.8.0_241 or later
- Apache Spark 3.5.2 or later
  
## Installation steps and executing this job on Spark in Local Mode
    
    1. **Clone the Github repository:**
        sh git clone https://github.com/sonalikapatel84/interview-carsales.git
        cd interview-carsales

    2. **Set up the environment**:
        - Install Java (JDK 1.8.0_241 or later)
        - Install Apache Spark (version 3.5.2 or later)
            Apache Spark can be installed locally by downloading a pre-built package.
            Steps:
                Go to the Apache Spark download page.
                Choose "Pre-built for Apache Hadoop" option and click Download Spark.
                For Windows:
                    Extract the downloaded .tgz file.
                    Set SPARK_HOME environment variable to point to the Spark folder.
                    Add %SPARK_HOME%\bin to the system's PATH.
        - Install Python (version 3.12 or later)
            If not https://www.python.org/downloads/release/python-3913/?source=post_page-----2eb2a27523a3--------------------------------
            Verify by this comand -> **python --version**  

    3. **Set environment variables**:
        (Linux)
        export JAVA_HOME="C:\Program Files\Java\jdk1.8.0_241"
        export SPARK_HOME="C:\Spark\spark-3.5.2-bin-hadoop3"
        export PATH=$SPARK_HOME/bin:$PATH
        export PYSPARK_PYTHON="C:\Python312\python.exe"
        (Windows)
        set JAVA_HOME="C:\Program Files\Java\jdk1.8.0_241"
        set SPARK_HOME="C:\Spark\spark-3.5.2-bin-hadoop3"
        set PATH=$SPARK_HOME/bin:$PATH
        set PYSPARK_PYTHON="C:\Python312\python.exe"

        Verify -> pyspark

    4. **Install Python dependencies**:
       pip install -r requirements.txt

## Run the ETL pipeline
   python orchestrator.py
   The output of the job is saved in the output folder, with a _SUCCESS file and a few .csv files containing the result. 
   On successful run it should show![image](https://github.com/user-attachments/assets/31bab480-725c-4da8-992a-5ce9ae0c67f1)

## Configuration
    The configuration file `config/config.json` contains paths to the input CSV files and other settings. Update this file as needed
    json 
    { 
        "accounts_file_path": "data/accounts.csv", 
        "skus_file_path": "data/skus.csv", 
        "invoices_file_path": "data/invoices.csv", 
        "invoice_line_items_file_path": "data/invoice_line_items.csv" 
    }

## Testing
1. **Run all tests**:
    pytest .\tests\test_orchestrator.py
    on successful run it should show ![image](https://github.com/user-attachments/assets/3caa40c5-ac90-4cdc-9917-da20638b5c71)
   

2. **Test specific functionality**:
      Individual tests can be also run ![image](https://github.com/user-attachments/assets/013bf9e3-622a-4aa4-b891-97cb6a23b228)
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
# interview-carsales
This script consumes four csv files and produces datasets which help in predictive analysis of the customers payment obligations.
