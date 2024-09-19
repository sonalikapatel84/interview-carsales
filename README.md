## Table of Contents
- [Carsales ETL Pipeline with Apache Spark](#carsales-ETL-Pipeline-with-Apache-Spark)
- [Prerequisites](#prerequisites)
- [Installation steps and executing this job on Spark in Local Mode](#installation-steps-and-executing-this-job-on-Spark-in-Local-Mode)
- [Run the ETL pipeline](#run-the-etl-pipeline)
- [Configuration](#configuration)
- [Testing](#testing)
- [Design Choices](#design-choices)
- [Scaling Considerations](#scaling-considerations)
- [Contributing](#contributing)
- [License](#license)

## Carsales ETL Pipeline with Apache Spark
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark to process car sales data. The pipeline extracts raw data from CSV files, transforms it into meaningful features required by data scientists, and loads the processed data into Parquet files for downstream analytics.

## Prerequisites
- Python 3.12 or later
- Java JDK 1.8.0_241 or later
- Apache Spark 3.5.2 or later
  
## Installation steps and executing this job on Spark in Local Mode
    
**Clone the Github repository:**

   git clone https://github.com/sonalikapatel84/interview-carsales.git
   
   Note: This creates a folder in your home directory (Windows)
   
   cd interview-carsales
      

**Set up the environment**:

    Install Java (JDK 1.8.0_241 or later)
    
    Install Apache Spark (version 3.5.2 or later)
        Apache Spark can be installed locally by downloading a pre-built package.
        Steps:
            Go to the Apache Spark download page.
            Choose "Pre-built for Apache Hadoop" option and click Download Spark.
            For Windows:
                Extract the downloaded .tgz file.
                Set SPARK_HOME environment variable to point to the Spark folder.
                Add %SPARK_HOME%\bin to the system's PATH.
                
    Install Python (version 3.12 or later)
        If not https://www.python.org/downloads/release/python-3913/?source=post_page-----2eb2a27523a3--------------------------------
        Verify by this comand -> **python --version**  
**Set environment variables**:
    (Windows)
    set JAVA_HOME="C:\Program Files\Java\jdk1.8.0_241"
    set SPARK_HOME="C:\Spark\spark-3.5.2-bin-hadoop3"
    set HADOOP_HOME="C:\hadoop"
    set PYSPARK_PYTHON="C:\Python312\python.exe"
    Additionally, ensure that the directories for JAVA_HOME, SPARK_HOME, and HADOOP_HOME are added to your PATH environment variable. You can do this by appending the following lines to your PATH variable:
    set PATH=%PATH%;%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin


    Verify -> pyspark

**Install Python dependencies**:
   pip install -r requirements.txt

## Run the ETL pipeline
   python jobs/orchestrator.py
   The output of the job is saved in the output folder, with a _SUCCESS file and a few .csv files containing the result. 
   On successful run it should show ![ETL Orchestrator Execution](https://github.com/sonalikapatel84/interview-carsales/blob/main/assets/Output.png)
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
    on successful run it should show ![ETL Test Orcestrator Execution](https://github.com/sonalikapatel84/interview-carsales/blob/main/assets/Test%20orchestrator%20run.png)

2. **Test specific functionality**:
      Individual tests can be also run 
    - Unit tests for individual functions
    - Integration tests for the entire ETL pipeline ![ETL Single Test Orcestrator Execution](https://github.com/sonalikapatel84/interview-carsales/blob/main/assets/Test%20orchestrator%20single%20test%20run.png)

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
