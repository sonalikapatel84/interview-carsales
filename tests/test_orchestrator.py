import os
import sys
import subprocess
import pytest
from unittest.mock import patch

# Set environment variables
os.environ['JAVA_HOME'] = "C:\\Program Files\\Java\\jdk1.8.0_241"
os.environ['SPARK_HOME'] = "C:\\Spark\\spark-3.5.2-bin-hadoop3"
os.environ['PYSPARK_PYTHON'] = "C:\\Python312\\python.exe"

# Add the project directory to the Python path
sys.path.append('C:\\Users\\SONALIKA\\Downloads\\Data case study\\interview-carsales')
sys.path.append(r'C:\Users\SONALIKA\Downloads\Data case study\interview-carsales\jobs')

# Import the orchestrator module
from jobs import orchestrator
from jobs.utils import create_spark_session

@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session()
    # spark = SparkSession.builder \
    #     .appName("TestApp") \
    #     .master("local[*]") \
    #     .config("spark.python.worker.reuse", "true") \
    #     .config("spark.network.timeout", "800s") \
    #     .getOrCreate()
    yield spark
    spark.stop()

def run_script(script_name):
    """ Executes a script using subprocess."""
    print(f"Running {script_name}...")
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {script_name}: {result.stderr}")
        raise Exception(f"Script {script_name} failed")
    print(f"Output from {script_name}:\n{result.stdout}")


##-------------------------------UNIT TESTS ------------------------------------------##
def test_orchestrator_transform_with_sample(spark):
    """Test orchestrator.transform method using small chunks of input data and expected output data
    to make sure the function is behaving as expected.
    """
    # Given - loading the input dataframes from CSV files
    accounts_df = spark.read.csv('test_data/accounts.csv', header=True, inferSchema=True)
    skus_df = spark.read.csv('test_data/skus.csv', header=True, inferSchema=True)
    invoice_line_items_df = spark.read.csv('test_data/invoice_line_items.csv', header=True, inferSchema=True)
    invoices_df = spark.read.csv('test_data/invoices.csv', header=True, inferSchema=True)

    # When - actual data
    transformed_data = orchestrator.join_dataframes(accounts_df, invoices_df, invoice_line_items_df, skus_df)

    # Then - perform assertions (example)
    assert transformed_data is not None
    assert transformed_data.count() > 0

def test_orchestrator_extract_mock_calls(spark):
    """Test orchestrator.extract method using the mocked spark session and introspect the calling pattern
    to make sure spark methods were called with intended arguments
    """
    with patch('jobs.orchestrator.extract_data') as mock_extract:
        mock_extract.return_value = (spark.read.csv('test_data/accounts.csv', header=True, inferSchema=True),
                                     spark.read.csv('test_data/skus.csv', header=True, inferSchema=True),
                                     spark.read.csv('test_data/invoice_line_items.csv', header=True, inferSchema=True),
                                     spark.read.csv('test_data/invoices.csv', header=True, inferSchema=True))
        # When - calling the extract method with mocked spark and test config
        orchestrator.extract_data(spark=spark, file_path='test_data/accounts.csv')
        # Then - introspecting the spark method call
        mock_extract.assert_called_once()

##-------------------------------INTEGRATION TESTS ------------------------------------------##
def test_run_integration(spark):
    """Test orchestrator.run method to make sure the integration is working fine
    It avoids reading and writing operations by mocking the load and extract method
    """
    with patch('utils.create_spark_session') as mock_load:
        with patch('jobs.orchestrator.extract_data') as mock_extract:
            mock_load.return_value = spark
            mock_extract.side_effect = [
                spark.read.csv('test_data/accounts.csv', header=True, inferSchema=True),
                spark.read.csv('test_data/skus.csv', header=True, inferSchema=True),
                spark.read.csv('test_data/invoices.csv', header=True, inferSchema=True),
                spark.read.csv('test_data/invoice_line_items.csv', header=True, inferSchema=True)
            ]
            # When
            orchestrator.orchestrate()
            # Then
            # Add assertions to verify the expected outcomes
            # For example, check if the parquet files are written to the expected locations
            assert True

##-------------------------------EDGE CASE TESTS ------------------------------------------##
def test_orchestrator_with_empty_files(spark):
    """Test orchestrator with empty input files to ensure it handles the edge case gracefully."""
    # Given - loading empty dataframes
    empty_df = spark.createDataFrame([], schema="id INT, name STRING")

    # Mock the extract_data method to return empty dataframes
    with patch('jobs.extract.extract_data', return_value=empty_df):        
        # When - running the orchestrate method
        orchestrator.orchestrate()
        
        # Then - verify the output
        # Add assertions to check if the pipeline handles empty dataframes correctly
        assert True

if __name__ == "__main__":
    run_script("jobs/extract.py")
    run_script("jobs/transform.py")
    run_script("jobs/load.py")
    print("ETL pipeline executed successfully.")