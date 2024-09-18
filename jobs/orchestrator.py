import subprocess
from utils import create_spark_session
from jobs.extract import load_config, extract_data
from jobs.transform import join_dataframes, calculate_profit_per_invoice, calculate_profit_per_customer


def run_script(script_name):
    """ 
    Executes a script using subprocess.
    """
    print(f"Running {script_name}...")
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {script_name}: {result.stderr}")
        raise Exception(f"Script {script_name} failed")
    print(f"Output from {script_name}:\n{result.stdout}")


def orchestrate():
    # Load configuration
    config = load_config()

    # Initialize Spark session
    spark = create_spark_session()


    ##---------------------EXTRACTION PROCESS-----------------------------##

    # Use the file path from the config
    accounts_file_path = config["accounts_file_path"]
    skus_file_path = config["skus_file_path"]
    invoices_file_path = config["invoices_file_path"]
    invoice_line_items_file_path = config["invoice_line_items_file_path"]

    # Extract data from CSV files
    accounts_df = extract_data(spark, accounts_file_path)
    skus_df = extract_data(spark, skus_file_path)
    invoices_df = extract_data(spark, invoices_file_path)
    invoice_line_items_df = extract_data(spark, invoice_line_items_file_path)

    # Save DataFrames as Parquet for intermediate use
    accounts_df.write.mode('overwrite').parquet("output/accounts")
    skus_df.write.mode('overwrite').parquet("output/skus")
    invoices_df.write.mode('overwrite').parquet("output/invoices")
    invoice_line_items_df.write.mode('overwrite').parquet("output/invoice_line_items")

    ##---------------------TRANSFORM PROCESS-----------------------------##

    # Load the extracted parquet files
    accounts_df = spark.read.parquet("output/accounts/")
    skus_df = spark.read.parquet("output/skus/")
    invoices_df = spark.read.parquet("output/invoices/")
    invoice_line_items_df = spark.read.parquet("output/invoice_line_items/")

    # Join dataframes to get full dataset
    full_df = join_dataframes(accounts_df, invoices_df, invoice_line_items_df, skus_df)

    # Calculate profit for each invoice
    invoice_profit_df = calculate_profit_per_invoice(full_df)

    # Calculate profit per customer
    customer_profit_df = calculate_profit_per_customer(full_df)

    # Save the results as Parquet for downstream analytics
    invoice_profit_df.write.mode('overwrite').parquet("output/profit_invoice/")
    customer_profit_df.write.mode('overwrite').parquet("output/profit_customer/")


    ##----------------------------LOAD PROCESS--------------------------##

    # Create a SparkSession for loading
    load_spark = create_spark_session()
    
    # Load the transformed Parquet files
    invoice_profit_df = load_spark.read.parquet("output/profit_invoice/")
    customer_profit_df = load_spark.read.parquet("output/profit_customer/")
    
    # Define paths where you want to save the data
    output_invoice_path = "final_output/invoice_profit/"
    output_customer_path = "final_output/customer_profit/"
    
    # Save the DataFrames to specified locations
    invoice_profit_df.write.mode('overwrite').parquet(output_invoice_path)
    customer_profit_df.write.mode('overwrite').parquet(output_customer_path)
    
    print(f"Data successfully saved to {output_invoice_path} and {output_customer_path}.")

if __name__ == "__main__":
    orchestrate()
    print("ETL pipeline executed successfully.")