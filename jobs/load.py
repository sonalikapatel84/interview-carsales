from utils import create_spark_session


if __name__ == "__main__":
    # Create a SparkSession
    spark = create_spark_session()
    
    # Load the transformed Parquet files
    invoice_profit_df = spark.read.parquet("output/profit_invoice/")
    customer_profit_df = spark.read.parquet("output/profit_customer/")
    
    # Define paths where you want to save the data
    output_invoice_path = "final_output/invoice_profit/"
    output_customer_path = "final_output/customer_profit/"
    
    # Save the DataFrames to specified locations
    invoice_profit_df.write.mode('overwrite').parquet(output_invoice_path)
    customer_profit_df.write.mode('overwrite').parquet(output_customer_path)
    
    print(f"Data successfully saved to {output_invoice_path} and {output_customer_path}.")