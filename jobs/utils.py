from pyspark.sql import SparkSession

def create_spark_session():
    """
    Creates and returns a SparkSession.
    """
    return SparkSession.builder \
        .appName("InvoiceETL") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()