import json
import os
from utils import create_spark_session


def load_config():
    """Loads configuration from a JSON file."""
    config_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
    with open(config_file, 'r') as f:
        return json.load(f)

def extract_data(spark, file_path):
    """
        Reads a CSV file with multiline fields and a header, and returns a DataFrame.
        Parameters:
            spark : SparkSession The active SparkSession.
            file_path : str Path to the CSV file.
        Returns:
            DataFrame The DataFrame containing the CSV data.
    """
    return spark.read.options(multiline="true", header=True).csv(file_path)

if __name__=="__main__":
    # Load configuration
    config = load_config()

    # Initialize Spark session
    spark = create_spark_session()

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

    # Extracting accounts.csv
    accounts_df.show()
    skus_df.show()
    invoices_df.show()
    invoice_line_items_df.show()
 
    # Save DataFrames as Parquet for intermediate use
    accounts_df.write.mode('overwrite').parquet("output/accounts")
    skus_df.write.mode('overwrite').parquet("output/skus")
    invoices_df.write.mode('overwrite').parquet("output/invoices")
    invoice_line_items_df.write.mode('overwrite').parquet("output/invoice_line_items")
    
# +----------+--------------------+--------------------+------------------+--------------------+------+------------+
# |account_id|        company_name|     company_address|    contact_person|       contact_phone|gender|joining_date|
# +----------+--------------------+--------------------+------------------+--------------------+------+------------+
# |         1|     Apex Auto Group|58987 Crystal Ter...|      Brent Barnes|001-470-476-0187x922|     M|  27/10/2018|
# |         2|     Velocity Motors|5498 Jonathan Ram...|    Jordan Sherman|   590-306-0173x3024|     M|   6/11/2018|
# |         3|   Prime Car Dealers|35614 Amanda Land...|     Jeffery Boone|+1-527-599-2269x3043|     M|  26/12/2016|
# |         4|  Liberty Auto Sales|3732 Suarez Ville...|  Ricardo Robinson|        848.936.5974|     M|  22/08/2017|
# |         5|      Horizon Motors|366 Jackson Hills...|     Michael Bowen|  (088)626-2015x0464|     M|   10/9/2017|
# |         6|   Elite Auto Center|17110 Booth Mount...|     Gabriel Morse|    501-765-1025x899|     M|    2/7/2017|

# +-------------+----------------+----------------+---------------+-----------------+
# |      item_id|       item_name|item_description|item_cost_price|item_retail_price|
# +-------------+----------------+----------------+---------------+-----------------+
# |0-19-815284-1|     AutoAdBoost|High-Performance|    13.38563993|      81.67170578|
# |0-318-32907-7|     CarPromoPro|    Cutting-Edge|    22.92944039|      20.15594251|
# |1-9776-0963-5|  DriveMarketMax|      Innovative|    2.934576054|      2.152106662|
# |1-08-136974-4|    MotorAdElite|  Cost-Effective|    11.99106842|      46.21556397|

# +----------+----------+-----------+
# |invoice_id|account_id|date_issued|
# +----------+----------+-----------+
# |         1|         1| 2021-01-03|
# |         2|         1| 2020-03-19|

# +----------+-------------+--------+
# |invoice_id|      item_id|quantity|
# +----------+-------------+--------+
# |         1|1-60559-539-X|       4|
# |         1|1-944086-93-5|       3|


   