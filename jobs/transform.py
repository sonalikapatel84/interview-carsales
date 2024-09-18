from pyspark.sql import functions as F
from utils import create_spark_session


def join_dataframes(accounts_df, invoices_df, invoice_line_items_df, skus_df):
    """
    Joins accounts, invoices, invoice line items, and SKU data into a single DataFrame.
    """
    # Join accounts with invoices
    joined_df = invoices_df.join(accounts_df, "account_id", "left")
    # Join invoice line items with SKUs to get cost and retail prices
    items_df = invoice_line_items_df.join(skus_df, "item_id", "left")
    return joined_df.join(items_df, "invoice_id", "left")

def calculate_profit_per_invoice(df):
    """
    Calculates profit for each invoice by subtracting cost price from 
    retail price and multiplying by quantity sold.
    """
    # Add a profit column for each line item
    df = df.withColumn("profit_per_item", F.col("item_retail_price") - F.col("item_cost_price"))
    df = df.withColumn("total_profit", F.col("profit_per_item") * F.col("quantity"))
    # Aggregate total profit per invoice
    return df.groupBy("invoice_id").agg(F.sum("total_profit").alias("total_invoice_profit"))

def calculate_profit_per_customer(df):
    """
    Aggregates total profit per customer.
    """
    # Calculate profit per invoice first
    invoice_profit_df = calculate_profit_per_invoice(df)
    # Join invoice profit with the main DataFrame to get customer information
    df_with_profit = df.join(invoice_profit_df, on="invoice_id", how="left")
    # Aggregate total profit per customer
    return df_with_profit.groupBy("account_id").agg(F.sum("total_invoice_profit").alias("total_customer_profit"))


if __name__ == "__main__":

     # Create a SparkSession
    spark = create_spark_session()
    
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

    # Show transformed data
    full_df.show()
    invoice_profit_df.show()
    customer_profit_df.show()

    # Save the results as Parquet for downstream analytics
    invoice_profit_df.write.mode('overwrite').parquet("output/profit_invoice/")
    customer_profit_df.write.mode('overwrite').parquet("output/profit_customer/")

# +----------+----------+-----------+---------------+--------------------+--------------+--------------------+------+------------+-------------+--------+----------------+-------------------+---------------+-----------------+
# |invoice_id|account_id|date_issued|   company_name|     company_address|contact_person|       contact_phone|gender|joining_date|      item_id|quantity|       item_name|   item_description|item_cost_price|item_retail_price|
# +----------+----------+-----------+---------------+--------------------+--------------+--------------------+------+------------+-------------+--------+----------------+-------------------+---------------+-----------------+
# |        11|         1| 2020-08-13|Apex Auto Group|58987 Crystal Ter...|  Brent Barnes|001-470-476-0187x922|     M|  27/10/2018|0-16-697190-1|       3|    AutoAdMaster|        Multi-Touch|    41.61074125|      12.32305887|
# |        11|         1| 2020-08-13|Apex Auto Group|58987 Crystal Ter...|  Brent Barnes|001-470-476-0187x922|     M|  27/10/2018|0-575-85477-4|       3|    VehicleAdMax|           Adaptive|     2.96812806|      45.54531476|
# |        11|         1| 2020-08-13|Apex Auto Group|58987 Crystal Ter...|  Brent Barnes|001-470-476-0187x922|     M|  27/10/2018|1-9776-0963-5|       4|  DriveMarketMax|         Innovative|    2.934576054|      2.152106662|
# |        11|         1| 2020-08-13|Apex Auto Group|58987 Crystal Ter...|  Brent Barnes|001-470-476-0187x922|     M|  27/10/2018|1-60620-035-6|       2|      CarAdBlitz|             Secure|    5.668773089|      157.3289851|

# +----------+--------------------+
# |invoice_id|total_invoice_profit|
# +----------+--------------------+
# |       296|       158.194185995|
# |       467|      1016.429290028|
# |       675|        105.71735566|
# |       691|       229.764433452|

# +----------+---------------------+
# |account_id|total_customer_profit|
# +----------+---------------------+
# |       125|   21208.634644720994|
# |        51|   229451.74937294459|
# |         7|   215269.62040368342|
# |       124|    73506.25420762588|