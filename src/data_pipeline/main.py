import sys
import logging
import logging.config

import utils
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, max, to_date, col


def main():
 
    #starting logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("main")
    logger.info("Program started.")

    #starting Spark session
    try:
        spark = (
                SparkSession.builder
                            .master("local[*]")
                            .appName("data_pipeline")
                            .getOrCreate()
        )

    except Exception as e:
        logger.error(f"initiating spark session, exception: {e}")
        logger.critical("Program failed!\n\n")
        sys.exit()

    #reading data
    input_location  = utils.get_file_locations("input_location")

    customers_data = input_location + "customers.csv"
    customers_schema = "customer_id string, loyalty_score int, _corrupt_record string"
    customers_read_options = {"header": "true", "mode": "PERMISSIVE"}
    customers_df = utils.read_data(spark, "csv", customers_read_options, customers_schema, customers_data)

    products_data  = input_location + "products.csv"
    products_schema = "product_id string, product_description string, product_category string, _corrupt_record string"
    products_read_options = {"header": "true", "mode": "PERMISSIVE"}
    products_df = utils.read_data(spark, "csv", products_read_options, products_schema, products_data)

    transactions_data  = input_location + "transactions\\"
    transactions_schema = "customer_id string, basket array<struct<product_id string, price double>>, date_of_purchase timestamp, _corrupt_record string"
    transactions_read_options = {"recursiveFileLookup": "true", "multiLine": "true"}
    transactions_df = utils.read_data(spark, "json", transactions_read_options, transactions_schema, transactions_data)

    #flattening json data
    transactions_df_flattened = utils.flatten_json(transactions_df)

    #transforming data
    try:
        joined_df = transactions_df_flattened.join(customers_df, "customer_id", "left").join(products_df, "product_id", "left")
        joined_df = joined_df.drop(customers_df["customer_id"]).drop(products_df["product_id"])
        joined_df = joined_df.withColumn("purchase_date", to_date(col("date_of_purchase")))
        customer_purchases_df = joined_df.groupBy("customer_id", "product_id", "product_description", "product_category")\
                                         .agg(max("loyalty_score").alias("loyalty_score"), count("*").alias("purchase_count"),\
                                              sum("price").alias("total_spend"), max("purchase_date").alias("last_purchase_date"))
    except Exception as e:
        logger.error(f"transforming dataframes, exception: {e}")

    #writing output
    output_location = utils.get_file_locations("output_location") + "customer_purchases.csv"
    try:
        customer_purchases_df.write.format("csv").mode("overwrite").partitionBy("loyalty_score").option("header", True).save(output_location)
    except Exception as e:
        logger.error(f"writing output data, exception: {e}")

    #writing corrupt data
    corrupt_data_location = utils.get_file_locations("corrupt_data_location")
    customers_corrupt_data = corrupt_data_location + "customers.csv"
    products_corrupt_data  = corrupt_data_location + "products.csv"
    transactions_corrupt_data  = corrupt_data_location + "transactions.json"

    try:
        customers_corrupt_data_df = customers_df.where(col("_corrupt_record").isNotNull())
        products_corrupt_data_df = products_df.where(col("_corrupt_record").isNotNull())
        transactions_corrupt_data_df = transactions_df.where(col("_corrupt_record").isNotNull())

        customers_corrupt_data_df.write.format("csv").mode("overwrite").option("header", True).save(customers_corrupt_data)
        products_corrupt_data_df.write.format("csv").mode("overwrite").option("header", True).save(products_corrupt_data)
        transactions_corrupt_data_df.write.format("json").mode("overwrite").save(transactions_corrupt_data)
    except Exception as e:
        logger.error(f"generating & writing corrupt data, exception: {e}")

    spark.stop()
    logger.info("Program ended.\n\n")


if __name__ == "__main__":
    main()