import os
import sys
import logging
import logging.config
import configparser

import utils
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, max, to_date, col


def main():
 
    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("main")
    logger.info("Program started.")

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


    # read settings.ini, get settings to create url
    settings_file = "configs\\settings.ini"
    section = "file_locations"
    config = configparser.ConfigParser()

    try:
        config.read(settings_file)
        settings = dict(config.items(section))
        input_location  = settings['input_location']
        output_location = settings['output_location']

    except FileNotFoundError:
        logger.error(f"reading settings.ini, FileNotFound Error: {settings_file} not found.")
        logger.critical("Program failed!\n\n")
        sys.exit()
    except configparser.Error as err:
        logger.error(f"reading settings.ini, ConfigParser Error: {err}")
        logger.critical("Program failed!\n\n")
        sys.exit()
    except Exception as errex:
        logger.error(f"reading settings.ini, Exception: {errex}")
        logger.critical("Program failed!\n\n")
        sys.exit()

    customers_csv = input_location + "customers.csv"
    products_csv  = input_location + "products.csv"
    transactions_location  = input_location + "transactions\\"

    try:
        spark = (
                SparkSession.builder
                            .master("local[*]")
                            .appName("data_pipeline")
                            .getOrCreate()
        )

    except Exception as errex:
        logger.error(f"initiating spark session, Exception: {errex}")
        logger.critical("Program failed!\n\n")
        sys.exit()


    customers_df, products_df, transactions_df = utils.read_input_data(
        spark,
        customers_csv,
        products_csv,
        transactions_location
    )

    transactions_df_flattened = utils.flatten_json(transactions_df)

    try:
        joined_df = transactions_df_flattened.join(customers_df, "customer_id", "left").join(products_df, "product_id", "left")
        joined_df = joined_df.drop(customers_df["customer_id"]).drop(products_df["product_id"])
        joined_df = joined_df.withColumn("purchase_date", to_date(col("date_of_purchase")))
        customer_purchases_df = joined_df.groupBy("customer_id", "product_id", "product_description", "product_category")\
                                         .agg(max("loyalty_score").alias("loyalty_score"), count("*").alias("purchase_count"),\
                                              sum("price").alias("total_spend"), max("purchase_date").alias("latest_purchase_date"))

    except Exception as errex:
        logger.error(f"transforming dataframes, Exception: {errex}")

    utils.write_output_data(spark, customer_purchases_df, output_location)


if __name__ == "__main__":
    main()