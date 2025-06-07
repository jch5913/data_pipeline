import logging
import logging.config

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType


def read_input_data(
    spark: SparkSession,
    customers_location: str,
    products_location: str,
    transactions_location: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Reads customers.csv, products.csv and transactions json files into dataframes
    """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("utils")
    logger.info("Program started.")

    _csv_options = {"header": "true", "mode": "PERMISSIVE"}
    _customers_schema = "customer_id string, loyalty_score int, _corrupt_record string"
    _products_schema = "product_id string, product_description string, product_category string, _corrupt_record string"

    _json_options = {"recursiveFileLookup": "true", "mode": "PERMISSIVE"}
    _transactions_schema = "customer_id string, basket array<struct<product_id string, price double>>, date_of_purchase datetime, _corrupt_record string"

    try:
        customers_df = spark.read.option(**_csv_options).schema(_customers_schema).csv(customers_location)
        products_df  = spark.read.option(**_csv_options).schema(_products_schema).csv(products_location)
        transactions_df = spark.read.option(**_json_options).schema(_transactions_schema).json(transactions_location, multiLine=True)
        return customers_df, products_df, transactions_df
    except Exception as errex:
        logger.error(f"read_input_data, Spark exception: {errex}")


def write_output_data(
    spark: SparkSession,
    df: DataFrame,
    output_location: str
):
    """
    Writes a dataframe to csv, save a table in default db.
    """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("utils")
    logger.info("Program started.")

    try:
        df.write.mode("overwrite").option("header", True).partitionBy("loyalty_score").bucketBy(10, "product_id").sortBy("product_id").saveAsTable("customer_purchases").csv(output_location)
    except Exception as errex:
        logger.error(f"write_output_data, Spark exception: {errex}")


def flatten_json(df):
    """
    Flattens a dataFrame with complex nested fields (Arrays and Structs) by converting them into individual columns.
    """

    # compute complex fields (arrays and structs) in schema   
    complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
    
        # if struct, use select to expand elements to columns, and drop the struct column
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)
    
        # if array, use explode to expand elements to rows
        elif (type(complex_fields[col_name]) == ArrayType):    
            df = df.withColumn(col_name, explode(col_name))
    
        # recompute remaining complex fields in schema       
        complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])

    return df
