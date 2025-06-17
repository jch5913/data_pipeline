import logging
import logging.config
import configparser

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType


def get_file_locations(key: str):
    """
    Get input/output locations
    """
 
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("utils")

    settings_file = "configs\\settings.ini"
    section = "file_locations"
    config = configparser.ConfigParser()

    try:
        config.read(settings_file)
        settings = dict(config.items(section))
        file_location  = settings[key]
        return file_location
    except FileNotFoundError:
        logger.error(f"get_file_locations, settings.ini not found.")
        logger.critical("Program failed!\n\n")
        sys.exit()
    except configparser.Error as e:
        logger.error(f"get_file_locations, ConfigParser error: {err}")
        logger.critical("Program failed!\n\n")
        sys.exit()
    except Exception as e:
        logger.error(f"get_file_locations, exception: {e}")
        logger.critical("Program failed!\n\n")
        sys.exit()


def read_data(
    spark: SparkSession,
    _format: str,
    _options: dict,
    _schema: str,
    _location: str
):
    """
    Reads a data file to a dataframe
    """

    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("utils")

    try:
        df = spark.read.format(_format).options(**_options).schema(_schema).load(_location)
        return df
    except Exception as e:
        logger.error(f"read_data, exception: {e}")


def flatten_json(
    df: DataFrame
):
    """
    Flattens a dataFrame with nested fields (arrays and structs) by converting them into individual columns.
    """

    # compute complex fields (arrays and structs) in schema   
    complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
    
        # if struct, use select to expand elements to columns, and drop the struct column
        if (type(complex_fields[col_name]) == StructType):
            #expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
            expanded = [col(col_name+'.'+k).alias(k) for k in [ n.name for n in  complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)
    
        # if array, use explode to expand elements to rows
        elif (type(complex_fields[col_name]) == ArrayType):    
            df = df.withColumn(col_name, explode(col_name))
    
        # recompute remaining complex fields in schema       
        complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])

    return df
