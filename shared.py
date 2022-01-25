from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

"""
**********************************************************************************************

company : Accenture EDD
File shared.py
Date: 01/24/2022

Module Spark application

@functionality :
created some shared functions such as load data from csv and save as csv


************************************************************************************************
"""

import yaml
import pyspark.sql.functions as fx


def load_from_yaml(path: str):
    with open('properties.yaml') as fh:
        dictionary_data = yaml.safe_load(fh)

        return dictionary_data


def load_from_csv(
        path: str
) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('address', StringType(), True),

    ])

    read_DL_df = spark.read.option("header", True).schema(schema). \
        csv(
        path=path
    )
    return read_DL_df


def save_to_csv(
        df: DataFrame,
        path: str,

) -> None:
    spark = SparkSession.builder.getOrCreate()
    df.write.csv(path=path)


def get_dtype1(df, colname, data_type):
    count = 0
    try:
        for name, dtype in df.dtypes:
            if name == colname:
                if (dtype != data_type):
                    count = count + 1


    except:
        print('error happend')
    return count


def execute(keyword, column_name, replace_value, df: DataFrame):
    if (keyword == "null_check"):
        return null_check(df, column_name, replace_value)
    if (keyword == "data_check"):
        return data_check(df, column_name, replace_value)


def verfiy_null(df: DataFrame, c) -> DataFrame:
    return df.filter(fx.col("last_name") == ' ').count()


def create_df(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField('data_file', StringType(), True),
        StructField('columns', StringType(), True),
        StructField('validation', StringType(), True),
        StructField('validtion_type', StringType(), True),
        StructField('fail_counts', StringType(), True),
        StructField('total_counts', StringType(), True)
    ])
    # columns = ['data_file', 'columns', 'validation', 'fail_counts', 'total_counts']

    df_result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)
    return df_result


def get_dtype(df, colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


def data_check(df: DataFrame, colname, type):
    count = 0
    if (get_dtype(df, colname) != type):
        count = count + 1
    return count, df, type


def null_check(df: DataFrame, c, repl_value) -> (str, DataFrame, str):
    return df.filter(fx.col(c) == ' ').count(), df.withColumn(c, fx.when(fx.col(c) == ' ', repl_value).otherwise(
        fx.col(c))), ' '

# def data_check(df: DataFrame, c, repl_value) -> (str, DataFrame):
#  return dict(df.select(fx.col(c)).dtypes)[c], df
