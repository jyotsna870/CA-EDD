from pyspark.sql import SparkSession, DataFrame

"""
**********************************************************************************************

company : Accenture EDD
File shared.py
Module Spark application

@functionality :
created some shared functions such as load data from csv and save as csv


************************************************************************************************
"""


def load_from_csv(
        path: str
) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    read_DL_df = spark.read.csv(
        path=path
    )
    return read_DL_df


def save_to_csv(
        df: DataFrame,
        path: str,

) -> None:
    spark = SparkSession.builder.getOrCreate()
    df.write.csv(path=path)
