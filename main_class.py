"""
*************************************************************************************************

Date: 1/24/2022
functionaltiy : Main class to start spark job
commands to run the class python main_class.py test.yaml

************************************************************************************************
"""
from SparkJob import SparkJob
from pyspark.sql import SparkSession, DataFrame
import shared as sh
import edd as ed
from argparse import ArgumentParser
import yaml

"""
*************************************************************************************************

Datavalidation function to call the function from shared lib and run the main logic 

************************************************************************************************
"""


def data_validation(
        dictionary_data, spark: SparkSession, df: DataFrame
) -> (DataFrame, DataFrame):
    file_yaml = dictionary_data[ed.DATA_FILE][ed.FILE_NAME]
    list_yaml = dictionary_data[ed.DATA_FILE][ed.VALIDATION][ed.VALIDATION_LIST]

    df_result = sh.create_df(spark)
    count1 = df.count()
    for i in range(len(list_yaml)):
        for j in list_yaml[i][ed.COLUMNS]:
            (count, df, type) = sh.execute(
                list(list_yaml[i].values())[0], j,
                list(list_yaml[i].values())[2], df)
            vals = [(dictionary_data[ed.DATA_FILE][ed.FILE_NAME], j,
                     list(list_yaml[i].values())[0], type, count,
                     count1)]
            df1 = spark.createDataFrame(vals, ed.COLUMNS_LIST)
            df_result = df_result.union(df1)

    return df_result, df


"""
*************************************************************************************************

main class extends SparkJob 
************************************************************************************************
"""


class MainClass(SparkJob):
    def __init__(self, yaml_file):
        super().__init__(yaml_file=yaml_file)
        self.yaml_file = yaml_file

    def read_yaml(self):
        with open(self.yaml_file, 'r') as f:
            self.dictionary_data = yaml.load(f, Loader=yaml.FullLoader)  # also, yaml.SafeLoader

    def load_all_data(self):
        self.data = sh.load_from_csv(self.dictionary_data[ed.DATA_FILE][ed.FILE_NAME])

    """
    *************************************************************************************************
   override the run function 
    ************************************************************************************************
    """

    def run(self):
        spark = SparkSession.builder.appName(
            "DataValidation"
        ).getOrCreate()

        self.read_yaml()
        self.load_all_data()
        (df_result, df_output) = data_validation(self.dictionary_data, spark, self.data)
        df_result.show()
        df_output.show()


def main():
    parser = ArgumentParser()
    parser.add_argument("yaml_file", nargs="?")
    args = parser.parse_args()
    mainclass = MainClass(
        yaml_file=args.yaml_file,

    )
    mainclass.run()


if __name__ == "__main__":
    main()
