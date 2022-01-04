"""
**********************************************************************************************

company : Accenture EDD
File Logic_test.py
Module Spark application

@functionality :
Extend class Logictest from Spark Job supper class
Override the run function


************************************************************************************************
"""
import shared as sh
from execute import SparkJob
from pyspark.sql import SparkSession, DataFrame


class LogicTest(SparkJob):
    def __init__(self):
        super().__init__()

    def load_all_data(self):
        self.TEST_DAT = sh.load_from_csv(
            path="C:\\Users\\balwinder.a.kaur\\Desktop\\address.txt"

        )
        self.TEST_DAT.show()

    def run(self):
        spark = SparkSession.builder.appName("test").getOrCreate()
        self.load_all_data()


def main():
    object = LogicTest(mode=True)
    object.run()


if __name__ == "__main__":
    main()
