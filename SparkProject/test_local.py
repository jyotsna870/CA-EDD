"""
**********************************************************************************************

date: 12/27/2021
filename: test_local.py
subject : spark
JobType : TestLocal


************************************************************************************************
"""
from execute import SparkJob
from pyspark.sql import SparkSession, DataFrame


class TestLocal(SparkJob):
    def __init__(self, mode=False):
        super().__init__(mode=mode)
        self.mode = mode

    def run(self):
        spark = SparkSession.builder.appName("Test_Local").getOrCreate()
        print("test--------------------")
        df = spark.read.csv("C:\\Users\\balwinder.a.kaur\\Desktop\\address.txt")
        df.show()


def main():
    object = TestLocal(mode=True)
    object.run()


if __name__ == "__main__":
    main()
