import json
from argparse import ArgumentParser
from execute import spark_main


class SparkLocal:

    def __init__(
            self,
            spark_job_name: str,

    ):
        self.spark_job_name = spark_job_name

    def submit(self):
        """
        Submit job to local spark
        """

        job_dict = (
            {
                "name": self.spark_job_name,
                "params": json.loads('{ }'),
            }

        )
        spark_main(self.spark_job_name, job_dict)


if __name__ == "__main__":
    parser = ArgumentParser()

    # parser.add_argument("--job-name", dest="job_name")

    parser.set_defaults(
        job_name=None,

    )

    args = parser.parse_args()

    sparklocal = SparkLocal(
        spark_job_name="logic_test.LogicTest",

    )

    sparklocal.submit()
