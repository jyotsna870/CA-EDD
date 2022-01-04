from typing import Dict, List
from argparse import ArgumentParser, Namespace
import json



class Nothing:
    """
    Class for suppressing output from subprocess.
    """

    def nothing(*args):
        pass

    write = nothing
    errors = nothing
    flush = nothing



def get_spark_confs(conf_list: List[str]) -> List:
    """
    Take spark configuration list of strings from arg-parser and convert to a list of tuples pairing each
    spark configuration with '--conf'. If no spark configurations are provided, return an empty list.
    """
    if conf_list is None:
        return []

    if "," in conf_list[0]:
        # fail if user submit config as --config arg1,arg2,arg3,...
        raise ValueError(
            "Unexpected --conf format {}. Please submit as --config arg1 --config arg2 ect...".format(
                conf_list[0]
            )
        )

    spark_confs = [("--conf", conf) for conf in conf_list]

    return spark_confs


class SparkSubmit:
    """
    A EmrStep standardizes how spark jobs are submitted to EMR via boto3.

    See main program for example.
    """

    def __init__(
        self,
        spark_job_name: str,

    ):

        self.spark_job_name = spark_job_name








    @staticmethod
    def _flatten_confs(list_of_confs: List) -> List[str]:
        """
        Check if the list_of_confs from the user-provided spark configurations is in tuple format.
        if its a tuple, it will convert a list of tuples to a list.
        """
        # make sure list isn't empty
        if list_of_confs and type(list_of_confs[0]) == tuple:
            conf_flat_list = [item for sublist in list_of_confs for item in sublist]
            return conf_flat_list

        return list_of_confs

    def _add_spark_confs(self) -> List[str]:
        """
        Add user-provided spark configurations to default configurations.
        If duplicate values are provided, pyspark will use last provided value.
        """
        # base spark configurations
        default_spark_confs = [


        ]

        all_spark_confs = default_spark_confs

        # cli shortcuts for additional spark configs



        return all_spark_confs




    def _collect_step_args(self) -> List[str]:
        """
        Gather spark jar files, spark configurations, and job arguments into a single list.
        """
        spark_jars = [
            "spark-submit",

        ]

        spark_args = self._add_spark_confs()

        job_dict = json.dumps(
            {
                "name": "logic_test.LogicTest",
                "params": '{ }',
            }
        )

        # reformatting required for cluster deploy mode
        job_dict = job_dict.replace("}}", "} }")
        job_dict = job_dict.replace("{{", "{ {")
        driver_program = "shared.py logic_test.py execute.py"

        job_args = [
            "--py-files",
            "shared.py execute.py logic_test.py"
,
            job_dict,
        ]

        step_args = spark_jars + spark_args + job_args

        return step_args

    def submit(self):
        """
        Submit job to cluster.
        """
        assert self.spark_job_name is not None, "Job name must be supplied"

        step_args = self._collect_step_args()
        print(step_args)





if __name__ == "__main__":
    import sys

    parser = ArgumentParser()

    subparsers = parser.add_subparsers()
    job_subparser = subparsers.add_parser("job")



    job_subparser.set_defaults(
        job_name=None,


    )

    args = parser.parse_args()





    mystep = SparkSubmit(
        spark_job_name="logic_test.LogicTest"



    )

    mystep.submit()


