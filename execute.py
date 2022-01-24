"""
**********************************************************************************************

company : Accenture EDD
File execute.py
Module Spark application

@functionality :
create abstract class SparkJob
Create abstract run that need to defind in base class

************************************************************************************************
"""
import json
from abc import ABC, abstractmethod
from typing import Container, Type
import importlib


class SparkJob(ABC):
    """
    A SparkJob implements some conventions for:
    - Parameterizing a Spark application.
    - Serializing a Spark application / parameters
      (to be passed via spark-submit).

    See spark_main for example usage.
    """

    def __init__(self, **json_serializable_params):
        """
        Classes that implement SparkJob must always invoke SparkJob.__init__

        :param json_serializable_params: Classes that implement SparkJob
        must convert their initialization parameters to keyword parameters
        and ensure they are JSON serializable.

        See spark_main for an example.
        """
        self.json_serializable_params = json_serializable_params

    def serialize(self):
        return json.dumps(
            {"name": self.__class__.__name__, "params": self.json_serializable_params}
        )

    __str__ = serialize

    @classmethod
    def from_json(cls, params):
        return cls(**params)

    @abstractmethod
    def run(self):
        """
        `run` must be overridden with the business logic for this SparkJob.
        :return: None

        See spark_main for an example.
        """
        pass


def spark_main(job, spark_jobs: Container[Type[SparkJob]]):
    job_class = class_getter(job, spark_jobs)
    cls = string_importer(job)

    cls.from_json(spark_jobs["params"]).run()


def string_importer(str_cls: str):
    *mod, cls = str_cls.split(".")
    imod = importlib.import_module(".".join(mod))
    return getattr(imod, cls)


def class_getter(job, spark_jobs):
    print("job:" + job)
    cls = spark_jobs.get("name")
    print(cls)
    if not cls:
        print(f"Job key {job} not found. Attempting to import...")
        cls = string_importer(job)
    return cls


"""
if __name__ == '__main__':
        spark_main(sys.argv[1])
if __name__ == '__main__':
    job_dict = (
        {
            "name": "LogicTest",
            "params": json.loads('{ }'),
        }

    )
    spark_main("logic_test.LogicTest", job_dict)
"""
