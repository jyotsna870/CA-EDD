"""
date: 01/24/2022
"""
import json
import sys
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


def spark_main(job: str, spark_jobs: Container[Type[SparkJob]]):
    """
    Spark applications typically involve multiple "jobs" that must be
    deployed to a Spark cluster that each implement different business
    logic and can each be parameterized differently.

    For example, a ML "fit" job may take hyperparameters as input or
    parameters to load the desired training data. A ML "transform" job
    may take model selection parameters, or parameters to load
    the desired production data. Both jobs would be deployed as
    part of the same Spark application.

    This function implements a single driver that
    uses the SparkJob API to initialize a job
    using the parameters provided by a spark-submit.

    Example:

    Given a `main.py` with the following pattern:
    ```
    from accenture.spark_util.spark_util import SparkJob, spark_main


    class Fit(SparkJob):

        def __init__(self, foo: str):
            super().__init__(foo=foo)
            self.foo = foo

        def run(self):
            print('fit: {foo}'.format(foo=self.foo)


    class Transform(SparkJob):

        def __init__(self, bar: str):
            super().__init__(bar=bar)
            self.bar = bar

        def run(self):
            print('transform: {bar}'.format(bar=self.bar)


    if __name__ == '__main__':
        spark_main(sys.argv[1], [Fit, Transform])
    ```

    And given the `spark-util` dependency has been packaged
    and deployed to my-app.whl.

    A `Fit` job with `foo="hello world"` could be submitted as:
    ```
    from subprocess import check_call

    check_call([
        'spark-submit',
        # ... typical spark-submit arguments
        --py-files my-app.whl,
        'main.py',
        Fit("hello world")
    ])
    ```

    :param job:
    :param spark_jobs: SparkJob types that are available to be initialized.
    :return: None
    """
    job_class = class_getter(job["name"], spark_jobs)
    job_class.from_json(job["params"]).run()


def string_importer(str_cls: str):
    *mod, cls = str_cls.split(".")
    imod = importlib.import_module(".".join(mod))
    return getattr(imod, cls)


def class_getter(job, spark_jobs):
    cls = spark_jobs.get(job)
    if not cls:
        print(f"Job key {job} not found. Attempting to import...")
        cls = string_importer(job)
    return cls
