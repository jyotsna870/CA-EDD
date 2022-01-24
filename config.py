import logging

""" 
**********************************************************************************************
Company : Accenture EDD
Date 12/28/3021
Module : Spark application 
File : Config file 
 @param Put config properties such as database details 
 **********************************************************************************************
 """
from typing import NamedTuple


def SQLConfig():
    try:
        # from sql_config import SQLConfig as sqlcfg
        print('test')
    # return sqlcfg()
    except (ModuleNotFoundError, ImportError):
        # logging.warning("ANConfig not found")
        return None


# contruct the db str
class DbConf(NamedTuple):
    """
    Class for holding database configuration info
    """

    host: str
    port: int
    user: str
    password: str
    database: str
    db_type: str
    driver: str

    def __str__(self):
        return f"jdbc:{self.db_type}//{self.host}:{self.port}/{self.database}"
