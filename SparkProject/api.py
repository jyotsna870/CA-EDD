"""
**********************************************************************************************

company : Accenture EDD
File api.py
Module Spark application

@functionality :
read data from api


************************************************************************************************
"""

import requests
import datetime

x = datetime.datetime.now()

jsondata = requests.request('GET', 'http://www.7timer.info/bin/api.pl?lon=113.17&lat=23.09&product=astro&output=json')
print(jsondata.json())
