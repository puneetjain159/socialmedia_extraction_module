import pandas as pd 
import time
import json
import psycopg2
import csv
import io
from sqlalchemy import create_engine, Integer, Float, String, DateTime, Text,BigInteger
from datetime import datetime,timedelta
from googleanalytics.gamodule import googleApi
from utils.postgrewrapper import PostGreWrapper

CONFIG_LOCATION = 'config.json'

# Reading the config file 
with open(CONFIG_LOCATION) as f:
  config = json.load(f)

#  Google API Configuration Files
CONFIG_LOCATION = 'googleanalytics/google_analytic_config.json'
KEY_FILE_LOCATION =  config['googleanalytics']['KEY_FILE_LOCATION']

with open(CONFIG_LOCATION) as f:
  ga_config = json.load(f)




# RDS Connection Strings
rds_con_dict = {
    "host": config['rds_connection']['host'],
    "database": config['rds_connection']['database'],
    "user": config['rds_connection']['user'],
    "password": config['rds_connection']['password']
    }


def handler(event,context):
    '''
    Function to run inside Lambda Container
    '''
    #  Initialize the Google Analytics API 
    ga = googleApi(KEY_FILE_LOCATION = KEY_FILE_LOCATION)

    # Initialize PostGreSQL

    db = PostGreWrapper(rds_con_dict)
    db.connect()

    # Read the Google Configuration Files and start collecting the metrics
    rds_con = {}
    for metric in ga_config:
        requestbody = ga.create_request_json(metric)
        df = ga.convert_json_pandas(ga.get_report(requestbody))
        rds_con["googleanalytics." + metric['name_view'].lower()] = df

    # Write to the RDS 
    db.copy_to_rds(rds_con)

    return "Data written for Google Analytics"

    