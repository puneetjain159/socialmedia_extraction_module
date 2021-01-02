from os import X_OK
import pandas as pd 
import tweepy
import time
import psycopg2
import csv
import io
import json
from sqlalchemy import create_engine, Integer, Float, String, DateTime, Text,BigInteger
from datetime import datetime,timedelta
from twitter.TwitterAPI import TwitterAPI
from utils.postgrewrapper import PostGreWrapper

CONFIG_LOCATION = 'config.json'

# Reading the config file 
with open(CONFIG_LOCATION) as f:
  config = json.load(f)


# Twitter API configuration files
consumer_key = config['twitter']['consumer_key']
consumer_secret = config['twitter']['consumer_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']
user_name = config['twitter']['user_name']


# RDS Connection Strings
rds_con_dict = {
    "host": config['rds_connection']['host'],
    "database": config['rds_connection']['database'],
    "user": config['rds_connection']['user'],
    "password": config['rds_connection']['password']
    }



auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


def handler(event,context):
    '''
    Function to run inside Lambda Container
    '''
    #  Initiate the PostGreSQL PostWrapper

    db = PostGreWrapper(rds_con_dict)
    db.connect()

    # Initiate the twitter class
    twitterapi = TwitterAPI(api = api, 
                            user_name = user_name)

    # get Max Tweets id
    try:
        since_id = pd.read_sql("""
                    SELECT max(delta_id)
                    FROM twitter.tweets
                    """, con = db.rds_con)
        since_id = since_id.iloc[0][0]
    except:
        since_id = None

                        
    # Get all the latest tweets from the user timeline
    status = twitterapi.GetStatus(since_id)
    tweets = twitterapi.Iterator(status)

    # Create the dataframe from pandas

    if tweets != []:
        data = pd.DataFrame(tweets)
        data['created_at'] = pd.to_datetime(data['created_at'])
        data['updated_at'] = datetime.today()
        delta = datetime.today() - timedelta(days=30)
        data_delta = data[data['created_at'] <= str(delta) ]
        if data_delta.shape[0] > 0:
            data_delta.sort_values('created_at',ascending=True,inplace=True)
            data['delta_id'] = data_delta.iloc[0]['id']
        else:
            data.sort_values('created_at',ascending=False,inplace=True)
            data['delta_id'] = data.iloc[0]['id']
        rds_con = {'twitter.tweets': data}
    else:
        rds_con = {}

    # Create the user dataframe
    status = twitterapi.GetStatus()
    data_user = pd.DataFrame(twitterapi.IteratorUser(status))
    data_user['updated_at'] = datetime.today()
    rds_con['twitter.user_data'] = data_user


    # 
    # data.to_csv('tweet.csv',index=False)
    # data_user.to_csv('user.csv',index = False)
    # Write the data back to SQL

    db.copy_to_rds(rds_con)

    return "Data updated For the Twitter API"