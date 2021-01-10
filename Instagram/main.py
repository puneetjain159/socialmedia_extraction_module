from Instagram.base_api import InstagramAPI
from Instagram.mediafetcher import media
from Instagram.user import UserAPI
from Instagram.insightsfetcher import InsightsAPI
from utils.postgrewrapper import PostGreWrapper


import pytz
import json
import pandas as pd
from datetime import datetime,timedelta
import dateutil.parser as dateparser
from pangres import upsert
import shutil


CONFIG_LOCATION = 'config.json'

# Reading the config file 
with open(CONFIG_LOCATION) as f:
  config = json.load(f)

# RDS Connection Strings
rds_con_dict = {
    "host": config['rds_connection']['host'],
    "database": config['rds_connection']['database'],
    "user": config['rds_connection']['user'],
    "password": config['rds_connection']['password']
    }


API = InstagramAPI(access_token = config['instagram']['access_token'],
                   username = config['instagram']['ig_username'])
API.GetUserInstaid()

m = media(API)

# Get all the content has a limit or since filter with it
datetime.today()
delta = datetime.today() - timedelta(days=700)
utc=pytz.UTC
delta =utc.localize(delta)

post_content = m.mediafetcher(limit =25,since =delta)

# Process the comments into another list 
##################################################
comments = []
for post in post_content['json_data']['data']:
    _comments = m.process_comments(post)
    comments.extend(_comments)

comments = pd.DataFrame(comments)
comments['update_date'] = str(datetime.today())
comments.set_index('id',inplace=True)
#################################################
# Process and correct the insights
##################################################
post_content = m.process_insights(post_content)
post_content = pd.DataFrame(post_content['json_data']['data'])
post_content['update_date'] = str(datetime.today())
#################################################
# Get the user statistics
############################################################################
userapi = UserAPI(API)
insightapi = InsightsAPI(API)
user_data = userapi._GetDailyUserData()['json_data']
daily_insight = insightapi._GetDailyInsights()
lifetime_insight = insightapi._GetLifeTimeInsights()

for metric in daily_insight['json_data']['insights']['data']:
    user_data["daily_" + metric['name']] = metric['values'][1]['value']

user_data['update_date'] = str(dateparser.parse(metric['values'][1]['end_time']))

for metric in lifetime_insight['json_data']['insights']['data']:
    user_data["lifetime_" + metric['name']] = metric['values'][0]['value']
############################################################################


########## RDS connector #############
db = PostGreWrapper(rds_con_dict)
db.connect()

# Incrementaly update and change the rows
upsert(db.rds_con, comments, "comments", "update", schema="instagram", create_schema=False, add_new_columns=True, adapt_dtype_of_empty_db_columns=False, chunksize=10000, dtype=None)


# add the other tables
db.copy_to_rds({"instagram.posts",post_content,
                "instagram.user",user_data})
