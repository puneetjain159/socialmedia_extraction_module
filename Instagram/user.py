import json
import boto3
import requests
import urllib.request
from datetime import datetime,timedelta
import dateutil.parser as dateparser
import pytz


class UserAPI():
    def __init__(self,api):
        self.api = api
    
    def _GetDailyUserData(self,pagingUrl='',since = None):
        """ 
        Get users media
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/{ig-user-id}/fields={fields}&access_token={access-token}
        Returns:
            object: data from the endpoint
        """
        endpointParams = dict() # parameter to send to the endpoint

        if ( '' == pagingUrl ) : # get first page
           
            endpointParams['fields'] = 'username,followers_count,follows_count,ig_id,biography,name,stories,recently_searched_hashtags,website' # fields to get back
            endpointParams['access_token'] = self.api.access_token # access token
            url = self.api.endpoint_base + self.api.insta_id  # endpoint url

        else : # get specific page
            url = pagingUrl  # endpoint url

        return self.api.makeApiCall(url, endpointParams)  