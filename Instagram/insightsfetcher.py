import json
import boto3
import requests
import urllib.request
from datetime import datetime,timedelta
import dateutil.parser as dateparser
import pytz



class InsightsAPI():
    def __init__(self,api):
        self.api = api
    
    def _GetDailyInsights(self,pagingUrl='',since = None):
        """ 
        Get users media
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/{ig-user-id}/fields={fields}&access_token={access-token}
        Returns:
            object: data from the endpoint
        """
        endpointParams = dict() # parameter to send to the endpoint

        if ( '' == pagingUrl ) : # get first page
           
            endpointParams['fields'] = 'insights.metric(impressions, reach, email_contacts, phone_call_clicks, text_message_clicks, get_directions_clicks, website_clicks, profile_views).period(day)' # fields to get back
            endpointParams['access_token'] = self.api.access_token # access token
            url = self.api.endpoint_base + self.api.insta_id  # endpoint url

        else : # get specific page
            url = pagingUrl  # endpoint url

        return self.api.makeApiCall(url, endpointParams) 
    

    def _GetLifeTimeInsights(self,pagingUrl='',since = None):
        """ 
        Get users media
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/{ig-user-id}/fields={fields}&access_token={access-token}
        Returns:
            object: data from the endpoint
        """
        endpointParams = dict() # parameter to send to the endpoint

        if ( '' == pagingUrl ) : # get first page
           
            endpointParams['fields'] = 'insights.metric(audience_gender_age, audience_locale, audience_country, audience_city, online_followers).period(lifetime)' # fields to get back
            endpointParams['access_token'] = self.api.access_token # access token
            url = self.api.endpoint_base + self.api.insta_id  # endpoint url

        else : # get specific page
            url = pagingUrl  # endpoint url

        return self.api.makeApiCall(url, endpointParams) 