import json
import boto3
import requests
import urllib.request
from datetime import datetime,timedelta
import dateutil.parser as dateparser
import pytz


from Instagram.mediafetcher import media


class StoriesMedia(media):
    def __init__(self, api):
        super().__init__(api=api)

    
    def _getUserMedia(self,pagingUrl='',since = None):
        """ 
        Get users media
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/{ig-user-id}/stories?fields={fields}&access_token={access-token}
        Returns:
            object: data from the endpoint
        """
        endpointParams = dict() # parameter to send to the endpoint

        if ( '' == pagingUrl ) : # get first page
           
            endpointParams['fields'] = 'caption,comments_count,id,like_count,media_type,media_url,timestamp,username,children,insights.metric(impressions, reach)' # fields to get back
            endpointParams['access_token'] = self.api.access_token # access token
            url = self.api.endpoint_base + self.api.insta_id + '/stories' # endpoint url

        else : # get specific page
            url = pagingUrl  # endpoint url

        return self.api.makeApiCall(url, endpointParams) 