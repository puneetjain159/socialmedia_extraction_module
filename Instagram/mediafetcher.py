import json
import boto3
import requests
import urllib.request
from datetime import datetime,timedelta
import dateutil.parser as dateparser
import pytz

class media():
    '''
    Class to fetch all the media content currently stored in the media node
    '''
    def __init__(self,
                 api = None):
        self.api = api
        self.s3 = boto3.resource('s3',region_name = 'eu-west-1')

    
    def mediafetcher(self,limit =10000,since = None):
        '''
        API Endpoint:
        https://graph.facebook.com/{graph-api-version}/{page-id}?access_token={your-access-token}&fields=instagram_business_account
        '''


        data = self._getUserMedia(pagingUrl='')
        index,timestat = self.calc_timedifference(since,data['json_data']['data'])
        if timestat:
            data['json_data']['data'] = data['json_data']['data'][:index+1]
            return data
        itr = self.api._check_paginator(data)
        next = data.copy()
        [self.DownloadMedia(_d) for _d in next['json_data']['data']]

        while itr:
            next = self._getUserMedia(pagingUrl=next['json_data']['paging']['next'])
            index,timestat = self.calc_timedifference(since,next['json_data']['data'])
            if timestat:
                print("debug :")
                next['json_data']['data'] = next['json_data']['data'][:index+1]
                data['json_data']['data'].extend(next['json_data']['data'])
                break
            itr = self.api._check_paginator(next)
            data['json_data']['data'].extend(next['json_data']['data'])
            [self.DownloadMedia(_d) for _d in next['json_data']['data']]
            print("Fetching Data :",len(data['json_data']['data']))
            if len(data['json_data']['data']) >= limit:
                break
        
        return data


    def _getUserMedia(self,pagingUrl='',since = None):
        """ 
        Get users media
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/{ig-user-id}/media?fields={fields}&access_token={access-token}
        Returns:
            object: data from the endpoint
        """
        endpointParams = dict() # parameter to send to the endpoint

        if ( '' == pagingUrl ) : # get first page
           
            endpointParams['fields'] = 'id,caption,media_type,media_url,comments_count,timestamp,is_comment_enabled,like_count,comments{text,like_count},children{media_url,media_type},insights.metric(engagement,impressions,reach,saved){description,name,period,title,id,values}' # fields to get back
            endpointParams['access_token'] = self.api.access_token # access token
            url = self.api.endpoint_base + self.api.insta_id + '/media' # endpoint url

        else : # get specific page
            url = pagingUrl  # endpoint url

        return self.api.makeApiCall(url, endpointParams) 
    

    def DownloadMedia(self,media_data):
        ''' Function to download the media to s3'''
        req_for_image = requests.get(media_data['media_url'], stream=True)
        req_for_image.raw.decode_content = True
        file_object_from_req = req_for_image.raw
        req_data = file_object_from_req.read()
        if media_data['media_type'] == 'VIDEO':
            name = "video.mp4"
        elif media_data['media_type'] == 'IMAGE':
            name = "img.png"
        elif media_data['media_type'] == 'CAROUSEL_ALBUM':
            for index, _m in enumerate(media_data['children']['data']):
                req_for_image = requests.get(media_data['media_url'], stream=True)
                req_for_image.raw.decode_content = True
                file_object_from_req = req_for_image.raw
                req_data = file_object_from_req.read()
                if _m['media_type'] == 'VIDEO':
                    name = "video_"+ str(index) + ".mp4"
                elif _m['media_type'] == 'IMAGE':
                    name = "img_"+ str(index) + ".png"
                self.s3.Bucket(self.api.bucket).put_object(Key=self.api.username + "/" + media_data['id']+ "/" +name,
                                                     Body=req_data)

            return "Null"
        self.s3.Bucket(self.api.bucket).put_object(Key=self.api.username + "/" + media_data['id']+ "/" +name,
                                                     Body=req_data)
        return "Null"
    

    def calc_timedifference(self,timecompare,data):
        ''' check the time difference '''
        if timecompare is None:
            return 0,False
        result = False
        for index,d_ in enumerate(data):
            dt = dateparser.parse(d_['timestamp'])
            if dt <= timecompare:
                result = True
                break    
        return index,result
    

    def _add_insta_id_to_comments(self,data,data_id):
        ''' process the comments and create a separate json for the same list'''
        if "comments" in data.keys():
            for comment in data['comments']['data']:
                comment['insta_post_id'] = data_id
            comments = data['comments']['data']
            del data['comments']
            return comments
        else:
            return []


    def process_comments(self,data):
        ''' collating the comments '''

        data_id = data['id']
        comments = self._add_insta_id_to_comments(data,data_id)
        itr = self.api._check_comment_paginator(data)

        while itr:
            next = self._getUserMedia(pagingUrl=data['comments']['paging']['next'])
            comments.extend(self._add_insta_id_to_comments(next['json_data']['data'],data_id))
            itr = self.api._check_comment_paginator(next)
        
        return comments
    
    def process_insights(self,data):
        ''' collating and correcting the insights '''
        for index, post in enumerate(data['json_data']['data']):
            for insight in post['insights']['data']:
                data['json_data']['data'][index][insight['name']] = insight['values'][0]['value']
            del post['insights']
        return data








