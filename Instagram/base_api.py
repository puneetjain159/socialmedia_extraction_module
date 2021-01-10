import requests
import json
import ast
import time



class InstagramAPI():
    def __init__(self,
                 access_token = None,
                 username = None,
                 graph_domain = 'https://graph.facebook.com/',
                 graph_version = 'v9.0',
                 bucket = 'phokusinstagram',
                 debug = 'no'):
        self.access_token = access_token
        self.username = username
        self.graph_domain = graph_domain
        self.graph_version = graph_version
        self.bucket = bucket
        self.debug = debug
        self.endpoint_base = self.graph_domain + self.graph_version + '/'
        self.insta_id = None
    

    def makeApiCall(self , url, endpointParams):
        
        data = requests.get( url, endpointParams ) # make get request

        # Retry once and then leave as is
        if "error" in json.loads(data.content):
            print("ERROR : Sleeping for 30 seconds and retrying")
            time.sleep(30)
            data = requests.get( url, endpointParams )

        # Raise an Error
        if "error" in json.loads(data.content):
            raise f'{json.loads(data.content)}'

        response = dict() # hold response info
        response['url'] = url # url we are hitting
        response['endpoint_params'] = endpointParams #parameters for the endpoint

        response['json_data'] = json.loads( data.content ) # response data from the api


        if ( 'yes' == self.debug ) : # display out response info
            response['endpoint_params_pretty'] = json.dumps( endpointParams, indent = 4 ) # pretty print for cli
            response['json_data_pretty'] = json.dumps( response['json_data'], indent = 4 ) # pretty print for cli
            self.displayApiCallData( response ) # display response

        return response # get and return content


    def displayApiCallData(self, response ) :
        """ Print out to cli response from api call """

        print("\nURL: ") # title
        print(response['url']) # display url hit
        print("\nEndpoint Params: " )# title
        print(response['endpoint_params_pretty']) # display params passed to the endpoint
        print("\nResponse: ") # title
        print(response['json_data_pretty']) # make look pretty for cli
    

    def getUserPages( self ) :
        """ Get facebook pages for a user
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/me/accounts?access_token={access-token}
        Returns:
            object: data from the endpoint
        """

        endpointParams = dict() # parameter to send to the endpoint
        endpointParams['access_token'] = self.access_token # access token

        url = self.endpoint_base + 'me/accounts' # endpoint url

        return self.makeApiCall( url, endpointParams ) # make the api call
    
    

    def getInstagramAccount( self,user_id ) :
        """ Get instagram account
        
        API Endpoint:
            https://graph.facebook.com/{graph-api-version}/{page-id}?access_token={your-access-token}&fields=instagram_business_account
        Returns:
            object: data from the endpoint
        """

        endpointParams = dict() # parameter to send to the endpoint
        endpointParams['access_token'] = self.access_token # tell facebook we want to exchange token
        endpointParams['fields'] = 'instagram_business_account' # access token

        url = self.endpoint_base + user_id # endpoint url

        return self.makeApiCall( url, endpointParams )  # make the api call
    
    def GetUserInstaid(self):
        users = self.getUserPages()
        for user in users['json_data']['data']:
            if user['name'] == self.username:
                user_fb_id = user['id']
                insta_id = self.getInstagramAccount(user_fb_id)
                self.insta_id = insta_id['json_data']['instagram_business_account']['id']
        if not self.insta_id:
            raise "Could not get the instagram ID"
    

    def _check_paginator(self,data):
        ''' Iterator to check pagination '''
        itr = False
        if 'paging' in data['json_data']:
            if 'next' in data['json_data']['paging']:
                itr = True
        return itr
    
    def _check_comment_paginator(self,data):
        ''' Iterator to check pagination '''
        itr = False
        if 'paging' in data:
            if 'next' in data['paging']:
                itr = True
        return itr






