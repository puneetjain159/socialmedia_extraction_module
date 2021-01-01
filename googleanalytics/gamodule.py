#%%
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import boto3
import json
import sys




class googleApi():
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

    def __init__(self,
                 VIEW_ID = '228725055',
                 KEY_FILE_LOCATION = 'credentials.json'):
        self.VIEW_ID = VIEW_ID
        self.KEY_FILE_LOCATION = KEY_FILE_LOCATION
        self.analytics = self.initialize_analyticsreporting()


    def initialize_analyticsreporting(self):
        """Initializes an Analytics Reporting API V4 service object.

        Returns:
        An authorized Analytics Reporting API V4 service object.
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.KEY_FILE_LOCATION, self.SCOPES)

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics

    def get_report(self,requestbody):
        """Queries the Analytics Reporting API V4.

        Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        Returns:
        The Analytics Reporting API V4 response.
        """
        return self.analytics.reports().batchGet(
            body={
            'reportRequests': [requestbody]
            }
        ).execute()


    def create_request_json(self,data):
        requestbody = {
                'viewId': self.VIEW_ID,
                'dateRanges': [{'startDate': '1daysAgo', 'endDate': 'yesterday'}],
                'metrics': [],
                'dimensions': []
            }
        ## update date and time
        requestbody['dateRanges'][0]['startDate'] = data['startDate']
        requestbody['dateRanges'][0]['endDate'] = data['endDate']
        ## update the metrics
        for metric in data['metrics']:
            requestbody['metrics'].append({"expression" : metric})
        ## update the Expression
        for dimension in data['dimensions']:
            requestbody['dimensions'].append({"name" : dimension})
        return requestbody
    

    def convert_json_pandas(self , response):
        list = []
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])
            if rows == []:
                columns = []
                columns.extend(dimensionHeaders)
                for metric in metricHeaders:
                    columns.append(metric['name'])
                df = pd.DataFrame(columns= columns)
                return df

            for row in rows:
                dict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])
                for header, dimension in zip(dimensionHeaders, dimensions):
                    dict[header] = dimension
                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metricHeaders, values.get('values')):
                        if ',' in value or '.' in value:
                            dict[metric.get('name')] = float(value)
                        else:
                            dict[metric.get('name')] = int(value)
                list.append(dict)
        df = pd.DataFrame(list)
        return df



if __name__ == "__main__":
    ## Ingest the current flow file ##
    data =     {
        "name_view": "gender",
        "startDate": "1daysago",
        "endDate": "yesterday",
        "metrics": [
            "ga:sessions",
            "ga:sessionDuration",
            "ga:transactionsPerSession",
            "ga:bounceRate",
            "ga:newUsers",
            "ga:users"
        ],
        "dimensions": [
            "ga:date",
            "ga:userGender"

        ]
    }
    ga = googleApi(KEY_FILE_LOCATION = 'googleanalytics/credentials.json')
    requestbody = ga.create_request_json(data)
    ga.convert_json_pandas(ga.get_report(requestbody))
    response = ga.get_report(requestbody)



    

# %%

# %%
