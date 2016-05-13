import httplib2
import datetime
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


class GoogleAnalytics(object):
    """
    Query the Google Analytics API.

    Quick start: https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/service-py#enable
    Credentials:
    """
    api = 'analytics'
    version = 'v3'

    def __init__(self, client_email, credentials_file):
        self.client_email = client_email
        self.scopes = ['https://www.googleapis.com/auth/analytics.readonly']
        self.credentials_file = credentials_file

    def add_scope(self):
        raise NotImplementedError

    def query(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=self.credentials_file,
            scopes=self.scopes
        )
        http = credentials.authorize(httplib2.Http())
        service = build(self.api, self.version, http=http)

        res = service.data().ga().get(
            ids='',
            start_date='',
            end_date='',
            dimensions='',
            metrics=''
        ).execute()

