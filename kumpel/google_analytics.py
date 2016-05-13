import httplib2
import datetime
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from kumpel.helpers import date_split


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

    def query(self, **kwargs):
        start_index = kwargs.pop('start_index', 1)
        batch = kwargs.pop('batch', 10000)
        ids = kwargs.pop('ids')
        start_date = kwargs.pop('start_date')
        end_date = kwargs.pop('end_date')
        dimensions = kwargs.pop('dimensions')
        metrics = kwargs.pop('metrics')

        if not isinstance(start_date, datetime.date):
            try:
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                pass

        if not isinstance(end_date, datetime.date):
            try:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                pass

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=self.credentials_file,
            scopes=self.scopes
        )
        http = credentials.authorize(httplib2.Http())
        service = build(self.api, self.version, http=http)

        for start, end in date_split(start_date, end_date):
            all_rows_are_fetched = False
            while not all_rows_are_fetched:
                res = service.data().ga().get(
                    ids=ids,
                    start_date=start.strformat('%Y-%m-%d'),
                    end_date=end,
                    dimensions=dimensions,
                    metrics=metrics,
                    start_index=start_index,
                    max_results=batch
                ).execute()

                if res.get('rows'):
                    print(len(res.get('rows')), 'rows fetched.')

                    for row in res.get('rows'):
                        yield row

                    start_index += batch

                    if start_index >= res['totalResults']:
                        all_rows_are_fetched = True
                        print('Done')
                else:
                    print('Query returns no rows for this time frame.')
                    all_rows_are_fetched = True
