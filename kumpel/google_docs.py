import gspread
from oauth2client.service_account import ServiceAccountCredentials


class GoogleDocs(object):
    """ """
    def __init__(self, credentials_file):
        self.scopes = ['https://spreadsheets.google.com/feeds']
        self.credentials_file = credentials_file

    def add_scope(self):
        """

        :return:
        """
        raise NotImplementedError

    def worksheets(self, workbook):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=self.credentials_file,
            scopes=self.scopes
        )
        drive = gspread.authorize(credentials)
        doc = drive.open(title=workbook)
        for sheet in doc.worksheets():
            yield str(sheet).split("'")[1]

    def get_records(self, workbook, worksheet, empty2zero=False, head=1):
        """

        :param workbook:
        :param worksheet:
        :param empty2zero:
        :param head:
        :return:
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=self.credentials_file,
            scopes=self.scopes
        )
        drive = gspread.authorize(credentials)
        doc = drive.open(title=workbook)
        sheet = doc.worksheet(title=worksheet)
        for record in sheet.get_all_records(empty2zero=empty2zero, head=head):
            yield record
