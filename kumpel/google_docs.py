import gspread
from oauth2client.service_account import ServiceAccountCredentials


class GoogleDocs(object):
    """ """
    scopes = ['https://spreadsheets.google.com/feeds']

    def __init__(self, credentials_file):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=credentials_file,
            scopes=self.scopes
        )
        self.service = gspread.authorize(credentials)

    def worksheets(self, workbook):
        doc = self.service.open(title=workbook)
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
        doc = self.service.open(title=workbook)
        sheet = doc.worksheet(title=worksheet)
        for record in sheet.get_all_records(empty2zero=empty2zero, head=head):
            yield record
