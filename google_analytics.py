__doc__ = """ """


class GoogleAnalytics(object):

    def __init__(self, service):
        self.service = service

    def __repr__(self):
        pass

    def query(self, id, start_date, end_date, dimensions, metrics, **kwargs):
        pass