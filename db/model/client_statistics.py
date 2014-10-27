__author__ = 'Bohdan Mushkevych'

from db.model.site_statistics import *

CLIENT_ID = 'client_id'


class ClientStatistics(SiteStatistics):
    """
    class presents statistics for site owners: number of visits per defined period or list of search keywords
    """

    def __init__(self, document=None):
        super(ClientStatistics, self).__init__(document)

    @property
    def key(self):
        return self.data[CLIENT_ID], self.data[TIMEPERIOD]

    @key.setter
    def key(self, value):
        """
        :param value: tuple (client_id <string>, timeperiod <string in YYYYMMDDHH format>)
        """
        self.data[CLIENT_ID] = value[0]
        self.data[TIMEPERIOD] = value[1]
