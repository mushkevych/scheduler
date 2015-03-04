__author__ = 'Bohdan Mushkevych'

from db.model.site_statistics import SiteStatistics
from odm.fields import StringField

CLIENT_ID = 'client_id'


class ClientStatistics(SiteStatistics):
    """
    class presents statistics for site owners: number of visits per defined period or list of search keywords
    """

    client_id = StringField(CLIENT_ID)

    @SiteStatistics.key.getter
    def key(self):
        return self.client_id, self.timeperiod

    @SiteStatistics.key.setter
    def key(self, value):
        """
        :param value: tuple (client_id <string>, timeperiod <string in YYYYMMDDHH format>)
        """
        self.client_id = value[0]
        self.timeperiod = value[1]
