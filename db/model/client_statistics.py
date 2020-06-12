__author__ = 'Bohdan Mushkevych'

from db.model.site_statistics import SiteStatistics
from odm.fields import StringField


class ClientStatistics(SiteStatistics):
    """
    class presents statistics for site owners: number of visits per defined period or list of search keywords
    """

    client_id = StringField()

    @classmethod
    def key_fields(cls):
        return cls.client_id.name, cls.timeperiod.name


CLIENT_ID = ClientStatistics.client_id.name
