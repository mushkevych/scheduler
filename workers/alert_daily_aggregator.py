__author__ = 'Bohdan Mushkevych'

from db.model.site_statistics import SiteStatistics
from synergy.conf import settings
from workers.abstract_vertical_worker import AbstractVerticalWorker
from synergy.system import time_helper


class AlertDailyAggregator(AbstractVerticalWorker):
    """ class compares site daily statistics with 1-week old one and
    reports an alert should any of the given threshold be crossed """

    def __init__(self, process_name):
        super(AlertDailyAggregator, self).__init__(process_name)

    def _get_tunnel_port(self):
        return settings.settings['tunnel_site_port']

    def _init_sink_key(self, *args):
        return args[0], time_helper.hour_to_day(args[1])

    def _init_source_object(self, document):
        return SiteStatistics(document)

    def _init_sink_object(self, composite_key):
        obj = SiteStatistics()
        obj.key = (composite_key[0], composite_key[1])
        return obj

    def _process_single_document(self, document):
        source_obj = self._init_source_object(document)
        # subtract 7 days from the document's timeperiod
        # fetch the document to compare and perform comparison

        if views_trigger or pageviews_trigger:
            composite_key = self._init_sink_key(source_obj.key[0], source_obj.key[1])
            target_obj = self._get_aggregated_object(composite_key)
            target_obj.number_of_visits = source_obj.number_of_visits - old_source_obj.number_of_visits
            target_obj.number_of_pageviews = source_obj.number_of_pageviews - old_source_obj.number_of_pageviews


if __name__ == '__main__':
    from constants import PROCESS_ALERT_DAILY

    source = AlertDailyAggregator(PROCESS_ALERT_DAILY)
    source.start()
