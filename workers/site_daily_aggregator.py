__author__ = 'Bohdan Mushkevych'

from db.model.site_statistics import SiteStatistics
from db.model.base_model import BaseModel
from conf import settings
from workers.abstract_vertical_worker import AbstractVerticalWorker
from system import time_helper


class SiteDailyAggregator(AbstractVerticalWorker):
    """ class works as an aggregator from the hourly_site_collection into the daily_site_collection """

    def __init__(self, process_name):
        super(SiteDailyAggregator, self).__init__(process_name)

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
        composite_key = self._init_sink_key(source_obj.key[0], source_obj.key[1])
        target_obj = self._get_aggregated_object(composite_key)

        target_obj.number_of_visits += source_obj.number_of_visits
        target_obj.number_of_pageviews += source_obj.number_of_pageviews
        target_obj.total_duration += source_obj.total_duration
        BaseModel._copy_and_sum_families(source_obj.os, target_obj.os)
        BaseModel._copy_and_sum_families(source_obj.browsers, target_obj.browsers)
        BaseModel._copy_and_sum_families(source_obj.screen_res, target_obj.screen_res)
        BaseModel._copy_and_sum_families(source_obj.languages, target_obj.languages)
        BaseModel._copy_and_sum_families(source_obj.countries, target_obj.countries)


if __name__ == '__main__':
    from constants import PROCESS_SITE_DAILY

    source = SiteDailyAggregator(PROCESS_SITE_DAILY)
    source.start()
