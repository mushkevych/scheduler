__author__ = 'Bohdan Mushkevych'

from db.model.site_statistics import SiteStatistics
from synergy.db.model.base_model import BaseModel
from synergy.conf import settings
from workers.abstract_vertical_worker import AbstractVerticalWorker
from synergy.system import time_helper


class SiteDailyAggregator(AbstractVerticalWorker):
    """ class works as an aggregator from the site_hourly into the site_daily """

    def __init__(self, process_name):
        super(SiteDailyAggregator, self).__init__(process_name)

    def _get_tunnel_port(self):
        return settings.settings['tunnel_site_port']

    def _init_sink_key(self, *args):
        return args[0], time_helper.hour_to_day(args[1])

    def _init_source_object(self, document):
        return SiteStatistics.from_json(document)

    def _init_sink_object(self, composite_key):
        obj = SiteStatistics()
        obj.key = (composite_key[0], composite_key[1])
        return obj

    def _process_single_document(self, document):
        source_obj = self._init_source_object(document)
        composite_key = self._init_sink_key(source_obj.key[0], source_obj.key[1])
        target_obj = self._get_aggregated_object(composite_key)

        target_obj.stat.number_of_visits += source_obj.stat.number_of_visits
        target_obj.stat.number_of_pageviews += source_obj.stat.number_of_pageviews
        target_obj.stat.total_duration += source_obj.stat.total_duration
        BaseModel._copy_and_sum_families(source_obj.stat.os, target_obj.stat.os)
        BaseModel._copy_and_sum_families(source_obj.stat.browsers, target_obj.stat.browsers)
        BaseModel._copy_and_sum_families(source_obj.stat.screen_res, target_obj.stat.screen_res)
        BaseModel._copy_and_sum_families(source_obj.stat.languages, target_obj.stat.languages)
        BaseModel._copy_and_sum_families(source_obj.stat.countries, target_obj.stat.countries)


if __name__ == '__main__':
    from constants import PROCESS_SITE_DAILY

    source = SiteDailyAggregator(PROCESS_SITE_DAILY)
    source.start()
