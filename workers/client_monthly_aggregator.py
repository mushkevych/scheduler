__author__ = 'Bohdan Mushkevych'

from db.model.client_statistics import ClientStatistics
from synergy.system.utils import copy_and_sum_families
from synergy.system import time_helper
from workers.client_daily_aggregator import ClientDailyAggregator


class ClientMonthlyAggregator(ClientDailyAggregator):
    """ illustration suite worker:
        - an aggregator from the client_daily into the client_monthly """

    def __init__(self, process_name):
        super(ClientMonthlyAggregator, self).__init__(process_name)

    def _init_sink_key(self, *args):
        return args[0], time_helper.day_to_month(args[1])

    def _init_source_object(self, document):
        return ClientStatistics.from_json(document)

    def _process_single_document(self, document):
        source_obj = self._init_source_object(document)
        composite_key = self._init_sink_key(source_obj.key[0], source_obj.key[1])
        target_obj = self._get_aggregated_object(composite_key)

        target_obj.number_of_visits += source_obj.number_of_visits
        target_obj.number_of_pageviews += source_obj.number_of_pageviews
        target_obj.total_duration += source_obj.total_duration
        copy_and_sum_families(source_obj.os, target_obj.os)
        copy_and_sum_families(source_obj.browsers, target_obj.browsers)
        copy_and_sum_families(source_obj.screen_resolution, target_obj.screen_resolution)
        copy_and_sum_families(source_obj.languages, target_obj.languages)
        copy_and_sum_families(source_obj.countries, target_obj.countries)


if __name__ == '__main__':
    from constants import PROCESS_CLIENT_MONTHLY

    source = ClientMonthlyAggregator(PROCESS_CLIENT_MONTHLY)
    source.start()
