__author__ = 'Bohdan Mushkevych'

from constants import COLLECTION_SITE_DAILY
from db.model.raw_data import DOMAIN_NAME, TIMEPERIOD
from db.model.site_statistics import SiteStatistics
from db.model.alert import Alert
from db.dao.site_dao import SiteDao
from workers.abstract_mongo_worker import AbstractMongoWorker
from synergy.system import time_helper
from synergy.system.time_qualifier import *


class AlertDailyWorker(AbstractMongoWorker):
    """ illustration suite worker:
        - compares site daily statistics with 1-week old one
          and reports an alert should any of the given threshold be crossed """

    def __init__(self, process_name):
        super(AlertDailyWorker, self).__init__(process_name)
        self.site_dao = SiteDao(self.logger)

    def _init_sink_key(self, *args):
        return args[0], time_helper.hour_to_day(args[1])

    def _mongo_sink_key(self, *args):
        return {DOMAIN_NAME: args[0], TIMEPERIOD: args[1]}

    def _init_source_object(self, document):
        return SiteStatistics.from_json(document)

    def _init_sink_object(self, composite_key):
        obj = Alert()
        obj.key = (composite_key[0], composite_key[1])
        return obj

    def _process_single_document(self, document):
        source_obj = self._init_source_object(document)
        week_old_timeperiod = time_helper.increment_timeperiod(QUALIFIER_DAILY, source_obj.key[1], delta=-7)
        try:
            week_old_obj = self.site_dao.get_one(COLLECTION_SITE_DAILY, source_obj.key[0], week_old_timeperiod)

            visits_threshold_crossed = source_obj.number_of_visits / week_old_obj.number_of_visits < 0.8 \
                                       or source_obj.number_of_visits / week_old_obj.number_of_visits > 1.2

            pageviews_threshold_crossed = source_obj.number_of_pageviews / week_old_obj.number_of_pageviews < 0.8 \
                                          or source_obj.number_of_pageviews / week_old_obj.number_of_pageviews > 1.2

            if visits_threshold_crossed or pageviews_threshold_crossed:
                composite_key = self._init_sink_key(source_obj.key[0], source_obj.key[1])
                target_obj = self._get_aggregated_object(composite_key)
                target_obj.number_of_visits = source_obj.number_of_visits - week_old_obj.number_of_visits
                target_obj.number_of_pageviews = source_obj.number_of_pageviews - week_old_obj.number_of_pageviews
        except LookupError:
            self.logger.debug('site statistics ({0}:{1}) was not found. skipping comparison'
                              .format(source_obj.key[0], week_old_timeperiod))


if __name__ == '__main__':
    from constants import PROCESS_ALERT_DAILY

    source = AlertDailyWorker(PROCESS_ALERT_DAILY)
    source.start()
