__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.scheduler.tree import FourLevelTree, ThreeLevelTree, TwoLevelTree
from synergy.mx.mx_decorators import valid_action_request
from synergy.mx.rest_models import RestTreeDetail


class TreeDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.referrer = self.request.referrer
        self.is_request_valid = self.mbean is not None

    def _get_reprocessing_details(self, process_name):
        resp = []
        per_process = self.mbean.timetable.reprocess.get(process_name)
        if per_process is not None:
            resp = sorted(per_process.keys())
        return resp

    def _get_tree_details(self, tree):
        timetable = self.mbean.timetable
        details = RestTreeDetail()
        try:
            if isinstance(tree, FourLevelTree):
                details.number_of_levels = 4
                details.reprocessing_queues.yearly = self._get_reprocessing_details(tree.process_yearly)
                details.reprocessing_queues.monthly = self._get_reprocessing_details(tree.process_monthly)
                details.reprocessing_queues.daily = self._get_reprocessing_details(tree.process_daily)
                details.reprocessing_queues.hourly = self._get_reprocessing_details(tree.process_hourly)
                details.processes.yearly = tree.process_yearly
                details.processes.monthly = tree.process_monthly
                details.processes.daily = tree.process_daily
                details.processes.hourly = tree.process_hourly
                details.next_timeperiods.yearly = timetable.get_next_job_record(tree.process_yearly).timeperiod
                details.next_timeperiods.monthly = timetable.get_next_job_record(tree.process_monthly).timeperiod
                details.next_timeperiods.daily = timetable.get_next_job_record(tree.process_daily).timeperiod
                details.next_timeperiods.hourly = timetable.get_next_job_record(tree.process_hourly).timeperiod
            elif isinstance(tree, ThreeLevelTree):
                details.number_of_levels = 3
                details.reprocessing_queues.yearly = self._get_reprocessing_details(tree.process_yearly)
                details.reprocessing_queues.monthly = self._get_reprocessing_details(tree.process_monthly)
                details.reprocessing_queues.daily = self._get_reprocessing_details(tree.process_daily)
                details.processes.yearly = tree.process_yearly
                details.processes.monthly = tree.process_monthly
                details.processes.daily = tree.process_daily
                details.next_timeperiods.yearly = timetable.get_next_job_record(tree.process_yearly).timeperiod
                details.next_timeperiods.monthly = timetable.get_next_job_record(tree.process_monthly).timeperiod
                details.next_timeperiods.daily = timetable.get_next_job_record(tree.process_daily).timeperiod
            elif isinstance(tree, TwoLevelTree):
                details.number_of_levels = 1
                details.reprocessing_queues.linear = self._get_reprocessing_details(tree.process_name)
                details.processes.linear = tree.process_name
                details.next_timeperiods.linear = timetable.get_next_job_record(tree.process_name).timeperiod
            else:
                raise ValueError('Tree type %s has no support within MX module.' % tree.__class__.__name__)
        except Exception as e:
            self.logger.error('MX Exception: ' + str(e), exc_info=True)
        finally:
            return details.document

    @cached_property
    @valid_action_request
    def details(self):
        """ method iterates thru all trees and visualize only those, that has "mx_page" field set
        to current self.referrer value """
        resp = dict()
        timetable = self.mbean.timetable

        for tree_name, tree in timetable.trees.iteritems():
            if tree.mx_page in self.referrer:
                resp[tree.mx_name] = self._get_tree_details(tree)

        return resp
