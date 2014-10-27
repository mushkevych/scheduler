__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.conf.process_context import ProcessContext
from synergy.scheduler.tree import FourLevelTree, ThreeLevelTree, TwoLevelTree
from synergy.mx.mx_decorators import valid_action_request


class TimeperiodDetails(object):
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

    def _get_nodes_details(self, tree):
        timetable = self.mbean.timetable
        description = dict()
        description['reprocessing_queues'] = dict()
        description['processes'] = dict()
        description['next_timeperiods'] = dict()
        try:
            if isinstance(tree, FourLevelTree):
                description['number_of_levels'] = 4
                description['reprocessing_queues']['yearly'] = self._get_reprocessing_details(tree.process_yearly)
                description['reprocessing_queues']['monthly'] = self._get_reprocessing_details(tree.process_monthly)
                description['reprocessing_queues']['daily'] = self._get_reprocessing_details(tree.process_daily)
                description['reprocessing_queues']['hourly'] = self._get_reprocessing_details(tree.process_hourly)
                description['processes']['yearly'] = tree.process_yearly
                description['processes']['monthly'] = tree.process_monthly
                description['processes']['daily'] = tree.process_daily
                description['processes']['hourly'] = tree.process_hourly
                description['next_timeperiods']['yearly'] = timetable.get_next_job_record(tree.process_yearly).timeperiod
                description['next_timeperiods']['monthly'] = timetable.get_next_job_record(tree.process_monthly).timeperiod
                description['next_timeperiods']['daily'] = timetable.get_next_job_record(tree.process_daily).timeperiod
                description['next_timeperiods']['hourly'] = timetable.get_next_job_record(tree.process_hourly).timeperiod
                description['type'] = ProcessContext.get_process_type(tree.process_yearly)
            elif isinstance(tree, ThreeLevelTree):
                description['number_of_levels'] = 3
                description['reprocessing_queues']['yearly'] = self._get_reprocessing_details(tree.process_yearly)
                description['reprocessing_queues']['monthly'] = self._get_reprocessing_details(tree.process_monthly)
                description['reprocessing_queues']['daily'] = self._get_reprocessing_details(tree.process_daily)
                description['processes']['yearly'] = tree.process_yearly
                description['processes']['monthly'] = tree.process_monthly
                description['processes']['daily'] = tree.process_daily
                description['next_timeperiods']['yearly'] = timetable.get_next_job_record(tree.process_yearly).timeperiod
                description['next_timeperiods']['monthly'] = timetable.get_next_job_record(tree.process_monthly).timeperiod
                description['next_timeperiods']['daily'] = timetable.get_next_job_record(tree.process_daily).timeperiod
                description['type'] = ProcessContext.get_process_type(tree.process_yearly)
            elif isinstance(tree, TwoLevelTree):
                description['number_of_levels'] = 1
                description['reprocessing_queues']['linear'] = self._get_reprocessing_details(tree.process_name)
                description['processes']['linear'] = tree.process_name
                description['next_timeperiods']['daily'] = timetable.get_next_job_record(tree.process_name).timeperiod
                description['type'] = ProcessContext.get_process_type(tree.process_name)
            else:
                raise ValueError('Tree type %s has no support within MX module.' % tree.__class__.__name__)
        except Exception as e:
            self.logger.error('MX Exception: ' + str(e), exc_info=True)
        finally:
            return description

    @cached_property
    @valid_action_request
    def details(self):
        """ method iterates thru all trees and visualize only those, that has "mx_page" field set
        to current self.referrer value """
        resp = dict()
        timetable = self.mbean.timetable

        for tree_name, tree in timetable.trees.iteritems():
            if tree.mx_page in self.referrer:
                resp[tree.mx_name] = self._get_nodes_details(tree)

        return resp
