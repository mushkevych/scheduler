__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.conf import settings
from synergy.system import time_helper
from synergy.conf import context
from synergy.mx.rest_model import RestTimetableTreeNode, RestJob
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request


class TreeNodeDetails(BaseRequestHandler):
    def __init__(self, request, **values):
        super(TreeNodeDetails, self).__init__(request, **values)
        self.process_name = request.args.get('process_name')
        self.timeperiod = request.args.get('timeperiod')
        self.tree = self.scheduler.timetable.get_tree(self.process_name)
        self.is_request_valid = True if self.tree else False

    @classmethod
    def get_details(cls, node, as_model=False):
        """method returns either RestJob instance or corresponding document, depending on the as_model argument """
        rest_job = RestJob(
            process_name=node.process_name,
            timeperiod=node.timeperiod,
            time_qualifier=node.time_qualifier,
            number_of_children=len(node.children),
            number_of_failures='NA' if not node.job_record else node.job_record.number_of_failures,
            state='NA' if not node.job_record else node.job_record.state,
            event_log=[] if not node.job_record else node.job_record.event_log)

        if as_model:
            return rest_job
        else:
            return rest_job.document

    @cached_property
    @valid_action_request
    def details(self):
        rest_node = RestTimetableTreeNode()

        if not self.timeperiod:
            # return list of yearly nodes OR leafs for linear tree
            # limit number of children to return, since a linear tree can holds thousands of nodes
            children_keys = list(self.tree.root.children)
            sorted_keys = sorted(children_keys, reverse=True)
            sorted_keys = sorted_keys[:settings.settings['mx_children_limit']]
            for key in sorted_keys:
                child = self.tree.root.children[key]
                rest_node.children[key] = TreeNodeDetails.get_details(child)

        else:
            time_qualifier = context.process_context[self.process_name].time_qualifier
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = self.tree.get_node(self.process_name, self.timeperiod)
            rest_node.node = TreeNodeDetails.get_details(node, as_model=True)
            for key, child in node.children.items():
                rest_node.children[key] = TreeNodeDetails.get_details(child)

        return rest_node.document
