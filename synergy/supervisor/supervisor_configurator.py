__author__ = 'Bohdan Mushkevych'

import re

from synergy.conf import context
from synergy.supervisor.supervisor_constants import PROCESS_SUPERVISOR
from synergy.system.data_logging import get_logger


class SupervisorEntry(object):
    def __init__(self, process_name):
        if process_name not in context.process_context:
            raise ValueError('SupervisorEntry: process {0} is not found in process_context. Aborting'
                             .format(process_name))

        self.logger = get_logger(PROCESS_SUPERVISOR)
        self.process_name = process_name
        self.re_boxes = context.process_context[process_name].present_on_boxes
        self.re_co_boxes = []  # compiled RE of boxes where the process should be present

        for re_box in self.re_boxes:
            try:
                if isinstance(re_box, str):
                    re_box = re_box.lower()
                    self.re_co_boxes.append(re.compile(re_box))
                elif isinstance(re_box, int):
                    self.re_co_boxes.append(re_box)
                else:
                    raise ValueError('SupervisorEntry support (string, integer) values. Type {0} unsupported'
                                     .format(type(re_box)))
            except TypeError:
                self.logger.warn('SupervisorEntry compilation error for {0}'.format(re_box))

    def is_present(self, box_id):
        is_present = False
        for co_re_box in self.re_co_boxes:
            if isinstance(co_re_box, int):
                is_present = co_re_box == box_id

            else:
                box_id = box_id.lower()
                if co_re_box.search(box_id):
                    is_present = True

            if is_present:
                break

        return is_present


class SupervisorConfigurator(object):
    def __init__(self):
        super(SupervisorConfigurator, self).__init__()
        self.process_map = dict()
        for process_name in context.process_context:
            self.process_map[process_name] = SupervisorEntry(process_name)

    def is_present(self, box_id):
        pass

    def init_db(self, box_id):
        pass
