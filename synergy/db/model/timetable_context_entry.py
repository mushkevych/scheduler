__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *

TREE_NAME = 'tree_name'
TREE_CLASSNAME = 'tree_classname'
DEPENDENT_ON = 'dependent_on'
ENCLOSED_PROCESSES = 'enclosed_processes'
MX_PAGE = 'mx_page'
MX_NAME = 'mx_name'


class TimetableContextEntry(BaseModel):
    """ Non-persistent model. Class presents single process tree (an atomic entry for the Timetable) """

    def __init__(self, document=None):
        super(TimetableContextEntry, self).__init__(document)

    @property
    def key(self):
        return self.data[TREE_NAME]

    @key.setter
    def key(self, value):
        self.data[TREE_NAME] = value

    @property
    def tree_name(self):
        return self.data[TREE_NAME]

    @tree_name.setter
    def tree_name(self, value):
        self.data[TREE_NAME] = value

    @property
    def tree_classname(self):
        return self.data[TREE_CLASSNAME]

    @tree_classname.setter
    def tree_classname(self, value):
        self.data[TREE_CLASSNAME] = value

    @property
    def dependent_on(self):
        return self.data.get(DEPENDENT_ON, [])

    @dependent_on.setter
    def dependent_on(self, value):
        self.data[DEPENDENT_ON] = value

    @property
    def enclosed_processes(self):
        return self.data[ENCLOSED_PROCESSES]

    @enclosed_processes.setter
    def enclosed_processes(self, value):
        self.data[ENCLOSED_PROCESSES] = value

    @property
    def mx_name(self):
        return self.data[MX_NAME]

    @mx_name.setter
    def mx_name(self, value):
        self.data[MX_NAME] = value

    @property
    def mx_page(self):
        return self.data[MX_PAGE]

    @mx_page.setter
    def mx_page(self, value):
        self.data[MX_PAGE] = value


def _timetable_context_entry(tree_name,
                             tree_classname,
                             enclosed_processes,
                             dependent_on=None,
                             mx_name=None,
                             mx_page=None):
    """ creates timetable context entry """
    assert enclosed_processes is not None and not isinstance(enclosed_processes, str)
    assert dependent_on is not None and not isinstance(dependent_on, str)

    timetable_entry = TimetableContextEntry()
    timetable_entry.tree_name = tree_name
    timetable_entry.tree_classname = tree_classname
    timetable_entry.enclosed_processes = enclosed_processes
    timetable_entry.dependent_on = dependent_on
    timetable_entry.mx_name = mx_name
    timetable_entry.mx_page = mx_page
    return timetable_entry
