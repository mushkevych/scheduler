__author__ = 'Bohdan Mushkevych'

from db.model.base_model import *

TREE_NAME = 'tree_name'
TREE_CLASSNAME = 'tree_classname'
DEPENDENT_ON = 'dependent_on'
ENCLOSED_PROCESSES = 'enclosed_processes'


class TimetableEntry(BaseModel):
    """ Class presents single process tree (an atomic entry for the Timetable) """

    def __init__(self, document=None):
        super(TimetableEntry, self).__init__(document)

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


def _timetable_entry(tree_name,
                     tree_classname,
                     enclosed_processes,
                     dependent_on=None,
                     token=None,
                     mx_page=None):
    """ forms timetable context entry """
    assert enclosed_processes is not None and not isinstance(enclosed_processes, str)
    assert dependent_on is not None and not isinstance(dependent_on, str)

    timetable_entry = TimetableEntry()
    timetable_entry.tree_name = tree_name
    timetable_entry.tree_classname = tree_classname
    timetable_entry.enclosed_processes = enclosed_processes
    timetable_entry.dependent_on = dependent_on
    return timetable_entry
