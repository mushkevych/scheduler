__author__ = 'Bohdan Mushkevych'

from db.model.base_model import *

TREE_NAME = 'tree_name'
TREE_CLASSNAME = 'tree_classname'
DEPENDENT_ON = 'dependent_on'
ENCLOSED_PROCESSES = 'enclosed_processes'


class TimetableEntry(BaseModel):
    """ Class presents single process tree (an atomic entry for the TimeTable) """

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
        return self.data[DEPENDENT_ON]

    @dependent_on.setter
    def dependent_on(self, value):
        self.data[DEPENDENT_ON] = value

    @property
    def enclosed_processes(self):
        return self.data[ENCLOSED_PROCESSES]

    @enclosed_processes.setter
    def enclosed_processes(self, value):
        self.data[ENCLOSED_PROCESSES] = value
