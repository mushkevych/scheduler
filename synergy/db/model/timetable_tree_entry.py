__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ListField


class TimetableTreeEntry(BaseDocument):
    """ Non-persistent model. Class presents single process tree (an atomic entry for the Timetable) """

    tree_name = StringField()
    dependent_on = ListField()
    enclosed_processes = ListField()
    mx_name = StringField()
    mx_page = StringField()

    @classmethod
    def key_fields(cls):
        return cls.tree_name.name


def timetable_tree_entry(tree_name,
                         enclosed_processes,
                         dependent_on=None,
                         mx_name=None,
                         mx_page=None):
    """ creates timetable context entry """
    assert enclosed_processes is not None and not isinstance(enclosed_processes, str)
    assert dependent_on is not None and not isinstance(dependent_on, str)

    timetable_entry = TimetableTreeEntry(tree_name=tree_name,
                                         enclosed_processes=enclosed_processes,
                                         dependent_on=dependent_on,
                                         mx_name=mx_name,
                                         mx_page=mx_page)
    return timetable_entry
