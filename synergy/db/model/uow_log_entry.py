__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, DictField, ListField, DateTimeField

RELATED_UNIT_OF_WORK = 'related_unit_of_work'
LOG = 'log'
DETAILS = 'details'
CREATED_AT = 'created_at'


class UowLogEntry(BaseDocument):
    """ Persistent model: represents log/processing details from the Unit Of Work execution """
    db_id = ObjectIdField('_id', null=True)
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)
    log = ListField(LOG)
    details = DictField(DETAILS)
    created_at = DateTimeField(CREATED_AT)  # Field used by TTL mechanism

    @BaseDocument.key.getter
    def key(self):
        return self.related_unit_of_work

    @key.setter
    def key(self, value):
        self.related_unit_of_work = value
