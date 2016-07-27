__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import ObjectIdField, DictField, ListField, DateTimeField

PARENT_OBJECT_ID = 'parent_object_id'
LOG = 'log'
DETAILS = 'details'
CREATED_AT = 'created_at'


class LogRecording(BaseDocument):
    """ Persistent model: represents log/processing details from some activity,
        such as the Unit Of Work execution """
    db_id = ObjectIdField('_id', null=True)
    parent_object_id = ObjectIdField(PARENT_OBJECT_ID)
    log = ListField(LOG)
    details = DictField(DETAILS)
    created_at = DateTimeField(CREATED_AT)  # Field used by TTL mechanism

    @BaseDocument.key.getter
    def key(self):
        return self.parent_object_id

    @key.setter
    def key(self, value):
        self.parent_object_id = value
