__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import ObjectIdField, DictField, ListField, DateTimeField


class LogRecording(BaseDocument):
    """ Persistent model: represents log/processing details from some activity,
        such as the Unit Of Work execution """
    db_id = ObjectIdField(name='_id', null=True)
    parent_object_id = ObjectIdField()
    log = ListField()
    details = DictField()
    created_at = DateTimeField()  # Field used by TTL mechanism

    @classmethod
    def key_fields(cls):
        return cls.parent_object_id.name


PARENT_OBJECT_ID = LogRecording.parent_object_id.name
LOG = LogRecording.log.name
CREATED_AT = LogRecording.created_at.name
