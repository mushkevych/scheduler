__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, IntegerField, BooleanField


class BoxConfiguration(BaseDocument):
    """ Persistent model: identifies the machine name and the process where it should be executing """
    db_id = ObjectIdField(name='_id', null=True)
    box_id = StringField()
    process_name = StringField()
    is_on = BooleanField(default=False)
    pid = IntegerField(null=True)

    @classmethod
    def key_fields(cls):
        return cls.box_id.name, cls.process_name.name


BOX_ID = BoxConfiguration.box_id.name
PROCESS_NAME = BoxConfiguration.process_name.name
