__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, IntegerField, BooleanField

BOX_ID = 'box_id'
PROCESS_NAME = 'process_name'
PID = 'pid'
IS_ON = 'is_on'


class BoxConfiguration(BaseDocument):
    """ Persistent model: identifies the machine name and the process where it should be executing """
    db_id = ObjectIdField('_id', null=True)
    box_id = StringField(BOX_ID)
    process_name = StringField(PROCESS_NAME)
    is_on = BooleanField(IS_ON, default=False)
    pid = IntegerField(PID, null=True)

    @BaseDocument.key.getter
    def key(self):
        return self.box_id, self.process_name

    @key.setter
    def key(self, value):
        """ :param value: tuple holding (box_id, process_name) """
        self.box_id, self.process_name = value
