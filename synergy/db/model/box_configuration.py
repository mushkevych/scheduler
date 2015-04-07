__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, IntegerField

BOX_ID = 'box_id'
PROCESS_NAME = 'process_name'
PID = 'pid'
STATE = 'state'
STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class BoxConfiguration(BaseDocument):
    """
    Class presents list of processes that are supposed to run on particular box.
    """
    db_id = ObjectIdField('_id', null=True)
    box_id = StringField(BOX_ID)
    process_name = StringField(PROCESS_NAME)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    pid = IntegerField(PID, null=True)
