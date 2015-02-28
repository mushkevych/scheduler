__author__ = 'Bohdan Mushkevych'

from synergy.conf import context
from synergy.db.model.queue_context_entry import QueueContextEntry


class MqQueueContext(object):
    def __init__(self):
        super(MqQueueContext, self).__init__()

    @classmethod
    def put(cls, context_entry):
        assert isinstance(context_entry, QueueContextEntry)
        context.mq_queue_context[context_entry.mq_queue] = context_entry

    @classmethod
    def get(cls, process_name=None):
        """
        :return instance of the QueueContextEntry associated with the process_name
        :raise KeyError if no entry is associated with given process_name
        """
        return context.mq_queue_context[process_name]
