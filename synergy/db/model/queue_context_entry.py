__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField


class QueueContextEntry(BaseDocument):
    """ Non-persistent model. This class presents Queue Context Entry record """

    mq_queue = StringField()
    mq_exchange = StringField()
    mq_routing_key = StringField()

    @classmethod
    def key_fields(cls):
        return cls.mq_queue.name


def queue_context_entry(exchange,
                        queue_name,
                        routing=None):
    """ forms queue's context entry """
    if routing is None:
        routing = queue_name

    queue_entry = QueueContextEntry(mq_queue=queue_name,
                                    mq_exchange=exchange,
                                    mq_routing_key=routing)
    return queue_entry
