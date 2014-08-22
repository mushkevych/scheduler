__author__ = 'Bohdan Mushkevych'

from db.model.base_model import *

MQ_QUEUE = 'mq_queue'
MQ_EXCHANGE = 'mq_exchange'
MQ_ROUTING_KEY = 'mq_routing_key'


class QueueContextEntry(BaseModel):
    """ This class presents Process Context Entry record """

    def __init__(self, document=None):
        super(QueueContextEntry, self).__init__(document)

    @BaseModel.key.getter
    def key(self):
        return self.data[MQ_QUEUE]

    @key.setter
    def key(self, value):
        """
        @param value: name of the process
        """
        self.data[MQ_QUEUE] = value

    @property
    def mq_queue(self):
        return self.data[MQ_QUEUE]

    @mq_queue.setter
    def mq_queue(self, value):
        self.data[MQ_QUEUE] = value

    @property
    def mq_exchange(self):
        return self.data[MQ_EXCHANGE]

    @mq_exchange.setter
    def mq_exchange(self, value):
        self.data[MQ_EXCHANGE] = value

    @property
    def mq_routing_key(self):
        return self.data[MQ_ROUTING_KEY]

    @mq_routing_key.setter
    def mq_routing_key(self, value):
        self.data[MQ_ROUTING_KEY] = value
