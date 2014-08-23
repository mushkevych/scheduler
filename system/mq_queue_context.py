__author__ = 'Bohdan Mushkevych'

from system.decorator import singleton
from db.model.queue_context_entry import QueueContextEntry

_ROUTING_PREFIX = 'routing_'
_QUEUE_PREFIX = 'queue_'

QUEUE_REQUESTED_PACKAGES = 'q_requested_package'
QUEUE_RAW_DATA = 'queue_raw_data'
ROUTING_IRRELEVANT = 'routing_irrelevant'

EXCHANGE_RAW_DATA = 'exchange_raw_data'
EXCHANGE_VERTICAL = 'exchange_vertical'
EXCHANGE_HORIZONTAL = 'exchange_horizontal'
EXCHANGE_ALERT = 'exchange_alert'
EXCHANGE_UTILS = 'exchange_utils'


def _queue_context_entry(exchange,
                         queue_name,
                         routing=None):
    """ forms queue's context entry """
    if routing is None:
        routing = queue_name

    queue_entry = QueueContextEntry()
    queue_entry.mq_queue = queue_name
    queue_entry.mq_exchange = exchange
    queue_entry.mq_routing_key = routing
    return queue_entry


@singleton
class QueueContext(object):
    logger_pool = dict()
    QUEUE_CONTEXT = dict()

    def __init__(self):
        super(QueueContext, self).__init__()

    @classmethod
    def put_context_entry(cls, context_entry):
        assert isinstance(context_entry, QueueContextEntry)
        cls.QUEUE_CONTEXT[context_entry.mq_queue] = context_entry

    @classmethod
    def get_context_entry(cls, process_name=None):
        """ method returns dictionary of strings, preset
        source collection, target collection, queue name, exchange, routing, etc"""
        return cls.QUEUE_CONTEXT[process_name]

    @classmethod
    def get_routing(cls, process_name=None):
        """ method returns routing; it is used to segregate traffic within the queue
        for instance: routing_hourly for hourly reports, while routing_yearly for yearly reports"""
        return cls.QUEUE_CONTEXT[process_name].mq_routing_key

    @classmethod
    def get_exchange(cls, process_name=None):
        """ method returns exchange for this classname.
        Exchange is a component that sits between queue and the publisher"""
        return cls.QUEUE_CONTEXT[process_name].mq_exchange

    @classmethod
    def get_queue(cls, process_name=None):
        """ method returns queue that is applicable for the worker/aggregator, specified by classname"""
        return cls.QUEUE_CONTEXT[process_name].mq_queue

    @classmethod
    def get_routing(cls, queue_name):
        """ method returns routing; it is used to segregate traffic within the queue"""
        return cls.QUEUE_CONTEXT[queue_name].mq_routing_key

    @classmethod
    def get_exchange(cls, queue_name):
        """ method returns exchange for this queue_name.
        Exchange is a component that sits between queue and the publisher"""
        return cls.QUEUE_CONTEXT[queue_name].mq_exchange


queue_entry = _queue_context_entry(exchange=QueueContext.EXCHANGE_HORIZONTAL,
                                   queue_name=QUEUE_REQUESTED_PACKAGES)
QueueContext.put_context_entry(queue_entry)


if __name__ == '__main__':
    pass
