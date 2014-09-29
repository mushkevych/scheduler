__author__ = 'Bohdan Mushkevych'

from synergy.conf import context
from synergy.db.model.queue_context_entry import QueueContextEntry


class MqQueueContext(object):
    # holds all registered queues. environment-aware
    CONTEXT = context.mq_queue_context

    def __init__(self):
        super(MqQueueContext, self).__init__()

    @classmethod
    def put_context_entry(cls, context_entry):
        assert isinstance(context_entry, QueueContextEntry)
        cls.CONTEXT[context_entry.mq_queue] = context_entry

    @classmethod
    def get_context_entry(cls, process_name=None):
        """ method returns dictionary of strings, preset
        source collection, target collection, queue name, exchange, routing, etc"""
        return cls.CONTEXT[process_name]

    @classmethod
    def get_routing(cls, process_name=None):
        """ method returns routing; it is used to segregate traffic within the queue
        for instance: routing_hourly for hourly reports, while routing_yearly for yearly reports"""
        return cls.CONTEXT[process_name].mq_routing_key

    @classmethod
    def get_exchange(cls, process_name=None):
        """ method returns exchange for this classname.
        Exchange is a component that sits between queue and the publisher"""
        return cls.CONTEXT[process_name].mq_exchange

    @classmethod
    def get_queue(cls, process_name=None):
        """ method returns queue that is applicable for the worker/aggregator, specified by classname"""
        return cls.CONTEXT[process_name].mq_queue
