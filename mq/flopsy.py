import amqp
import json
import uuid

from collections import deque
from threading import Lock

from settings import settings
from system.decorator import thread_safe
from system.process_context import ProcessContext


class SynergyAware(object):
    def __init__(self, name):
        self.name = name
        if name in ProcessContext.PROCESS_CONTEXT:
            self.routing_key = ProcessContext.get_routing(name)
            self.exchange = ProcessContext.get_exchange(name)
            self.queue = ProcessContext.get_queue(name)
        else:
            self.routing_key = ProcessContext.get_routing_for_q(name)
            self.exchange = ProcessContext.get_exchange_for_q(name)
            self.queue = name


class Connection(object):
    def __init__(self,
                 host=settings['mq_host'],
                 user_id=settings['mq_user_id'],
                 password=settings['mq_password'],
                 vhost=settings['mq_vhost'],
                 port=settings['mq_port']):
        self.db_host = host
        self.user_id = user_id
        self.password = password
        self.vhost = vhost
        self.port = port
        self.connection = None

        self.connect()

    def __del__(self):
        self.close()

    def connect(self):
        self.connection = amqp.Connection(
            host='%s:%s' % (self.db_host, self.port),
            userid=self.user_id,
            password=self.password,
            virtual_host=self.vhost
        )

    def close(self):
        if getattr(self, 'connection'):
            self.connection.close()


class Consumer(SynergyAware):
    def __init__(self,
                 name,
                 durable=settings['mq_durable'],
                 exclusive=settings['mq_exclusive'],
                 auto_delete=settings['mq_auto_delete'],
                 connection=None):
        super(Consumer, self).__init__(name)
        self.callback = None
        self.is_running = True

        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.connection = connection or Connection()
        self.channel = self.connection.connection.channel()

        self.channel.queue_declare(
            queue=self.queue,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete
        )
        self.channel.exchange_declare(
            exchange=self.exchange,
            type='direct',
            durable=self.durable,
            auto_delete=self.auto_delete
        )
        self.channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        self.channel.basic_consume(
            queue=self.queue,
            no_ack=settings['mq_no_ack'],
            callback=self.dispatch,
            consumer_tag=str(uuid.uuid4())
        )

    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()

        if getattr(self, 'connection'):
            self.connection.close()

        self.is_running = False

    def wait(self, timeout=None):
        while self.is_running:
            channel_id, method_sig, args, content = \
                self.connection.connection._wait_multiple(channels={self.channel.channel_id: self.channel},
                                                          allowed_methods=None,
                                                          timeout=timeout)
            self.channel.dispatch_method(method_sig, args, content)

    def dispatch(self, message):
        decoded = json.loads(message.body)
        message.body = decoded['data']
        if self.callback is not None:
            self.callback(message)

    def acknowledge(self, tag):
        if settings['mq_no_ack'] is False:
            self.channel.basic_ack(delivery_tag=tag)

    def reject(self, tag):
        if settings['mq_no_ack'] is False:
            self.channel.basic_reject(delivery_tag=tag, requeue=True)

    def cancel(self, tag):
        if settings['mq_no_ack'] is False:
            self.channel.basic_reject(delivery_tag=tag, requeue=False)

    def register(self, callback):
        self.callback = callback

    def unregister(self):
        self.callback = None


class Publisher(SynergyAware):
    def __init__(self,
                 name,
                 connection=None,
                 delivery_mode=settings['mq_delivery_mode'],
                 parent_pool=None):
        super(Publisher, self).__init__(name)
        self.connection = connection or Connection()
        self.channel = self.connection.connection.channel()
        self.delivery_mode = delivery_mode
        self.parent_pool = parent_pool

    def __del__(self):
        self.release()

    def publish(self, message_data):
        encoded = json.dumps({'data': message_data})
        message = amqp.Message(encoded)
        message.properties['delivery_mode'] = self.delivery_mode
        self.channel.basic_publish(
            message,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        return message

    def release(self):
        if self.parent_pool is not None:
            self.parent_pool.put(self)
        else:
            self.close()

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()

        if getattr(self, 'connection'):
            self.connection.close()


class _Pool(object):
    def __init__(self, logger, name):
        self.publishers = deque()
        self.name = name
        self.logger = logger
        self.lock = Lock()

    def __del__(self):
        self.close()

    @thread_safe
    def get(self):
        """ :return valid :mq::flopsy::Publisher instance """
        if len(self.publishers) == 0:
            return Publisher(name=self.name, parent_pool=self)
        else:
            return self.publishers.pop()

    @thread_safe
    def put(self, publisher):
        self.publishers.append(publisher)

    @thread_safe
    def close(self, suppress_logging=False):
        """ purges all connections. method closes ampq connection (disconnects) """
        for el in self.publishers:
            try:
                el.close()
            except Exception as e:
                if not suppress_logging:
                    self.logger.error('exception on closing the publisher %s: %s' % (self.name, str(e)))
                else:
                    self.logger.info('error trace while closing publisher %s suppressed' % self.name)
        self.publishers.clear()


class PublishersPool(object):
    def __init__(self, logger):
        self.publishers = dict()
        self.logger = logger

    def __del__(self):
        publisher_names = self.publishers.keys()
        for name in publisher_names:
            self.close(name, suppress_logging=True)

    def get(self, name):
        """ creates connection to the MQ with process-specific settings
        :return :mq::flopsy::Publisher instance"""
        if name not in self.publishers:
            self.publishers[name] = _Pool(logger=self.logger, name=name)
        return self.publishers[name].get()

    def put(self, publisher):
        """ releases the Publisher instance for reuse"""
        if publisher.name not in self.publishers:
            self.publishers[publisher.name] = _Pool(logger=self.logger, name=publisher.name)
        self.publishers[publisher.name].put(publisher)

    def reset_all(self, suppress_logging=False):
        """ iterates thru the list of established connections and resets them by disconnecting and reconnecting """
        publisher_names = self.publishers.keys()
        for name in publisher_names:
            self.reset(name, suppress_logging)

    def reset(self, name, suppress_logging=False):
        """ resets established connection by disconnecting and reconnecting """
        self.close(name, suppress_logging)
        self.get(name)
        self.logger.info('Reset AMPQ publisher for %s' % name)

    def close(self, name, suppress_logging=False):
        """ iterates thru all pooled publishers and closes them (closes amqp connection) """
        try:
            publisher_names = self.publishers.keys()
            if name in publisher_names:
                self.publishers[name].close()
                del self.publishers[name]
        except Exception as e:
            if not suppress_logging:
                self.logger.error('exception on closing the publisher for %s: %s' % (name, str(e)))
            else:
                self.logger.info('error trace while closing publisher for %s suppressed' % name)
