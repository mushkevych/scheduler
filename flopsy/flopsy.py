import json as simplejson
import uuid
from system.process_context import ProcessContext
from settings import settings
from amqplib import client_0_8 as amqp

DEFAULT_HOST = settings['mq_host']
DEFAULT_USER_ID = settings['mq_user_id']
DEFAULT_PASSWORD = settings['mq_password']
DEFAULT_VHOST = settings['mq_vhost']
DEFAULT_PORT = settings['mq_port']
DEFAULT_INSIST = settings['mq_insist']
DEFAULT_QUEUE = settings['mq_queue']
DEFAULT_ROUTING_KEY = settings['mq_routing_key']
DEFAULT_EXCHANGE = settings['mq_exchange']
DEFAULT_DURABLE = settings['mq_durable']
DEFAULT_EXCLUSIVE = settings['mq_exclusive']
DEFAULT_AUTO_DELETE = settings['mq_auto_delete']
DEFAULT_DELIVERY_MODE = settings['mq_delivery_mode']
DEFAULT_NO_ACK_MODE = settings['mq_no_ack']


class SynergyAware(object):
    def __init__(self, process_name):
        self.routing_key = ProcessContext.get_routing(process_name)
        self.exchange = ProcessContext.get_exchange(process_name)
        self.queue = ProcessContext.get_queue(process_name)


class Connection(object):
    def __init__(self, 
                 host=DEFAULT_HOST, 
                 user_id=DEFAULT_USER_ID,
                 password=DEFAULT_PASSWORD, 
                 vhost=DEFAULT_VHOST, 
                 port=DEFAULT_PORT,
                 insist=DEFAULT_INSIST):

        self.db_host = host
        self.user_id = user_id
        self.password = password
        self.vhost = vhost
        self.port = port
        self.insist = insist
        
        self.connect()

    def connect(self):
        self.connection = amqp.Connection(
            host='%s:%s' % (self.db_host, self.port),
            userid=self.user_id,
            password=self.password,
            virtual_host=self.vhost,
            insist=self.insist
        )
    
    def close(self):
        if getattr(self, 'connection'):
            self.connection.close()


class Consumer(SynergyAware):
    def __init__(self, 
                 process_name,
                 durable=DEFAULT_DURABLE, 
                 exclusive=DEFAULT_EXCLUSIVE,
                 auto_delete=DEFAULT_AUTO_DELETE, 
                 connection=None):
        super(Consumer, self).__init__(process_name)
        self.callback = None

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
            no_ack=DEFAULT_NO_ACK_MODE,
            callback=self.dispatch,
            consumer_tag=str(uuid.uuid4())
        )

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()
        if getattr(self, 'connection'):
            self.connection.close()

    def wait(self):
        while True:
            self.channel.wait()

    def dispatch(self, message):
        decoded = simplejson.loads(message.body)
        message.body = decoded['data']
        if self.callback is not None:
            self.callback(message)
    
    def acknowledge(self, tag):
        if DEFAULT_NO_ACK_MODE == False:
            self.channel.basic_ack(delivery_tag=tag)

    def reject(self, tag):
        if DEFAULT_NO_ACK_MODE == False:
            self.channel.basic_reject(delivery_tag=tag, requeue=True)

    def cancel(self, tag):
        if DEFAULT_NO_ACK_MODE == False:
            self.channel.basic_reject(delivery_tag=tag, requeue=False)

    def register(self, callback):
        self.callback = callback

    def unregister(self):
        self.callback = None


class Publisher(SynergyAware):
    def __init__(self, 
                 process_name,
                 connection=None,
                 delivery_mode=DEFAULT_DELIVERY_MODE):
        super(Publisher, self).__init__(process_name)
        self.connection = connection or Connection()
        self.channel = self.connection.connection.channel()
        self.delivery_mode = delivery_mode

    def publish(self, message_data):
        encoded = simplejson.dumps({'data' : message_data})
        message = amqp.Message(encoded)
        message.properties['delivery_mode'] = self.delivery_mode
        self.channel.basic_publish(
            message,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        return message

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()
        if getattr(self, 'connection'):
            self.connection.connection.close()


class PublishersPool(object):
    def __init__(self, logger):
        self.publishers = dict()
        self.logger = logger
    
    def __del__(self):
        for every in self.publishers:
            self.close_publisher(every)

    def get_publisher(self, process_name):
        """ creates connection to the MQ with process-specific settings
        @return amqp connection"""
        if process_name not in self.publishers:
            self.publishers[process_name] = Publisher(process_name)
        return self.publishers[process_name]
    
    def reset_all_publishers(self, suppress_logging=False):
        """ iterates thru the list of established connections and resets them by disconnecting and reconnecting """
        list_of_processes = self.publishers.keys()
        for process_name in list_of_processes:
            self.reset_publisher(process_name, suppress_logging)
            self.logger.info('Reset AMPQ publisher for %s' % process_name)

    def reset_publisher(self, process_name, suppress_logging=False):
        """ resets established connection by disconnecting and reconnecting """
        self.close_publisher(process_name, suppress_logging)
        del self.publishers[process_name]
        self.get_publisher(process_name)
        
    def close_publisher(self, process_name, suppress_logging=False):
        """ method closes ampq connection (disconnects) """
        try:
            if process_name in self.publishers:
                self.publishers[process_name].close()
        except Exception as e:
            if not suppress_logging:
                self.logger.error('exception on closing the publisher for %s: %s' % (process_name, str(e)))
            else:
                self.logger.info('error trace while closing publisher for %s suppressed' % process_name)

