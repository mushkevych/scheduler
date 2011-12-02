"""
Created on 2011-01-21

@author: Bohdan Mushkevych
"""
from flopsy.flopsy import Publisher
from data_collections.raw_data import RawData
from system.performance_ticker import WorkerPerformanceTicker
from system.synergy_process import SynergyProcess

from amqplib.client_0_8.exceptions import AMQPException
from threading import Thread
import datetime
import random
import time
import math

SLEEP_TIME = 0.03
TICK_INTERVAL = 10

class EventStreamGenerator(SynergyProcess):
    def __init__(self, process_name):
        super(EventStreamGenerator, self).__init__(process_name)
        self.publisher = Publisher(process_name)
        self.performance_ticker = WorkerPerformanceTicker(self.logger)
        self.previous_tick = time.time()
        self.thread_is_running = True

        utc_date = datetime.datetime.utcnow()
        self.number_of_groups = utc_date.year * math.pow(10, 12) + \
                                utc_date.month * math.pow(10, 10) + \
                                utc_date.day * math.pow(10, 8) + \
                                utc_date.hour * math.pow(10, 6) + \
                                utc_date.minute * math.pow(10, 4) + \
                                utc_date.second * math.pow(10, 2)

        self.logger.info('Started %s' % self.process_name)

    def __del__(self):
        self.publisher.close()
        self.performance_ticker.cancel()
        super(EventStreamGenerator, self).__del__()
        self.logger.info('Exiting main thread. All auxiliary threads stopped.')

    def _generate_key(self):
        id = random.randint(0, 100000)
        domain_name = 'domain%d.com' % id
        return domain_name, time.time()

    def _run_stream_generation(self):
        self.logger.info('Stream Generator: ON. Expected rate: %d/s, %d/m, %d/h, %d/d' % \
                         (1 / SLEEP_TIME, 1 / SLEEP_TIME * 60, 1 / SLEEP_TIME * 3600, 1 / SLEEP_TIME * 86400))
        self.performance_ticker.start()
        random.seed('RANDOM_SEED_OBJECT')
        document = RawData()
        while self.thread_is_running:
            if time.time() - self.previous_tick > TICK_INTERVAL:
                #increment group number every TICK_INTERVAL seconds
                self.number_of_groups += 100
                self.previous_tick = time.time()
            try:
                key = self._generate_key()
                document.composite_key(key[0], key[1])
                session_no = self.number_of_groups + random.randint(0, 99)

                document.set_session_id('session_%d' % session_no)
                document.set_ip('%d.%d.%d.%d' % (random.randint(0, 255),
                                                 random.randint(0, 255),
                                                 random.randint(0, 255),
                                                 random.randint(0, 255)))
                
                document.set_screen_res(random.randrange(340, 1080, 100), random.randrange(240, 980, 100))

                if self.performance_ticker.posts_per_tick % 7 == 0:
                    document.set_os('OSX')
                    document.set_browser('Safari-1.0')
                    document.set_language('en_us')
                    document.set_country('usa')
                elif self.performance_ticker.posts_per_tick % 5 == 0:
                    document.set_os('Linux')
                    document.set_browser('FireFox-4.0')
                    document.set_language('en_ca')
                    document.set_country('canada')
                elif self.performance_ticker.posts_per_tick % 3 == 0:
                    document.set_os('Windows')
                    document.set_browser('IE-6.0')
                    document.set_language('ge_de')
                    document.set_country('germany')
                else:
                    document.set_os('Android')
                    document.set_browser('FireMini-2.0')
                    document.set_language('es')
                    document.set_country('eu')
                    
                document.set_page_view()
                self.publisher.publish(document.get_document())
                self.performance_ticker.increment()
                time.sleep(SLEEP_TIME)
            except (AMQPException, IOError) as e:
                self.thread_is_running = False
                self.performance_ticker.cancel()
                self.logger.error('AMQPException: %s' % str(e))
            except Exception as e:
                self.logger.info('safety fuse: %s' % str(e)) 

    def start(self):
        self.main_thread = Thread(target=self._run_stream_generation)
        self.main_thread.start()

    def cancel(self):
        self.thread_is_running = False


if __name__ == '__main__':
    from system.process_context import PROCESS_STREAM_GEN

    generator = EventStreamGenerator(PROCESS_STREAM_GEN)
    generator.start()