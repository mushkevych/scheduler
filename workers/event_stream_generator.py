__author__ = 'Bohdan Mushkevych'

import datetime
import random
import time
import math

from threading import Thread
from amqp import AMQPError

from db.model.raw_data import RawData
from synergy.mq.flopsy import Publisher
from synergy.system.performance_tracker import SimpleTracker
from synergy.system.synergy_process import SynergyProcess


SLEEP_TIME = 0.05
TICK_INTERVAL = 10


class EventStreamGenerator(SynergyProcess):
    def __init__(self, process_name):
        super(EventStreamGenerator, self).__init__(process_name)
        self.main_thread = None
        self.publisher = Publisher(process_name)
        self.performance_ticker = SimpleTracker(self.logger)
        self.previous_tick = time.time()
        self.thread_is_running = True

        utc_date = datetime.datetime.utcnow()
        self.number_of_groups = utc_date.year * math.pow(10, 12) + \
            utc_date.month * math.pow(10, 10) + \
            utc_date.day * math.pow(10, 8) + \
            utc_date.hour * math.pow(10, 6) + \
            utc_date.minute * math.pow(10, 4) + \
            utc_date.second * math.pow(10, 2)

        self.logger.info('Started {0}'.format(self.process_name))

    def __del__(self):
        self.publisher.close()
        self.performance_ticker.cancel()
        super(EventStreamGenerator, self).__del__()
        self.logger.info('Exiting main thread. All auxiliary threads stopped.')

    def _generate_key(self):
        _id = random.randint(0, 100000)
        domain_name = 'domain{0}__com'.format(_id)

        session_no = self.number_of_groups + random.randint(0, 99)
        session_id = 'session_{0}'.format(session_no)

        return domain_name, time.time(), session_id

    def _run_stream_generation(self):
        self.logger.info('Stream Generator: ON. Expected rate: {0}/s, {1}/m, {2}/h, {3}/d'
                         .format(1 / SLEEP_TIME, 1 / SLEEP_TIME * 60, 1 / SLEEP_TIME * 3600, 1 / SLEEP_TIME * 86400))
        self.performance_ticker.start()
        random.seed('RANDOM_SEED_OBJECT')
        document = RawData()
        while self.thread_is_running:
            if time.time() - self.previous_tick > TICK_INTERVAL:
                # increment group number every TICK_INTERVAL seconds
                self.number_of_groups += 100
                self.previous_tick = time.time()
            try:
                document.key = self._generate_key()
                document.ip = '{0}.{1}.{2}.{3}'.format(random.randint(0, 255),
                                                       random.randint(0, 255),
                                                       random.randint(0, 255),
                                                       random.randint(0, 255))

                document.screen_res = (random.randrange(340, 1080, 100), random.randrange(240, 980, 100))

                if self.performance_ticker.tracker.success.per_tick % 7 == 0:
                    document.os = 'OSX'
                    document.browser = 'Safari-10'
                    document.language = 'en_us'
                    document.country = 'usa'
                elif self.performance_ticker.tracker.success.per_tick % 5 == 0:
                    document.os = 'Linux'
                    document.browser = 'FireFox-40'
                    document.language = 'en_ca'
                    document.country = 'canada'
                elif self.performance_ticker.tracker.success.per_tick % 3 == 0:
                    document.os = 'Windows'
                    document.browser = 'IE-60'
                    document.language = 'ge_de'
                    document.country = 'germany'
                else:
                    document.os = 'Android'
                    document.browser = 'FireMini-20'
                    document.language = 'es'
                    document.country = 'eu'

                document.is_page_view = True
                self.publisher.publish(document.document)
                self.performance_ticker.tracker.increment_success()
                time.sleep(SLEEP_TIME)
            except (AMQPError, IOError) as e:
                self.thread_is_running = False
                self.performance_ticker.cancel()
                self.logger.error('AMQPError: {0}'.format(e))
            except Exception as e:
                self.performance_ticker.tracker.increment_failure()
                self.logger.info('safety fuse: {0}'.format(e))

    def start(self, *_):
        self.main_thread = Thread(target=self._run_stream_generation)
        self.main_thread.start()

    def cancel(self):
        self.thread_is_running = False


if __name__ == '__main__':
    from constants import PROCESS_STREAM_GEN

    generator = EventStreamGenerator(PROCESS_STREAM_GEN)
    generator.start()
