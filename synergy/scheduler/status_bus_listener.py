__author__ = 'Bohdan Mushkevych'

from threading import Thread

from amqp import AMQPError

from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.scheduler.scheduler_constants import QUEUE_UOW_REPORT
from synergy.mq.flopsy import Consumer


class StatusBusListener(object):
    """ class instance listens to the QUEUE_UOW_REPORT queue and updates Timetable records correspondingly """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.timetable = scheduler.timetable
        self.logger = scheduler.logger
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.consumer = Consumer(QUEUE_UOW_REPORT)
        self.main_thread = None

    def __del__(self):
        try:
            self.logger.info('StatusBusListener: Closing Flopsy Consumer...')
            self.consumer.close()
        except Exception as e:
            self.logger.error('StatusBusListener: Exception caught while closing Flopsy Consumer: %s' % str(e))

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ method processes messages from Synergy Worker and updates corresponding Timetable record,
        as well as the job itself
        @param message: <SynergyMqTransmission> mq message """
        try:
            mq_request = SynergyMqTransmission(message.body)
            uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            if uow.state in [unit_of_work.STATE_PROCESSED]:
                # unit_of_work was successfully completed. update the job record
                # 1. get process_name and timeperiod
                # 2. make sure the UOW type is MANAGED
                # 3. get the job record from self.timetable
                # 4. make sure the UOW from the message is the UOW from the job entry
                # 5. retrieve appropriate pipeline from Scheduler and call update_uow method
                pass

        except Exception:
            self.logger.error('StatusBusListener: Can not identify unit_of_work %s' % str(message.body), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)

    def _run_mq_listener(self):
        self.logger.info('StatusBusListener: Starting...')
        try:
            self.consumer.register(self._mq_callback)
            self.consumer.wait()
        except (AMQPError, IOError) as e:
            self.logger.error('StatusBusListener: AMQPError %s' % str(e))
        finally:
            self.__del__()
            self.logger.info('StatusBusListener: Shutting down... All auxiliary threads stopped.')

    def start(self, *_):
        self.main_thread = Thread(target=self._run_mq_listener)
        self.main_thread.daemon = True
        self.main_thread.start()
