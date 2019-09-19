__author__ = 'Bohdan Mushkevych'

from threading import Thread

from amqp import AMQPError

from synergy.db.model.mq_transmission import MqTransmission
from synergy.db.model import unit_of_work
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.scheduler.scheduler_constants import QUEUE_UOW_STATUS
from synergy.mq.flopsy import Consumer


class UowStatusListener(object):
    """ class instance listens to the QUEUE_UOW_STATUS queue and updates Timetable records correspondingly """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.timetable = scheduler.timetable
        self.logger = scheduler.logger
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.consumer = Consumer(QUEUE_UOW_STATUS)
        self.main_thread = None

    def __del__(self):
        try:
            self.logger.info('UowStatusListener: Closing Flopsy Consumer...')
            self.consumer.close()
        except Exception as e:
            self.logger.error(f'UowStatusListener: Exception caught while closing Flopsy Consumer: {e}')

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ method processes messages from Synergy Worker and updates corresponding Timetable record,
            as well as the job itself
            :param message: <MqTransmission> mq message """
        try:
            self.logger.info('UowStatusListener {')

            mq_request = MqTransmission.from_json(message.body)
            uow = self.uow_dao.get_one(mq_request.record_db_id)
            if uow.unit_of_work_type != unit_of_work.TYPE_MANAGED:
                self.logger.info('Received transmission from non-managed UOW execution: {0}. Ignoring it.'
                                 .format(uow.unit_of_work_type))
                return

            tree = self.timetable.get_tree(uow.process_name)
            node = tree.get_node(uow.process_name, uow.timeperiod)

            if uow.db_id != node.job_record.related_unit_of_work:
                self.logger.info('Received transmission is likely outdated. Ignoring it.')
                return

            if not uow.is_finished:
                # rely on Garbage Collector to re-trigger the failing unit_of_work
                self.logger.info('Received transmission from {0}@{1} in non-final state {2}. Ignoring it.'
                                 .format(uow.process_name, uow.timeperiod, uow.state))
                return

            state_machine = self.scheduler.state_machine_for(node.process_name)
            self.logger.info('Commencing StateMachine.notify with UOW from {0}@{1} in {2}.'
                             .format(uow.process_name, uow.timeperiod, uow.state))
            state_machine.notify(uow)

        except KeyError:
            self.logger.error(f'Access error for {message.body}', exc_info=True)
        except Exception:
            self.logger.error(f'Error during StateMachine.notify call {message.body}', exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.logger.info('UowStatusListener }')

    def _run_mq_listener(self):
        try:
            self.consumer.register(self._mq_callback)
            self.logger.info('UowStatusListener: instantiated and activated.')
            self.consumer.wait()
        except (AMQPError, IOError) as e:
            self.logger.error(f'UowStatusListener: AMQPError {e}')
        finally:
            self.__del__()
            self.logger.info('UowStatusListener: Shut down.')

    def start(self, *_):
        self.main_thread = Thread(target=self._run_mq_listener)
        self.main_thread.daemon = True
        self.main_thread.start()

    def stop(self):
        """ method stops currently MQ Consumer listener, if any """
        self.__del__()
