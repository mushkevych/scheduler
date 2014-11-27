__author__ = 'Bohdan Mushkevych'

from threading import Thread

from amqp import AMQPError

from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.scheduler.abstract_pipeline import AbstractPipeline
from synergy.scheduler.scheduler_constants import TYPE_MANAGED
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
        :param message: <SynergyMqTransmission> mq message """
        try:
            self.logger.info('StatusBusListener {')

            mq_request = SynergyMqTransmission(message.body)
            uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            if uow.unit_of_work_type != TYPE_MANAGED:
                self.logger.info('Received transmission from TYPE_FREERUN execution. Ignoring it.')
                return

            tree = self.timetable.get_tree(uow.process_name)
            node = tree.get_node_by_process(uow.process_name, uow.timeperiod)

            if uow.db_id != node.job_record.related_unit_of_work:
                self.logger.info('Received transmission is likely outdated. Ignoring it.')
                return

            if uow.state not in [unit_of_work.STATE_PROCESSED, unit_of_work.STATE_CANCELED]:
                # rely on Garbage Collector to re-trigger the failing unit_of_work
                self.logger.info('Received unit_of_work status report from %s at %s in non-final state %s. Ignoring it.'
                                 % (uow.process_name, uow.timeperiod, uow.state))
                return

            scheduler_entry_obj = self.scheduler.managed_handlers[uow.process_name].arguments.scheduler_entry_obj
            pipeline = self.scheduler.pipelines[scheduler_entry_obj.state_machine_name]
            assert isinstance(pipeline, AbstractPipeline)

            self.logger.info('Commencing shallow state update for unit_of_work from %s at %s in %s.'
                             % (uow.process_name, uow.timeperiod, uow.state))
            pipeline.shallow_state_update(uow)

        except KeyError:
            self.logger.error('Access error for %s' % str(message.body), exc_info=True)
        except LookupError:
            self.logger.error('Can not perform shallow state update for %s' % str(message.body), exc_info=True)
        except Exception:
            self.logger.error('Unexpected error during shallow state update for %s' % str(message.body), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.logger.info('StatusBusListener }')

    def _run_mq_listener(self):
        try:
            self.consumer.register(self._mq_callback)
            self.logger.info('StatusBusListener: instantiated and actived.')
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
