__author__ = 'Bohdan Mushkevych'

from datetime import datetime

from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.mq.flopsy import PublishersPool
from synergy.workers.abstract_mq_worker import AbstractMqWorker
from synergy.system.performance_tracker import UowAwareTracker
from synergy.scheduler.scheduler_constants import QUEUE_UOW_REPORT


class AbstractUowAwareWorker(AbstractMqWorker):
    """ Abstract class is inherited by all workers/aggregators
    that are aware of unit_of_work and capable of processing it"""

    def __init__(self, process_name):
        super(AbstractUowAwareWorker, self).__init__(process_name)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.publishers = PublishersPool(self.logger)

    def __del__(self):
        self.publishers.close()
        super(AbstractUowAwareWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = UowAwareTracker(logger)
        self.performance_ticker.start()

    def _process_uow(self, uow):
        """
        :param uow: unit_of_work to process
        :return: a tuple (number of processed items/documents/etc, desired unit_of_work state) or None
        if None is returned then it is assumed that the return tuple is (0, unit_of_work.STATE_PROCESSED)
        :raise an Exception if the UOW shall be marked as STATE_INVALID
        """
        raise NotImplementedError('_process_uow must be overridden in the child class %s' % self.__class__.__name__)

    def _clean_up(self):
        """ method is called from the *finally* clause and is suppose to clean up after the uow processing """
        pass

    def _mq_callback(self, message):
        try:
            mq_request = SynergyMqTransmission.from_json(message.body)
            uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            if not uow.is_requested:
                # accept only UOW in STATE_REQUESTED
                self.logger.warn('Skipping unit_of_work: id %s; state %s;' % (str(message.body), uow.state),
                                 exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            self.uow_dao.update(uow)
            self.performance_ticker.start_uow(uow)

            result = self._process_uow(uow)
            if result is None or isinstance(result, str):
                number_of_aggregated_objects, target_state = 0, unit_of_work.STATE_PROCESSED
            else:
                number_of_aggregated_objects, target_state = result

            uow.number_of_aggregated_documents = number_of_aggregated_objects
            uow.number_of_processed_documents = self.performance_ticker.per_job
            uow.finished_at = datetime.utcnow()
            uow.state = target_state
            self.uow_dao.update(uow)
            self.performance_ticker.finish_uow()
        except Exception as e:
            fresh_uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            self.performance_ticker.cancel_uow()
            if fresh_uow.is_canceled:
                self.logger.warn('unit_of_work: id %s was likely marked by MX as SKIPPED. '
                                 'No unit_of_work update is performed.' % str(message.body),
                                 exc_info=False)
            else:
                self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'
                                  % (message.body, uow.timeperiod, e), exc_info=True)
                uow.state = unit_of_work.STATE_INVALID
                self.uow_dao.update(uow)

        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
            self._clean_up()

        try:
            publisher = self.publishers.get(QUEUE_UOW_REPORT)
            publisher.publish(mq_request.document)
            publisher.release()
            self.logger.info('Published unit_of_work status report into %s queue' % QUEUE_UOW_REPORT)
        except Exception:
            self.logger.error('Error on unit_of_work status report publishing', exc_info=True)
