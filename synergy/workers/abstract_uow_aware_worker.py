__author__ = 'Bohdan Mushkevych'

from datetime import datetime

from synergy.db.model import unit_of_work
from synergy.db.model.mq_transmission import MqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.system.mq_transmitter import MqTransmitter
from synergy.system.performance_tracker import UowAwareTracker
from synergy.system.log_recording_handler import LogRecordingHandler
from synergy.workers.abstract_mq_worker import AbstractMqWorker


class AbstractUowAwareWorker(AbstractMqWorker):
    """ Abstract class is inherited by all workers/aggregators
    that are aware of unit_of_work and capable of processing it """

    def __init__(self, process_name, perform_db_logging=False):
        super(AbstractUowAwareWorker, self).__init__(process_name)
        self.perform_db_logging = perform_db_logging
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.mq_transmitter = MqTransmitter(self.logger)

    def __del__(self):
        del self.mq_transmitter
        super(AbstractUowAwareWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_tracker(self, logger):
        self.performance_tracker = UowAwareTracker(logger)
        self.performance_tracker.start()

    def _process_uow(self, uow):
        """
        :param uow: unit_of_work to process
        :return: a tuple (number of processed items/documents/etc, desired unit_of_work state) or None
        if None is returned then it is assumed that the return tuple is (0, unit_of_work.STATE_PROCESSED)
        :raise an Exception if the UOW shall be marked as STATE_INVALID
        """
        raise NotImplementedError(f'method _process_uow must be implemented by {self.__class__.__name__}')

    def _clean_up(self):
        """ method is called from the *finally* clause and is suppose to clean up after the uow processing """
        pass

    def _mq_callback(self, message):
        try:
            mq_request = MqTransmission.from_json(message.body)
            uow = self.uow_dao.get_one(mq_request.record_db_id)
            if not uow.is_requested:
                # accept only UOW in STATE_REQUESTED
                self.logger.warning(f'Skipping UOW: id {message.body}; state {uow.state};', exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error(f'Safety fuse. Can not identify UOW {message.body}', exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        log_recording_handler = LogRecordingHandler(self.logger, uow.db_id)
        try:
            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            self.uow_dao.update(uow)
            self.performance_tracker.start_uow(uow)

            if self.perform_db_logging:
                log_recording_handler.attach()

            result = self._process_uow(uow)
            if result is None:
                self.logger.warning(f'Method {self.__class__.__name__}._process_uow returned None. '
                                    f'Assuming happy flow.')
                number_of_aggregated_objects, target_state = 0, unit_of_work.STATE_PROCESSED
            else:
                number_of_aggregated_objects, target_state = result

            uow.number_of_aggregated_documents = number_of_aggregated_objects
            uow.number_of_processed_documents = self.performance_tracker.success_per_job
            uow.finished_at = datetime.utcnow()
            uow.state = target_state
            self.uow_dao.update(uow)

            if uow.is_finished:
                self.performance_tracker.finish_uow()
            else:
                self.performance_tracker.cancel_uow()

        except Exception as e:
            fresh_uow = self.uow_dao.get_one(mq_request.record_db_id)
            self.performance_tracker.cancel_uow()
            if fresh_uow.is_canceled:
                self.logger.warning('UOW {0} for {1}@{2} was likely marked by MX as SKIPPED. No UOW update performed.'
                                    .format(uow.db_id, uow.process_name, uow.timeperiod), exc_info=False)
            else:
                self.logger.error('Safety fuse while processing UOW {0} for {1}@{2}: {3}'
                                  .format(uow.db_id, uow.process_name, uow.timeperiod, e), exc_info=True)
                uow.state = unit_of_work.STATE_INVALID
                self.uow_dao.update(uow)

        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
            self._clean_up()
            log_recording_handler.detach()

        try:
            self.mq_transmitter.publish_uow_status(uow)
            self.logger.info(f'UOW *{uow.state}* status report published into MQ')
        except Exception:
            self.logger.error('Error on UOW status report publishing', exc_info=True)
