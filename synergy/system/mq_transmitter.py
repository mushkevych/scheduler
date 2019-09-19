__author__ = 'Bohdan Mushkevych'

from threading import Lock

from synergy.scheduler.scheduler_constants import QUEUE_UOW_STATUS, QUEUE_JOB_STATUS
from synergy.db.model.mq_transmission import MqTransmission
from synergy.mq.flopsy import PublishersPool
from synergy.system.decorator import thread_safe


class MqTransmitter(object):
    """ a class hosting several Message Queue helper methods to send MqTransmission """

    def __init__(self, logger):
        self.logger = logger
        self.lock = Lock()
        self.publishers = PublishersPool(self.logger)

    def __del__(self):
        try:
            self.logger.info('Closing Flopsy Publishers Pool...')
            self.publishers.close()
        except Exception as e:
            self.logger.error(f'Exception caught while closing Flopsy Publishers Pool: {e}')

    @thread_safe
    def publish_managed_uow(self, uow):
        mq_request = MqTransmission(process_name=uow.process_name, record_db_id=uow.db_id)

        publisher = self.publishers.get(uow.process_name)
        publisher.publish(mq_request.document)
        publisher.release()

    @thread_safe
    def publish_freerun_uow(self, freerun_entry, uow):
        mq_request = MqTransmission(process_name=freerun_entry.process_name,
                                    entry_name=freerun_entry.entry_name,
                                    record_db_id=uow.db_id)

        publisher = self.publishers.get(freerun_entry.process_name)
        publisher.publish(mq_request.document)
        publisher.release()

    @thread_safe
    def publish_job_status(self, job_record, finished_only=True):
        if finished_only and not job_record.is_finished:
            return

        mq_request = MqTransmission(process_name=job_record.process_name, record_db_id=job_record.db_id)
        publisher = self.publishers.get(QUEUE_JOB_STATUS)
        publisher.publish(mq_request.document)
        publisher.release()

    @thread_safe
    def publish_uow_status(self, uow):
        mq_request = MqTransmission(process_name=uow.process_name, record_db_id=uow.db_id)

        publisher = self.publishers.get(QUEUE_UOW_STATUS)
        publisher.publish(mq_request.document)
        publisher.release()
