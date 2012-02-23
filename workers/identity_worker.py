"""
Created on 2011-12-12

Module contains logic for YES worker - one that marks any units_of_work as complete

@author: Bohdan Mushkevych
"""
from bson.objectid import ObjectId
from datetime import datetime

from scheduler.units_of_work_collection import UnitsOfWorkCollection
from scheduler import unit_of_work_helper
from workers.abstract_worker import AbstractWorker
from system.performance_ticker import AggregatorPerformanceTicker


class IdentityWorker(AbstractWorker):
    """ Marks all unit_of_work as <complete>"""

    def __init__(self, process_name):
        self.hadoop_process = None
        super(IdentityWorker, self).__init__(process_name)

    def __del__(self):
        super(IdentityWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = AggregatorPerformanceTicker(logger)
        self.performance_ticker.start()

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ try/except wrapper
        in case exception breaks the abstract method, this method:
        - catches the exception
        - logs the exception
        - marks unit of work as INVALID"""
        unit_of_work = None
        try:
            # @param object_id: ObjectId of the unit_of_work from mq
            object_id = ObjectId(message.body)
            unit_of_work = unit_of_work_helper.retrieve_by_id(self.logger, object_id)
            if unit_of_work.get_state() == UnitsOfWorkCollection.STATE_CANCELED \
                or unit_of_work.get_state() == UnitsOfWorkCollection.STATE_PROCESSED:
                # garbage collector might have reposted this UOW
                self.logger.warning('Skipping unit_of_work: id %s; state %s;' \
                                    % (str(message.body), unit_of_work.get_state()), exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            self.performance_ticker.start_uow(unit_of_work)
            unit_of_work.set_state(UnitsOfWorkCollection.STATE_PROCESSED)
            unit_of_work.set_number_of_processed_documents(0)
            unit_of_work.set_started_at(datetime.utcnow())
            unit_of_work.set_finished_at(datetime.utcnow())
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.finish_uow()
        except Exception as e:
            unit_of_work.set_state(UnitsOfWorkCollection.STATE_INVALID)
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.cancel_uow()
            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'\
                              % (message.body, unit_of_work.get_timestamp(), e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
