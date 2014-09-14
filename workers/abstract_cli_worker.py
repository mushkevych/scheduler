""" Module contains common logic for Command Line Callers.
It executes shell command and updates unit_of_work base on command's return code """

__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from psutil.error import TimeoutExpired
from db.model import unit_of_work
from db.dao.unit_of_work_dao import UnitOfWorkDao

from workers.abstract_mq_worker import AbstractMqWorker
from system.performance_tracker import AggregatorPerformanceTicker


class AbstractCliWorker(AbstractMqWorker):
    """ Abstract class is inherited by all workers/aggregators
    that are aware of unit_of_work and capable of processing it"""

    def __init__(self, process_name):
        super(AbstractCliWorker, self).__init__(process_name)
        self.cli_process = None
        self.uow_dao = UnitOfWorkDao(self.logger)

    def __del__(self):
        super(AbstractCliWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = AggregatorPerformanceTicker(logger)
        self.performance_ticker.start()

    # **************** Process Supervisor Methods ************************
    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        raise NotImplementedError('method _start_process must be implemented by AbstractCliWorker child classes')

    def _poll_process(self):
        """ between death of a process and its actual termination lies poorly documented requirement -
            <purging process' io pipes and reading exit status>.
            this can be done either by os.wait() or process.wait()
            @return tuple (boolean: alive, int: return_code) """
        try:
            self.logger.warn(self.cli_process.stderr.read())
            self.logger.info(self.cli_process.stdout.read())
            return_code = self.cli_process.wait(timeout=0.01)
            if return_code is None:
                # process is already terminated
                self.logger.info('Process %s is terminated' % self.process_name)
            else:
                # process is terminated; possibly by OS
                self.logger.info('Process %s got terminated. Cleaning up' % self.process_name)
            self.cli_process = None
            return False, return_code
        except TimeoutExpired:
            # process is alive and OK
            return True, None
        except Exception:
            self.logger.error('Exception on polling: %s' % self.process_name, exc_info=True)
            return False, 999

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ try/except wrapper
        in case exception breaks the abstract method, this method:
        - catches the exception
        - logs the exception
        - marks unit of work as INVALID"""
        try:
            # @param object_id: ObjectId of the unit_of_work from mq
            object_id = message.body
            uow = self.uow_dao.get_one(object_id)
            if uow.state in [unit_of_work.STATE_CANCELED, unit_of_work.STATE_PROCESSED]:
                # garbage collector might have reposted this UOW
                self.logger.warning('Skipping unit_of_work: id %s; state %s;'
                                    % (str(message.body), uow.state), exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            start_timeperiod = uow.start_timeperiod
            end_timeperiod = uow.end_timeperiod

            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            self.uow_dao.update(uow)
            self.performance_ticker.start_uow(uow)

            self._start_process(start_timeperiod, end_timeperiod, uow.arguments)
            code = None
            alive = True
            while alive:
                alive, code = self._poll_process()

            if code == 0:
                uow.number_of_processed_documents = self.performance_ticker.per_job
                uow.finished_at = datetime.utcnow()
                uow.state = unit_of_work.STATE_PROCESSED
                self.performance_ticker.finish_uow()
            else:
                uow.state = unit_of_work.STATE_INVALID
                self.performance_ticker.cancel_uow()

            self.logger.info('Command Line Command return code is %r' % code)
            self.uow_dao.update(uow)
        except Exception as e:
            uow.state = unit_of_work.STATE_INVALID
            self.uow_dao.update(uow)
            self.performance_ticker.cancel_uow()
            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'
                              % (message.body, uow.timeperiod, e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
