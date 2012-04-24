"""
Created on 2011-02-01

Module contains common logic for Hadoop Callers.
It calls Hadoop Map/Reduce and updates unit_of_work base on Hadoop return code

@author: Bohdan Mushkevych
"""
from subprocess import  PIPE
from bson.objectid import ObjectId
from datetime import datetime
import psutil
from psutil.error import TimeoutExpired
from model import unit_of_work_helper

from settings import settings
from model.unit_of_work_entry import UnitOfWorkEntry
from workers.abstract_worker import AbstractWorker
from system.performance_ticker import AggregatorPerformanceTicker


class AbstractHadoopWorker(AbstractWorker):
    """ Abstract class is inherited by all workers/aggregators
    that are aware of unit_of_work and capable of processing it"""

    def __init__(self, process_name):
        self.hadoop_process = None
        super(AbstractHadoopWorker, self).__init__(process_name)

    def __del__(self):
        super(AbstractHadoopWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = AggregatorPerformanceTicker(logger)
        self.performance_ticker.start()


    # **************** Process Supervisor Methods ************************
    def _start_process(self, start_timestamp, end_timestamp):
        try:
            self.logger.info('start: %s {' % self.process_name)
            p = psutil.Popen([settings['hadoop_command'],
                              'jar', settings['hadoop_jar'],
                              '-D', 'process.name=' + self.process_name,
                              '-D', 'timeperiod.working=' + str(start_timestamp),
                              '-D', 'timeperiod.next=' + str(end_timestamp)],
                               close_fds=True,
                               cwd=settings['process_cwd'],
                               stdin=PIPE,
                               stdout=PIPE,
                               stderr=PIPE)
            self.hadoop_process = p
            self.logger.info('Started %s with pid = %r' % (self.process_name, p.pid))
        except Exception:
            self.logger.error('Exception on starting: %s' % self.process_name, exc_info=True)
        finally:
            self.logger.info('}')


    def _poll_process(self):
        """ between death of a process and its actual termination lies poorly documented requirement -
            <purging process' io pipes and reading exit status>.
            this can be done either by os.wait() or process.wait()
            @return tuple (boolean: alive, int: return_code) """
        try:
            self.logger.warn(self.hadoop_process.stderr.read())
            self.logger.info(self.hadoop_process.stdout.read())
            returncode = self.hadoop_process.wait(timeout=0.01)
            if returncode is None:
                # process is already terminated
                self.logger.info('Process %s is terminated' % self.process_name)
            else:
                # process is terminated; possibly by OS
                self.logger.info('Process %s got terminated. Cleaning up' % self.process_name)
            self.hadoop_process = None
            return False, returncode
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
        unit_of_work = None
        try:
            # @param object_id: ObjectId of the unit_of_work from mq
            object_id = ObjectId(message.body)
            unit_of_work = unit_of_work_helper.retrieve_by_id(self.logger, object_id)
            if unit_of_work.get_state() == UnitOfWorkEntry.STATE_CANCELED \
                or unit_of_work.get_state() == UnitOfWorkEntry.STATE_PROCESSED:
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
            start_timestamp = unit_of_work.get_start_timestamp()
            end_timestamp = unit_of_work.get_end_timestamp()

            unit_of_work.set_state(UnitOfWorkEntry.STATE_IN_PROGRESS)
            unit_of_work.set_started_at(datetime.utcnow())
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.start_uow(unit_of_work)

            self._start_process(start_timestamp, end_timestamp)
            code = None
            alive = True
            while alive:
                alive, code = self._poll_process()

            if code == 0:
                unit_of_work.set_number_of_processed_documents(self.performance_ticker.posts_per_job)
                unit_of_work.set_finished_at(datetime.utcnow())
                unit_of_work.set_state(UnitOfWorkEntry.STATE_PROCESSED)
                self.performance_ticker.finish_uow()
            else:
                unit_of_work.set_state(UnitOfWorkEntry.STATE_INVALID)
                self.performance_ticker.cancel_uow()

            self.logger.info('Hadoop Map/Reduce return code is %r' % code)
            unit_of_work_helper.update(self.logger, unit_of_work)
        except Exception as e:
            unit_of_work.set_state(UnitOfWorkEntry.STATE_INVALID)
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.cancel_uow()
            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'\
                              % (message.body, unit_of_work.get_timestamp(), e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
