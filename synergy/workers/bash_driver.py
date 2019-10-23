__author__ = 'Bohdan Mushkevych'

import os
import time
import threading
from invoke import Context
from fabric2 import Connection
from datetime import datetime

from synergy.conf import settings
from synergy.workers.abstract_mq_worker import AbstractMqWorker
from synergy.workers.worker_constants import *
from synergy.db.model import unit_of_work
from synergy.db.model.mq_transmission import MqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao


class BashRunnable(threading.Thread):
    """Process starts remote or local bash script job, supervises its execution and updates mq"""

    def __init__(self, logger, message, consumer, performance_tracker):
        self.logger = logger
        self.message = message
        self.mq_request = MqTransmission.from_json(message.body)
        self.consumer = consumer
        self.performance_tracker = performance_tracker
        self.alive = False
        self.return_code = -1
        self.uow_dao = UnitOfWorkDao(self.logger)

        self.thread_name = str(self.mq_request)
        super(BashRunnable, self).__init__(name=self.thread_name)

    def _poll_process(self):
        return self.alive, self.return_code

    def _start_process(self):
        try:
            uow = self.uow_dao.get_one(self.mq_request.record_db_id)
            if not uow.is_requested:
                # accept only UOW in STATE_REQUESTED
                self.logger.warning(f'Skipping UOW: id {self.message.body}; state {uow.state};', exc_info=False)
                self.consumer.acknowledge(self.message.delivery_tag)
                return
        except Exception:
            self.logger.error(f'Safety fuse. Can not identify UOW {self.message.body}', exc_info=True)
            self.consumer.acknowledge(self.message.delivery_tag)
            return

        try:
            self.logger.info(f'start: {self.thread_name} {{')
            self.alive = True

            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            self.uow_dao.update(uow)

            command = os.path.join(uow.arguments[ARGUMENT_CMD_PATH],
                                   uow.arguments[ARGUMENT_CMD_FILE])
            command += ' {0}'.format(uow.arguments[ARGUMENT_CMD_ARGS])

            # Fabric1
            # fabric.operations.env.warn_only = True
            # fabric.operations.env.abort_on_prompts = True   # removed
            # fabric.operations.env.use_ssh_config = True     # True by default
            # fabric.operations.env.host_string = uow.arguments[ARGUMENT_CMD_HOST]
            # run_result = fabric.operations.run(command, pty=False)

            if uow.arguments[ARGUMENT_CMD_HOST]:
                runner = Connection(host=uow.arguments[ARGUMENT_CMD_HOST])
            else:
                runner = Context()

            run_result = runner.run(command, warn=True, pty=False)
            if run_result.succeeded:
                self.return_code = 0

            uow.finished_at = datetime.utcnow()
            uow.state = unit_of_work.STATE_PROCESSED
            self.uow_dao.update(uow)

            self.logger.info(f'Completed {self.thread_name} with result = {self.return_code}')
        except Exception:
            self.logger.error(f'Exception on starting: {self.thread_name}', exc_info=True)
            uow.state = unit_of_work.STATE_INVALID
            self.uow_dao.update(uow)
        finally:
            self.logger.info('}')
            self.alive = False

    def run(self):
        try:
            self._start_process()
            code = None
            alive = True
            while alive:
                alive, code = self._poll_process()
                time.sleep(0.1)

            if code == 0:
                self.performance_tracker.tracker.increment_success()
            else:
                self.performance_tracker.tracker.increment_failure()

            self.logger.info(f'BashDriver for {self.thread_name} return code is {code}')
        except Exception as e:
            self.performance_tracker.tracker.increment_failure()
            self.logger.error(f'Safety fuse while processing request {self.message.body}: {e}', exc_info=True)
        finally:
            self.consumer.acknowledge(self.message.delivery_tag)


class BashDriver(AbstractMqWorker):
    """Process facilitates threads running local or remote bash scripts"""

    def __init__(self, process_name):
        super(BashDriver, self).__init__(process_name)
        self.initial_thread_count = threading.active_count()

    def _mq_callback(self, message):
        """ reads JSON request from the mq message and delivers it for processing """
        while threading.active_count() > settings.settings['bash_runnable_count'] + self.initial_thread_count:
            time.sleep(0.1)

        t = BashRunnable(self.logger, message, self.consumer, self.performance_tracker)
        t.daemon = True
        t.start()


if __name__ == '__main__':
    source = BashDriver(PROCESS_BASH_DRIVER)
    source.start()
