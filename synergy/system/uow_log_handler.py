__author__ = 'Bohdan Mushkevych'

import logging
from datetime import datetime

from synergy.db.model.uow_log_entry import UowLogEntry
from synergy.db.dao.uow_log_dao import UowLogDao


class UowLogHandler(logging.Handler):
    def __init__(self, logger, uow_id):
        super(UowLogHandler, self).__init__()
        self.logger = logger
        self.uow_id = uow_id
        self.uow_log_dao = UowLogDao(logger)

    def attach(self):
        """ method attaches this handler to the logger
            from this moment every log record will be recorded in the DB """
        uow_log = UowLogEntry(related_unit_of_work=self.uow_id, created_at=datetime.utc_now())
        self.uow_log_dao.remove(self.uow_id)
        self.uow_log_dao.update(uow_log)
        self.logger.addHandler(self)

    def detach(self):
        """ method detaches this handler from the logger """
        self.logger.removeHandler(self)

    def emit(self, record):
        self.uow_log_dao.append_log(self.uow_id, record)
