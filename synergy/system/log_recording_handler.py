__author__ = 'Bohdan Mushkevych'

import logging
from datetime import datetime

from synergy.db.model.log_recording import LogRecording
from synergy.db.dao.log_recording_dao import LogRecordingDao


class LogRecordingHandler(logging.Handler):
    def __init__(self, logger, parent_object_id):
        super(LogRecordingHandler, self).__init__()
        self.logger = logger
        self.parent_object_id = parent_object_id
        self.log_recording_dao = LogRecordingDao(logger)

    def attach(self):
        """ method clears existing log_recorder entries for given parent_object_id,
            creates a new one and attaches this handler to the logger
            from this moment every log record will be recorded in the DB """
        log_recording = LogRecording(parent_object_id=self.parent_object_id, created_at=datetime.utcnow())
        self.log_recording_dao.remove(self.parent_object_id)
        self.log_recording_dao.update(log_recording)

        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        self.setFormatter(formatter)
        self.logger.addHandler(self)

    def detach(self):
        """ method detaches this handler from the logger """
        self.logger.removeHandler(self)

    def emit(self, record):
        msg = self.format(record)
        try:
            self.log_recording_dao.append_log(self.parent_object_id, msg.rstrip())
        except Exception as e:
            self.detach()
            self.logger.error(f'Detached LogRecordingHandler. Exception on LogRecordingDao.append_log: {e}',
                              exc_info=True)
