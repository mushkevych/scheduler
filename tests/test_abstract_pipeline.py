__author__ = 'Bohdan Mushkevych'

import unittest

from settings import enable_test_mode
enable_test_mode()

from mockito import spy, mock, when
from mockito.matchers import any

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.error import DuplicateKeyError
from synergy.db.model import job
from synergy.db.model.job import Job
from tests.base_fixtures import create_unit_of_work
from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.conf.process_context import ProcessContext
from constants import PROCESS_SITE_HOURLY
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.abstract_pipeline import AbstractPipeline
from tests.ut_context import PROCESS_UNIT_TEST


TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def then_raise(process_name, start_timeperiod, end_timeperiod, start_id, end_id, job_record):
    exc = DuplicateKeyError(process_name,
                            start_timeperiod,
                            start_id,
                            end_id,
                            'Simulated Exception')
    raise exc


def then_return_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id, job_record):
    return create_unit_of_work(process_name, start_id, end_id, start_timeperiod, uow_id='a_uow_id')


def get_job_record(state, timeperiod, process_name):
    job_record = Job()
    job_record.state = state
    job_record.timeperiod = timeperiod
    job_record.process_name = process_name
    job_record.document['_id'] = 'alpha_id'
    return job_record


class ContinuousPipelineUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock(Timetable)
        self.uow_dao_mocked = mock(UnitOfWorkDao)
        self.pipeline_real = AbstractPipeline(self.logger, self.time_table_mocked, 'AbstractPipeline')
        self.pipeline_real.uow_dao = self.uow_dao_mocked

    def tearDown(self):
        pass

    def test_insert_and_publish_uow(self):
        """ method tests happy-flow for insert_and_publish_uow method """
        self.pipeline_real._insert_uow = then_return_uow
        when(self.pipeline_real)._publish_uow(any(object), any(object)).thenReturn(True)

        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)

        pipeline = spy(self.pipeline_real)
        uow, is_duplicate = pipeline.insert_and_publish_uow(PROCESS_SITE_HOURLY,
                                                            TEST_PRESET_TIMEPERIOD,
                                                            None, 0, 1,
                                                            job_record)
        manual_uow = then_return_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1, job_record)
        self.assertFalse(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)

    def test_unhandled_exception_iapu(self):
        """ method tests unhandled UserWarning exception at insert_and_publish_uow method """
        self.pipeline_real._insert_uow = then_raise
        when(self.pipeline_real)._publish_uow(any(object), any(object)).thenReturn(True)

        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        pipeline = spy(self.pipeline_real)

        try:
            pipeline.insert_and_publish_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1, job_record)
            self.assertTrue(False, 'UserWarning exception should have been thrown')
        except UserWarning:
            self.assertTrue(True)

    def test_handled_exception_iapu(self):
        """ method tests handled UserWarning exception at insert_and_publish_uow method """
        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        manual_uow = then_return_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1, job_record)

        self.pipeline_real._insert_uow = then_raise
        when(self.pipeline_real)._publish_uow(any(object), any(object)).thenReturn(True)
        when(self.uow_dao_mocked).recover_from_duplicatekeyerror(any(object)).thenReturn(manual_uow)

        pipeline = spy(self.pipeline_real)
        uow, is_duplicate = pipeline.insert_and_publish_uow(PROCESS_SITE_HOURLY,
                                                            TEST_PRESET_TIMEPERIOD,
                                                            None, 0, 1,
                                                            job_record)
        self.assertTrue(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)


if __name__ == '__main__':
    unittest.main()
