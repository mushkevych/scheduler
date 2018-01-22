__author__ = 'Bohdan Mushkevych'

from synergy.db.error import DuplicateKeyError
from synergy.db.model.job import Job
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_HOURLY
from tests.base_fixtures import create_unit_of_work

TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
TEST_PAST_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD, delta=-1)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def then_raise_uw(*_):
    """mocks AbstractStateMachine._insert_uow and AbstractStateMachine.insert_and_publish_uow"""
    raise UserWarning('Simulated UserWarning Exception')


def mock_insert_uow_return_uow(process_name, timeperiod, start_timeperiod, end_timeperiod, start_id, end_id):
    """mocks AbstractStateMachine._insert_uow"""
    return create_unit_of_work(process_name, start_id, end_id, timeperiod, uow_id='a_uow_id')


def mock_insert_uow_raise_dpk(process_name, timeperiod, start_timeperiod, end_timeperiod, start_id, end_id):
    """mocks AbstractStateMachine._insert_uow"""
    raise DuplicateKeyError(process_name, timeperiod, start_id, end_id, 'Simulated Exception')


def then_raise_dpk(job_record, start_id, end_id):
    """mocks AbstractStateMachine.insert_and_publish_uow"""
    raise DuplicateKeyError(job_record.process_name, job_record.timeperiod, start_id, end_id, 'Simulated Exception')


def then_return_uow(job_record, start_id, end_id):
    """mocks AbstractStateMachine.insert_and_publish_uow"""
    return create_unit_of_work(job_record.process_name, start_id, end_id, job_record.timeperiod, uow_id='a_uow_id'), \
           False


def then_return_duplicate_uow(job_record, start_id, end_id):
    """mocks AbstractStateMachine.insert_and_publish_uow"""
    return create_unit_of_work(job_record.process_name, start_id, end_id, job_record.timeperiod, uow_id='a_uow_id'), \
           True


def get_job_record(state, timeperiod, process_name):
    return Job(process_name=process_name,
               timeperiod=timeperiod,
               state=state,
               db_id='000000000000000123456789'
               )
