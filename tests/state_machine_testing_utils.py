__author__ = 'Bohdan Mushkevych'

from tests.base_fixtures import create_unit_of_work
from synergy.db.error import DuplicateKeyError
from synergy.db.model.job import Job
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_HOURLY


TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
TEST_PAST_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD, delta=-1)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def then_raise_uw(*_):
    raise UserWarning('Simulated UserWarning Exception')


def then_raise_dpk(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    raise DuplicateKeyError(process_name, start_timeperiod, start_id, end_id, 'Simulated Exception')


def then_return_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    return create_unit_of_work(process_name, start_id, end_id, start_timeperiod, uow_id='a_uow_id'), False


def then_return_duplicate_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    return create_unit_of_work(process_name, start_id, end_id, start_timeperiod, uow_id='a_uow_id'), True


def get_job_record(state, timeperiod, process_name):
    job_record = Job()
    job_record.state = state
    job_record.timeperiod = timeperiod
    job_record.process_name = process_name
    job_record.db_id = 'alpha_id'
    return job_record
