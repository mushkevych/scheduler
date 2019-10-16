__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import INFO, WARNING, ERROR

from synergy.db.model import job
from synergy.db.model.job import Job
from synergy.db.error import DuplicateKeyError
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.job_dao import JobDao
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.system.mq_transmitter import MqTransmitter
from synergy.conf import context
from synergy.system.decorator import with_reconnect
from synergy.system import time_helper
from synergy.scheduler.tree_node import NodesCompositeState


class AbstractStateMachine(object):
    """ Abstract state machine used to host common logic for the rest of registered State Machines """

    def __init__(self, logger, timetable, name):
        self.name = name
        self.logger = logger
        self.mq_transmitter = MqTransmitter(self.logger)
        self.timetable = timetable
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.job_dao = JobDao(self.logger)

    @property
    def run_on_active_timeperiod(self):
        """
        :return: True if given State Machine allows execution on *live* timeperiod, as opposed to the finished one
        """
        raise NotImplementedError(f'property run_on_active_timeperiod must be implemented by {self.__class__.__name__}')

    def _log_message(self, level, process_name, timeperiod, msg):
        """ method performs logging into log file and Timetable's tree node"""
        self.timetable.add_log_entry(process_name, timeperiod, msg)
        self.logger.log(level, msg)

    @with_reconnect
    def _insert_uow(self, process_name, timeperiod, start_timeperiod, end_timeperiod, start_id, end_id):
        """creates unit_of_work and inserts it into the DB
            :raise DuplicateKeyError: if unit_of_work with given parameters already exists """
        uow = UnitOfWork()
        uow.process_name = process_name
        uow.timeperiod = timeperiod
        uow.start_id = str(start_id)
        uow.end_id = str(end_id)
        uow.start_timeperiod = start_timeperiod
        uow.end_timeperiod = end_timeperiod
        uow.created_at = datetime.utcnow()
        uow.submitted_at = datetime.utcnow()
        uow.source = context.process_context[process_name].source
        uow.sink = context.process_context[process_name].sink
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = unit_of_work.TYPE_MANAGED
        uow.number_of_retries = 0
        uow.arguments = context.process_context[process_name].arguments
        uow.db_id = self.uow_dao.insert(uow)

        msg = 'Created: UOW {0} for {1}@{2} over [{3}:{4}).'\
            .format(uow.db_id, process_name, timeperiod, start_timeperiod, end_timeperiod)
        self._log_message(INFO, process_name, timeperiod, msg)
        return uow

    def _publish_uow(self, uow):
        self.mq_transmitter.publish_managed_uow(uow)
        msg = 'Published: UOW {0} for {1}@{2} over [{3}:{4}).'\
            .format(uow.db_id, uow.process_name, uow.timeperiod, uow.start_timeperiod, uow.end_timeperiod)
        self._log_message(INFO, uow.process_name, uow.timeperiod, msg)

    def insert_and_publish_uow(self, job_record, start_id, end_id):
        """ method creates and publishes a unit_of_work. it also handles DuplicateKeyError and attempts recovery
        :return: tuple (uow, is_duplicate)
        :raise UserWarning: if the recovery from DuplicateKeyError was unsuccessful
        """
        process_name = job_record.process_name
        timeperiod = job_record.timeperiod
        start_timeperiod = self.compute_start_timeperiod(job_record.process_name, job_record.timeperiod)
        end_timeperiod = self.compute_end_timeperiod(job_record.process_name, job_record.timeperiod)

        try:
            is_duplicate = False
            uow = self._insert_uow(process_name, timeperiod, start_timeperiod, end_timeperiod, start_id, end_id)
        except DuplicateKeyError as e:
            is_duplicate = True
            msg = 'Catching up with latest UOW {0}@{1} over [{2}:{3}), because of: {4}' \
                  .format(process_name, timeperiod, start_timeperiod, end_timeperiod, e)
            self._log_message(WARNING, process_name, timeperiod, msg)
            uow = self.uow_dao.recover_from_duplicatekeyerror(e)

        if not uow:
            msg = 'PERSISTENT TIER ERROR! Unable to locate UOW for {0}@{1} over [{2}:{3})' \
                  .format(process_name, timeperiod, start_timeperiod, end_timeperiod)
            self._log_message(WARNING, process_name, timeperiod, msg)
            raise UserWarning(msg)

        if uow.is_canceled:
            # this UOW was marked for re-processing. recycle it
            uow.created_at = datetime.utcnow()      # reset created_at to bypass GC cancellation logic
            uow.submitted_at = datetime.utcnow()    # reset submitted_at to allow 1 hour free of GC resubmitting
            del uow.started_at
            del uow.finished_at
            del uow.number_of_aggregated_documents
            del uow.number_of_processed_documents
            uow.state = unit_of_work.STATE_REQUESTED
            self.uow_dao.update(uow)

        # publish the created/recovered/recycled unit_of_work
        self._publish_uow(uow)
        return uow, is_duplicate

    def notify(self, uow):
        """ method is used by StateMachine's users to provide notifications about change in UOW
        if applicable, method will update job_record state and Timetable tree node state
        :assumptions: uow is in [STATE_NOOP, STATE_CANCELED, STATE_PROCESSED] """
        pass

    def compute_start_timeperiod(self, process_name, timeperiod):
        """ computes lowest *inclusive* timeperiod boundary for job to process
            for process with time_grouping == 1, it returns given timeperiod with no change
            for process with time_grouping != 1, it computes first timeperiod, not processed by the previous job run
            For instance: with time_grouping = 3, QUALIFIER_HOURLY, and timeperiod = 2016042018,
            the start_timeperiod will be = 2016042016 (computed as 2016042018 - 3 + 1)
        """
        time_grouping = context.process_context[process_name].time_grouping
        if time_grouping == 1:
            return timeperiod

        # step1: translate given timeperiod to the time grouped one
        process_hierarchy = self.timetable.get_tree(process_name).process_hierarchy
        timeperiod_dict = process_hierarchy[process_name].timeperiod_dict
        translated_timeperiod = timeperiod_dict._translate_timeperiod(timeperiod)

        # step 2: compute previous grouped period
        # NOTICE: simple `time_helper.increment_timeperiod(time_qualifier, timeperiod)` is insufficient
        #         as it does not address edge cases, such as the last day of the month or the last hour of the day
        # For instance: with time_grouping=3, QUALIFIER_DAILY, and 2016123100
        # the `increment_timeperiod` will yield 2016122800 instead of 2016123100
        time_qualifier = context.process_context[process_name].time_qualifier
        for i in range(1, time_grouping + 1):
            prev_timeperiod = time_helper.increment_timeperiod(time_qualifier, translated_timeperiod, delta=-i)
            if prev_timeperiod == timeperiod_dict._translate_timeperiod(prev_timeperiod):
                # prev_timeperiod is currently at the last grouped timeperiod
                break

        # step 3: compute first exclusive timeperiod after the *prev_timeperiod*,
        # which becomes first inclusive timeperiod for this job run
        over_the_edge_timeperiod = time_helper.increment_timeperiod(time_qualifier, prev_timeperiod, delta=-1)
        if prev_timeperiod != timeperiod_dict._translate_timeperiod(over_the_edge_timeperiod):
            # over_the_edge_timeperiod fell into previous day or month or year
            # *prev_timeperiod* points to the first month, first day of the month or 00 hour
            start_timeperiod = prev_timeperiod
        else:
            start_timeperiod = self.compute_end_timeperiod(process_name, prev_timeperiod)

        return start_timeperiod

    def compute_end_timeperiod(self, process_name, timeperiod):
        """ computes first *exclusive* timeperiod for job to process """
        time_qualifier = context.process_context[process_name].time_qualifier
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, timeperiod)
        return end_timeperiod

    def _is_noop_timeperiod(self, process_name, timeperiod):
        """ method verifies if the given timeperiod for given process is valid or falls in-between grouping checkpoints
        :param process_name: name of the process
        :param timeperiod: timeperiod to verify
        :return: False, if given process has no time_grouping set or it is equal to 1.
                 False, if time_grouping is custom but the given timeperiod matches the grouped timeperiod.
                 True, if the timeperiod falls in-between grouping cracks
        """
        time_grouping = context.process_context[process_name].time_grouping
        if time_grouping == 1:
            return False

        process_hierarchy = self.timetable.get_tree(process_name).process_hierarchy
        timeperiod_dict = process_hierarchy[process_name].timeperiod_dict
        return timeperiod_dict._translate_timeperiod(timeperiod) != timeperiod

    def _process_noop_timeperiod(self, job_record):
        """ method is valid for processes having time_grouping != 1.
            should a job record fall in-between grouped time milestones,
            its state should be set to STATE_NOOP without any processing """
        job_record.state = job.STATE_NOOP
        self.job_dao.update(job_record)

        time_grouping = context.process_context[job_record.process_name].time_grouping
        msg = 'Job {0}@{1} with time_grouping {2} was transferred to STATE_NOOP' \
              .format(job_record.process_name, job_record.timeperiod, time_grouping)
        self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

    def _process_state_embryo(self, job_record):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        pass

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        pass

    def _process_state_final_run(self, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)
        if uow.is_processed:
            self.update_job(job_record, uow, job.STATE_PROCESSED)
        elif uow.is_noop:
            self.update_job(job_record, uow, job.STATE_NOOP)
        elif uow.is_canceled:
            self.update_job(job_record, uow, job.STATE_SKIPPED)
        elif uow.is_invalid:
            msg = 'Job {0}: UOW for {1}@{2} is in {3}; ' \
                  'relying on the Garbage Collector to either recycle or cancel the UOW.' \
                  .format(job_record.db_id, job_record.process_name, job_record.timeperiod, uow.state)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)
        else:
            msg = 'Suppressed creating UOW for {0}@{1}; Job is in {2}; uow is in {3}' \
                  .format(job_record.process_name, job_record.timeperiod, job_record.state, uow.state)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

        timetable_tree = self.timetable.get_tree(job_record.process_name)
        timetable_tree.build_tree()

    def _process_terminal_state(self, job_record):
        """ method logs a warning message notifying that the job is no longer govern by this state machine """
        msg = 'Job {0} for {1}@{2} is in the terminal state {3}, ' \
              'and is no further govern by the State Machine {4}' \
              .format(job_record.db_id, job_record.process_name, job_record.timeperiod, job_record.state, self.name)
        self._log_message(WARNING, job_record.process_name, job_record.timeperiod, msg)

    def manage_job_with_blocking_children(self, job_record):
        """ method will trigger job processing only if:
            - all children are finished (STATE_PROCESSED, STATE_SKIPPED, STATE_NOOP)
            - all external dependencies are finalized (STATE_PROCESSED, STATE_SKIPPED, STATE_NOOP) """
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)
        if is_job_finalizable:
            self.manage_job(job_record)
        else:
            msg = 'Job {0}@{1} is blocked by unprocessed children/dependencies. Waiting another tick' \
                  .format(job_record.process_name, job_record.timeperiod)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

    def manage_job_with_blocking_dependencies(self, job_record):
        """ method will trigger job processing only if:
            - all dependencies are in [STATE_PROCESSED, STATE_NOOP]
            NOTICE: method will transfer current job into STATE_SKIPPED if any dependency is in STATE_SKIPPED """
        composite_state = self.timetable.dependent_on_composite_state(job_record)
        assert isinstance(composite_state, NodesCompositeState)

        if composite_state.all_processed:
            self.manage_job(job_record)
        elif composite_state.skipped_present:
            # As soon as among <dependent on> periods are in STATE_SKIPPED
            # there is very little sense in waiting for them to become STATE_PROCESSED
            # Skip this timeperiod itself
            job_record.state = job.STATE_SKIPPED
            self.job_dao.update(job_record)
            self.mq_transmitter.publish_job_status(job_record)

            msg = 'Job {0}@{1} is blocked by STATE_SKIPPED dependencies. ' \
                  'Transferred the job to STATE_SKIPPED'.format(job_record.process_name, job_record.timeperiod)
            self._log_message(WARNING, job_record.process_name, job_record.timeperiod, msg)
        else:
            msg = 'Job {0}@{1} is blocked by unprocessed dependencies. Waiting another tick' \
                  .format(job_record.process_name, job_record.timeperiod)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

    def manage_job(self, job_record):
        """ method main duty - is to _avoid_ publishing another unit_of_work, if previous was not yet processed
            In case the State Machine sees that the unit_of_work already exist it could either:
            - update boundaries of the processing; republish
            - wait another tick; idle """
        assert isinstance(job_record, Job)
        try:
            if self._is_noop_timeperiod(job_record.process_name, job_record.timeperiod):
                self._process_noop_timeperiod(job_record)
                return

            if job_record.is_embryo:
                self._process_state_embryo(job_record)

            elif job_record.is_in_progress:
                self._process_state_in_progress(job_record)

            elif job_record.is_final_run:
                self._process_state_final_run(job_record)

            elif job_record.is_skipped:
                self._process_terminal_state(job_record)

            elif job_record.is_processed:
                self._process_terminal_state(job_record)

            elif job_record.is_noop:
                self._process_terminal_state(job_record)

            else:
                msg = 'Unknown state {0} of the job {1}'.format(job_record.state, job_record.db_id)
                self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)

        except LookupError as e:
            job_record.number_of_failures += 1
            self.job_dao.update(job_record)
            self.timetable.skip_if_needed(job_record)
            msg = 'Increasing fail counter for Job {0}@{1}, because of: {2}' \
                  .format(job_record.process_name, job_record.timeperiod, e)
            self._log_message(WARNING, job_record.process_name, job_record.timeperiod, msg)

        finally:
            self.mq_transmitter.publish_job_status(job_record)

    def reprocess_job(self, job_record):
        """ method marks given job for reprocessing:
            - if no UOW was bind with the job record - it is transferred to STATE_EMBRYO only
            - if related UOW was not finished - we mark the UOW as STATE_CANCELED
                and pass the job record thru the _process_in_progress routine
            - otherwise job record is passed thru the _process_in_progress routine """
        original_job_state = job_record.state
        job_record.number_of_failures = 0

        if not job_record.related_unit_of_work:
            job_record.state = job.STATE_EMBRYO
            self.job_dao.update(job_record)
        else:
            uow = self.uow_dao.get_one(job_record.related_unit_of_work)
            if not uow.is_finished:
                uow.state = unit_of_work.STATE_CANCELED
                uow.submitted_at = datetime.utcnow()
                self.uow_dao.update(uow)
            self._process_state_in_progress(job_record)

        msg = 'Reprocessed Job {0} for {1}@{2}: state transfer {3} -> {4};' \
              .format(job_record.db_id, job_record.process_name, job_record.timeperiod,
                      original_job_state, job_record.state)
        self._log_message(WARNING, job_record.process_name, job_record.timeperiod, msg)

    def skip_job(self, job_record):
        """ method transfers:
            - given job into STATE_SKIPPED if it is not not in finished state
            - UOW into STATE_CANCELED if it is not in finished state """
        original_job_state = job_record.state

        if not job_record.is_finished:
            job_record.state = job.STATE_SKIPPED
            self.job_dao.update(job_record)

        if job_record.related_unit_of_work:
            uow = self.uow_dao.get_one(job_record.related_unit_of_work)
            if not uow.is_finished:
                uow.state = unit_of_work.STATE_CANCELED
                uow.submitted_at = datetime.utcnow()
                self.uow_dao.update(uow)

        msg = 'Skipped Job {0} for {1}@{2}: state transfer {3} -> {4};' \
              .format(job_record.db_id, job_record.process_name, job_record.timeperiod,
                      original_job_state, job_record.state)
        self._log_message(WARNING, job_record.process_name, job_record.timeperiod, msg)

    def create_job(self, process_name, timeperiod):
        """ method creates a job record in STATE_EMBRYO for given process_name and timeperiod
            :returns: created job record of type <Job>"""
        job_record = Job()
        job_record.state = job.STATE_EMBRYO
        job_record.timeperiod = timeperiod
        job_record.process_name = process_name
        self.job_dao.update(job_record)

        self.logger.info('Created Job {0} for {1}@{2}'
                         .format(job_record.db_id, job_record.process_name, job_record.timeperiod))
        return job_record

    def update_job(self, job_record, uow, new_state):
        """ method updates job record with a new unit_of_work and new state"""
        original_job_state = job_record.state
        job_record.state = new_state
        job_record.related_unit_of_work = uow.db_id
        self.job_dao.update(job_record)

        msg = 'Updated Job {0} for {1}@{2}: state transfer {3} -> {4};' \
              .format(job_record.db_id, job_record.process_name, job_record.timeperiod, original_job_state, new_state)
        self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)
