"""
Created on 2011-02-10

@author: Bohdan Mushkevych
"""
import time, os, psutil

from system.repeat_timer import RepeatTimer
from settings import settings

class FootprintCalculator(object):
    def __init__(self):
        self.pid = os.getpid()

    def group(self, number):
        """ method formats number and inserts thousands separators """
        s = '%d' % number
        groups = []
        while s and s[-1].isdigit():
            groups.append(s[-3:])
            s = s[:-3]
        return s + '\''.join(reversed(groups))

    def get_snapshot_as_list(self):
        ps = psutil.Process(self.pid)
        return (self.group(ps.get_memory_info()[0]),
                self.group(ps.get_memory_info()[1]),
                '%02d' % ps.get_cpu_percent(),
                self.group(psutil.phymem_usage().free),
                self.group(psutil.virtmem_usage().free))

    def get_snapshot(self):
        resp = 'Footprint: RSS=%r VMS=%r CPU=%r; Available: PHYS=%r VIRT=%r' % self.get_snapshot_as_list()
        return resp


class WorkerPerformanceTicker(object):
    SECONDS_IN_24_HOURS = 86400
    TICKS_BETWEEN_FOOTPRINTS = 10

    def __init__(self, logger):
        self.logger = logger
        self.posts_per_24_hours = 0
        self.posts_per_tick = 0
        self.interval = settings['perf_ticker_interval']
        self.mark_24_hours = time.time()
        self.mark_footprint = time.time()
        self.footprint = FootprintCalculator()
        
    def start(self):
        self.timer = RepeatTimer(self.interval, self._run_tick_thread)
        self.timer.start()
    
    def cancel(self):
        self.timer.cancel()

    def _print_footprint(self):
        if time.time() - self.mark_footprint > self.TICKS_BETWEEN_FOOTPRINTS * self.interval:
            self.logger.info(self.footprint.get_snapshot())
            self.mark_footprint = time.time()

    def _run_tick_thread(self):
        self._print_footprint()
        self.logger.info('Processed: %d in last %d seconds; %d in last 24 hours;' \
                    % (self.posts_per_tick,
                       self.interval,
                       self.posts_per_24_hours))

        self.posts_per_tick = 0
        if time.time() - self.mark_24_hours > self.SECONDS_IN_24_HOURS:
            self.mark_24_hours = time.time()
            self.posts_per_24_hours = 0

    def increment(self):
        self.posts_per_tick += 1
        self.posts_per_24_hours += 1


class SessionPerformanceTicker(WorkerPerformanceTicker):

    def __init__(self, logger):
        super(SessionPerformanceTicker, self).__init__(logger)
        self.updates_per_24_hours = 0
        self.updates_per_tick = 0

    def _run_tick_thread(self):
        self._print_footprint()
        self.logger.info('Inserts/Updates. In last {0:d} seconds: {1:d}/{2:d}. In last 24 hours: {3:d}/{4:d};' \
                        .format(self.interval,
                                self.posts_per_tick,
                                self.updates_per_tick,
                                self.posts_per_24_hours,
                                self.updates_per_24_hours))

        self.posts_per_tick = 0
        self.updates_per_tick = 0
        if time.time() - self.mark_24_hours > self.SECONDS_IN_24_HOURS:
            self.mark_24_hours = time.time()
            self.posts_per_24_hours = 0
            self.updates_per_24_hours = 0

    def increment_insert(self):
        super(SessionPerformanceTicker, self).increment()

    def increment_update(self):
        self.updates_per_tick += 1
        self.updates_per_24_hours += 1


class AggregatorPerformanceTicker(WorkerPerformanceTicker):
    STATE_IDLE = 'state_idle'
    STATE_PROCESSING = 'state_processing'

    def __init__(self, logger):
        super(AggregatorPerformanceTicker, self).__init__(logger)
        self.state = self.STATE_IDLE
        self.posts_per_job = 0
        self.uow_obj = None
        self.state_triggered_at = time.time()
        
    def _run_tick_thread(self):
        self._print_footprint()
        if self.state == self.STATE_PROCESSING or self.posts_per_tick > 0 or self.posts_per_24_hours > 0:
            msg = 'State: %s for %d sec; processed: %d in %d sec. %d in this uow; %d in 24 hours;' \
                    % (self.state,
                       time.time() - self.state_triggered_at,
                       self.posts_per_tick,
                       self.interval,
                       self.posts_per_job,
                       self.posts_per_24_hours,)
        else:
            msg = 'State: %s for %d sec;' % (self.state, time.time() - self.state_triggered_at)
        self.logger.info(msg)

        self.posts_per_tick = 0
        if time.time() - self.mark_24_hours > self.SECONDS_IN_24_HOURS:
            self.mark_24_hours = time.time()
            self.posts_per_24_hours = 0

    def increment(self):
        self.posts_per_tick += 1
        self.posts_per_24_hours += 1
        self.posts_per_job += 1

    def start_uow(self, uow_obj):
        self.state = self.STATE_PROCESSING
        self.uow_obj = uow_obj
        self.state_triggered_at = time.time()

    def finish_uow(self):
        _id = self.uow_obj.get_document()['_id']
        self.logger.info('Success: unit_of_work %s in timeperiod %s; processed %d entries in %d seconds' \
                    % (_id,
                       self.uow_obj.get_timestamp(),
                       self.posts_per_job,
                       time.time() - self.state_triggered_at))
        self.cancel_uow()

    def cancel_uow(self):
        self.state = self.STATE_IDLE
        self.uow_obj = None
        self.state_triggered_at = time.time()
        self.posts_per_job = 0
