__author__ = 'Bohdan Mushkevych'

import time
import os

import psutil

from synergy.system.repeat_timer import RepeatTimer
from synergy.conf import settings


class FootprintCalculator(object):
    def __init__(self):
        self.pid = os.getpid()

    @property
    def document(self):
        ps = psutil.Process(self.pid)
        # '{:,}'.format(number) returns a string with coma as a thousand-separator
        return {'memory_rss': '{:,}'.format(ps.memory_info()[0]),
                'memory_vms': '{:,}'.format(ps.memory_info()[1]),
                'cpu_utilization': '{0:02.0f}'.format(ps.cpu_percent()),
                'mem_virtual_free': '{:,}'.format(psutil.virtual_memory().free),
                'mem_swap_free': '{:,}'.format(psutil.swap_memory().free)}

    def get_snapshot(self):
        resp = 'Footprint: RSS={memory_rss} VMS={memory_vms} CPU={cpu_utilization}; ' \
               'Available: PHYS={mem_virtual_free} VIRT={mem_swap_free}'.format(**self.document)
        return resp


class Tracker(object):
    def __init__(self, name):
        self.name = name
        self.per_24h = 0
        self.per_tick = 0

    def increment(self, delta=1):
        self.per_24h += delta
        self.per_tick += delta

    def reset_tick(self):
        self.per_tick = 0

    def reset_24h(self):
        self.per_24h = 0


class TrackerPair(object):
    def __init__(self, name, success='Success', failure='Failure'):
        self.name = name
        self.success = Tracker(success)
        self.failure = Tracker(failure)

    def increment_success(self, delta=1):
        self.success.increment(delta)

    def increment_failure(self, delta=1):
        self.failure.increment(delta)

    def reset_tick(self):
        self.success.reset_tick()
        self.failure.reset_tick()

    def reset_24h(self):
        self.success.reset_24h()
        self.failure.reset_24h()

    def to_string(self, tick_interval_seconds, show_header=True):
        header = self.name + ' : ' + self.success.name + '/' + self.failure.name + '.' if show_header else ''
        return header + 'In last {0:d} seconds: {1:d}/{2:d}. In last 24 hours: {3:d}/{4:d}'.format(
            tick_interval_seconds,
            self.success.per_tick,
            self.failure.per_tick,
            self.success.per_24h,
            self.failure.per_24h)


class TickerThread(object):
    SECONDS_IN_24_HOURS = 86400
    TICKS_BETWEEN_FOOTPRINTS = 10

    def __init__(self, logger):
        self.logger = logger
        self.trackers = dict()
        self.interval = settings.settings['perf_ticker_interval']
        self.mark_24_hours = time.time()
        self.mark_footprint = time.time()
        self.footprint = FootprintCalculator()
        self.timer = RepeatTimer(self.interval, self._run_tick_thread, daemonic=True)

    def add_tracker(self, tracker):
        self.trackers[tracker.name] = tracker

    def get_tracker(self, name):
        return self.trackers[name]

    def start(self):
        self.timer.start()

    def cancel(self):
        self.timer.cancel()

    def is_alive(self):
        return self.timer.is_alive()

    def _print_footprint(self):
        if time.time() - self.mark_footprint > self.TICKS_BETWEEN_FOOTPRINTS * self.interval:
            self.logger.info(self.footprint.get_snapshot())
            self.mark_footprint = time.time()

    def _run_tick_thread(self):
        self._print_footprint()

        current_time = time.time()
        do_24h_reset = current_time - self.mark_24_hours > self.SECONDS_IN_24_HOURS
        if do_24h_reset:
            self.mark_24_hours = current_time

        tracker_outputs = []
        for tracker_name, tracker in self.trackers.items():
            tracker_outputs.append(tracker.to_string(self.interval))
            tracker.reset_tick()
            if do_24h_reset:
                tracker.reset_24h()

        self.logger.info('\n'.join(tracker_outputs))


class SimpleTracker(TickerThread):
    TRACKER_PERFORMANCE = 'Performance'

    def __init__(self, logger):
        super(SimpleTracker, self).__init__(logger)
        self.add_tracker(TrackerPair(self.TRACKER_PERFORMANCE))

    @property
    def tracker(self):
        return self.get_tracker(self.TRACKER_PERFORMANCE)


class SessionPerformanceTracker(TickerThread):
    TRACKER_INSERT = 'Insert'
    TRACKER_UPDATE = 'Update'

    def __init__(self, logger):
        super(SessionPerformanceTracker, self).__init__(logger)
        self.add_tracker(TrackerPair(self.TRACKER_INSERT))
        self.add_tracker(TrackerPair(self.TRACKER_UPDATE))

    @property
    def insert(self):
        return self.get_tracker(self.TRACKER_INSERT)

    @property
    def update(self):
        return self.get_tracker(self.TRACKER_UPDATE)


class UowAwareTracker(SimpleTracker):
    STATE_IDLE = 'state_idle'
    STATE_PROCESSING = 'state_processing'

    def __init__(self, logger):
        super(UowAwareTracker, self).__init__(logger)
        self.state = self.STATE_IDLE
        self.success_per_job = 0
        self.failure_per_job = 0
        self.uow = None
        self.state_triggered_at = time.time()

    def _run_tick_thread(self):
        super(UowAwareTracker, self)._run_tick_thread()

        if self.state == self.STATE_PROCESSING:
            msg = 'State: {0} for {1:.2f} sec; Success/Failure {2}/{3} in this uow;'\
                  .format(self.state, time.time() - self.state_triggered_at, self.success_per_job, self.failure_per_job)
        else:
            msg = 'State: {0} for {1:.2f} sec;'.format(self.state, time.time() - self.state_triggered_at)
        self.logger.info(msg)

    def increment_success(self):
        self.tracker.increment_success()
        self.success_per_job += 1

    def increment_failure(self):
        self.tracker.increment_failure()
        self.failure_per_job += 1

    def start_uow(self, uow):
        self.state = self.STATE_PROCESSING
        self.uow = uow
        self.state_triggered_at = time.time()

    def finish_uow(self):
        self.logger.info('uow {0} at {1}: Success/Failure {2}/{3} entries in {4:.2f} seconds'.
                         format(self.uow.db_id, self.uow.timeperiod,
                                self.success_per_job, self.failure_per_job, time.time() - self.state_triggered_at))
        self.cancel_uow()

    def cancel_uow(self):
        self.state = self.STATE_IDLE
        self.uow = None
        self.state_triggered_at = time.time()
        self.success_per_job = 0
