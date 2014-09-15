__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from system.repeat_timer import RepeatTimer

TRIGGER_INTERVAL = 60  # 1 minute
EVERY_DAY = '*'        # marks every day as suitable to trigger the event
TIME_OF_DAY_FORMAT = "%H:%M"
TRIGGER_PREAMBLE_AT = 'at '
TRIGGER_PREAMBLE_EVERY = 'every '


def parse_time_trigger_string(trigger_time):
    """
    :param trigger_time: human-readable and editable string in one of two formats:
     - 'at Day_of_Week-HH:MM, ..., Day_of_Week-HH:MM'
     - 'every NNN'
    :return: return tuple (parsed_trigger_time, timer_klass)
    """
    # replace multiple spaces with one
    trigger_time = ' '.join(trigger_time.split())

    if trigger_time.startswith(TRIGGER_PREAMBLE_AT):
        # EventClock block
        trigger_time = trigger_time[len(TRIGGER_PREAMBLE_AT):]
        parsed_trigger_time = trigger_time.replace(' ', '').replace(',', ' ').split(' ')
        timer_klass = EventClock
    elif trigger_time.startswith(TRIGGER_PREAMBLE_EVERY):
        # RepeatTimer block
        trigger_time = trigger_time[len(TRIGGER_PREAMBLE_EVERY):]
        parsed_trigger_time = int(trigger_time)
        timer_klass = RepeatTimer
    else:
        raise ValueError('Unknown time trigger format %s' % trigger_time)

    return parsed_trigger_time, timer_klass


def format_time_trigger_string(timer_instance):
    """
    :param timer_instance: either instance of RepeatTimer or EventClock
    :return: human-readable and editable string in one of two formats:
     - 'at Day_of_Week-HH:MM, ..., Day_of_Week-HH:MM'
     - 'every NNN'
    """
    if isinstance(timer_instance, RepeatTimer):
        return TRIGGER_PREAMBLE_EVERY + str(timer_instance.interval_new)
    elif isinstance(timer_instance, EventClock):
        timestamps = [repr(x) for x in timer_instance.timestamps]
        return TRIGGER_PREAMBLE_AT + ','.join(timestamps)
    else:
        raise ValueError('Unknown timer instance type %s' % type(timer_instance).__name__)


class EventTime(object):
    def __init__(self, trigger_time):
        self.trigger_time = trigger_time

        tokens = self.trigger_time.split('-')
        if len(tokens) > 1:
            # Day of Week is provided
            self.day_of_week = tokens[0]
            self.time_of_day = datetime.strptime(tokens[1], TIME_OF_DAY_FORMAT)
        else:
            # Day of Week is not provided. Assume every day of the week
            self.day_of_week = EVERY_DAY
            self.time_of_day = datetime.strptime(tokens[0], TIME_OF_DAY_FORMAT)

    def __str__(self):
        return 'EventTime: day_of_week=%s time_of_day=%s' % \
               (self.day_of_week, datetime.strftime(self.time_of_day, TIME_OF_DAY_FORMAT))

    def __repr__(self):
        return '%s-%s' % (self.day_of_week, datetime.strftime(self.time_of_day, TIME_OF_DAY_FORMAT))

    def __eq__(self, other):
        if not isinstance(other, EventTime):
            return False

        return self.time_of_day == other.time_of_day \
            and (self.day_of_week == other.day_of_week
                 or self.day_of_week == EVERY_DAY
                 or other.day_of_week == EVERY_DAY)

    def __hash__(self):
        return hash((self.day_of_week, self.time_of_day))

    @classmethod
    def utc_now(cls):
        utc_now = datetime.utcnow()
        return EventTime('%s-%s' % (utc_now.weekday(), utc_now.strftime('%H:%M')))


class EventClock(object):
    """ This class triggers on predefined time set in format 'day_of_week-HH:MM' or 'HH:MM'
    Maintaining API compatibility with the RepeatTimer class """
    def __init__(self, interval, call_back, args=[], kwargs={}):
        self.timestamps = []
        self.change_interval(interval)

        self.args = args
        self.kwargs = kwargs
        self.call_back = call_back
        self.handler = RepeatTimer(TRIGGER_INTERVAL, self.manage_schedule)
        self.activation_dt = None

    def manage_schedule(self, *_):
        current_time = EventTime.utc_now()
        if current_time in self.timestamps:
            self.call_back(*self.args, **self.kwargs)
            self.activation_dt = datetime.utcnow()

    def start(self):
        self.handler.start()

    def cancel(self):
        self.handler.cancel()

    def trigger(self):
        current_time = EventTime.utc_now()
        if current_time not in self.timestamps:
            self.call_back(*self.args, **self.kwargs)
            self.activation_dt = datetime.utcnow()
        else:
            # leave it to the regular flow to trigger the call_back via manage_schedule method
            pass

    def change_interval(self, value):
        """ :param value: list of strings in format 'Day_of_Week-HH:MM' """
        assert not isinstance(value, str)
        self.timestamps = []

        for timestamp in value:
            event = EventTime(timestamp)
            self.timestamps.append(event)

    def next_run_in(self):
        """ :return: timedelta instance presenting amount of time before the trigger is triggered next time
         or None if the EventClock instance is not running """
        if self.handler.is_alive():
            return 'TBD'
        else:
            return None
