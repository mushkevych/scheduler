__author__ = 'Bohdan Mushkevych'

from datetime import datetime, timedelta

from synergy.system.repeat_timer import RepeatTimer

TIME_OF_DAY_FORMAT = "%H:%M"
EVERY_DAY = '*'        # marks every day as suitable to trigger the event
TRIGGER_INTERVAL = 30  # half a minute


class EventTime(object):
    def __init__(self, trigger_frequency):
        self.trigger_frequency = trigger_frequency

        tokens = self.trigger_frequency.split('-')
        if len(tokens) > 1:
            # Day of Week is provided
            self.day_of_week = tokens[0]
            self.time_of_day = datetime.strptime(tokens[1], TIME_OF_DAY_FORMAT)
        else:
            # Day of Week is not provided. Assume every day of the week
            self.day_of_week = EVERY_DAY
            self.time_of_day = datetime.strptime(tokens[0], TIME_OF_DAY_FORMAT)

    def __str__(self):
        return 'EventTime: day_of_week={0} time_of_day={1}'\
               .format(self.day_of_week, self.time_of_day.strftime(TIME_OF_DAY_FORMAT))

    def __repr__(self):
        return '{0}-{1}'.format(self.day_of_week, self.time_of_day.strftime(TIME_OF_DAY_FORMAT))

    def __eq__(self, other):
        if not isinstance(other, EventTime):
            return False

        return self.time_of_day == other.time_of_day \
            and (self.day_of_week == other.day_of_week
                 or self.day_of_week == EVERY_DAY
                 or other.day_of_week == EVERY_DAY)

    def __hash__(self):
        return hash((self.day_of_week, self.time_of_day))

    def next_trigger_frequency(self, utc_now=None):
        """ :param utc_now: optional parameter to be used by Unit Tests as a definition of "now"
            :return: datetime instance presenting next trigger time of the event """
        if utc_now is None:
            utc_now = datetime.utcnow()

        def wind_days(start_date):
            while True:
                if self.day_of_week == EVERY_DAY or start_date.weekday() == int(self.day_of_week):
                    return start_date.replace(hour=self.time_of_day.hour, minute=self.time_of_day.minute)
                else:
                    start_date += timedelta(days=1)

        if utc_now.time() > self.time_of_day.time():
            return wind_days(utc_now + timedelta(days=1))
        else:
            return wind_days(utc_now)

    @classmethod
    def utc_now(cls):
        utc_now = datetime.utcnow()
        return EventTime('{0}-{1}'.format(utc_now.weekday(), utc_now.strftime(TIME_OF_DAY_FORMAT)))


class EventClock(object):
    """ This class triggers on predefined time set in format 'day_of_week-HH:MM' or 'HH:MM'
    Maintaining API compatibility with the RepeatTimer class """

    def __init__(self, interval, call_back, args=None, kwargs=None):
        if not kwargs: kwargs = {}
        if not args: args = []

        self.timestamps = []
        self.change_interval(interval)

        self.args = args
        self.kwargs = kwargs
        self.call_back = call_back
        self.handler = RepeatTimer(TRIGGER_INTERVAL, self.manage_schedule)
        self.activation_dt = None

    def _trigger_now(self):
        if self.activation_dt is not None and datetime.utcnow() - self.activation_dt < timedelta(minutes=1):
            # the event was already triggered within 1 minute. no need to trigger it again
            return
        self.call_back(*self.args, **self.kwargs)
        self.activation_dt = datetime.utcnow()

    def manage_schedule(self, *_):
        current_time = EventTime.utc_now()
        if current_time in self.timestamps:
            self._trigger_now()

    def start(self):
        self.handler.start()

    def cancel(self):
        self.handler.cancel()

    def trigger(self):
        current_time = EventTime.utc_now()
        if current_time not in self.timestamps:
            self._trigger_now()
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

    def next_run_in(self, utc_now=None):
        """ :param utc_now: optional parameter to be used by Unit Tests as a definition of "now"
            :return: timedelta instance presenting amount of time before the trigger is triggered next time
         or None if the EventClock instance is not running """
        if utc_now is None:
            utc_now = datetime.utcnow()

        if self.is_alive():
            smallest_timedelta = timedelta(days=99, hours=0, minutes=0, seconds=0, microseconds=0, milliseconds=0)
            for event_time in self.timestamps:
                next_trigger = event_time.next_trigger_frequency(utc_now)
                if next_trigger - utc_now < smallest_timedelta:
                    smallest_timedelta = next_trigger - utc_now
            return smallest_timedelta

        else:
            return None

    def is_alive(self):
        return self.handler.is_alive()
