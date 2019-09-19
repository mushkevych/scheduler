__author__ = 'Bohdan Mushkevych'

from synergy.system.event_clock import EventClock
from synergy.system.repeat_timer import RepeatTimer

TRIGGER_PREAMBLE_AT = 'at '
TRIGGER_PREAMBLE_EVERY = 'every '


def parse_time_trigger_string(trigger_frequency):
    """
    :param trigger_frequency: human-readable and editable string in one of two formats:
     - 'at Day_of_Week-HH:MM, ..., Day_of_Week-HH:MM'
     - 'every NNN'
    :return: return tuple (parsed_trigger_frequency, timer_klass)
    """
    # replace multiple spaces with one
    trigger_frequency = ' '.join(trigger_frequency.split())

    if trigger_frequency.startswith(TRIGGER_PREAMBLE_AT):
        # EventClock block
        trigger_frequency = trigger_frequency[len(TRIGGER_PREAMBLE_AT):]
        parsed_trigger_frequency = trigger_frequency.replace(' ', '').replace(',', ' ').split(' ')
        timer_klass = EventClock
    elif trigger_frequency.startswith(TRIGGER_PREAMBLE_EVERY):
        # RepeatTimer block
        trigger_frequency = trigger_frequency[len(TRIGGER_PREAMBLE_EVERY):]
        parsed_trigger_frequency = int(trigger_frequency)
        timer_klass = RepeatTimer
    else:
        raise ValueError(f'Unknown time trigger format {trigger_frequency}')

    return parsed_trigger_frequency, timer_klass


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
        raise ValueError(f'Unknown timer instance type {timer_instance.__class__.__name__}')
