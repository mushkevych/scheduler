__author__ = 'Bohdan Mushkevych'

from synergy.system.time_trigger_factory import format_time_trigger_string
from synergy.mx.rest_model import *


def get_next_run_in(thread_handler):
    if not thread_handler.is_alive:
        return 'NA'

    next_run = thread_handler.next_run_in()
    return str(next_run).split('.')[0]


def get_next_timeperiod(timetable, process_name):
    if timetable.get_tree(process_name) is None:
        return 'NA'
    else:
        job_record = timetable.get_next_job_record(process_name)
        return job_record.timeperiod


def get_dependant_trees(timetable, tree_obj):
    trees = timetable._find_dependant_trees(tree_obj)
    return [x.tree_name for x in trees]


def get_reprocessing_queue(gc, process_name):
    per_process = gc.reprocess_uows[process_name]
    q = []
    for priority_entry in sorted(per_process.queue):
        q.append(priority_entry.entry.timeperiod)
    return q


def create_rest_managed_scheduler_entry(thread_handler, timetable, gc):
    process_entry = thread_handler.process_entry
    process_name = process_entry.process_name

    rest_model = RestManagedSchedulerEntry(
        is_on=process_entry.is_on,
        is_alive=thread_handler.is_alive,
        process_name=process_name,
        trigger_frequency=format_time_trigger_string(thread_handler.timer_instance),
        next_run_in=get_next_run_in(thread_handler),
        next_timeperiod=get_next_timeperiod(timetable, process_name),
        time_qualifier=process_entry.time_qualifier,
        time_grouping=process_entry.time_grouping,
        state_machine_name=process_entry.state_machine_name,
        blocking_type=process_entry.blocking_type,
        reprocessing_queue=get_reprocessing_queue(gc, process_name),
    )
    return rest_model


def create_rest_freerun_scheduler_entry(thread_handler):
    process_name, entry_name = thread_handler.key
    rest_model = RestFreerunSchedulerEntry(
        is_on=thread_handler.process_entry.is_on,
        is_alive=thread_handler.is_alive,
        process_name=process_name,
        entry_name=entry_name,
        trigger_frequency=format_time_trigger_string(thread_handler.timer_instance),
        description=thread_handler.process_entry.description,
        next_run_in=get_next_run_in(thread_handler),
        event_log=thread_handler.process_entry.event_log,
        arguments=thread_handler.process_entry.arguments
    )
    return rest_model


def create_rest_timetable_tree(timetable, tree_obj):
    rest_tree = RestTimetableTree(tree_name=tree_obj.tree_name,
                                  mx_page=tree_obj.mx_page,
                                  mx_name=tree_obj.mx_name,
                                  dependent_on=[tree.tree_name for tree in tree_obj.dependent_on],
                                  dependant_trees=get_dependant_trees(timetable, tree_obj),
                                  sorted_process_names=[x for x in tree_obj.process_hierarchy])
    return rest_tree
