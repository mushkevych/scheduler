__author__ = 'Bohdan Mushkevych'

from Queue import Queue
from threading import Thread

from synergy.scheduler.abstract_state_machine import AbstractStateMachine
from synergy.scheduler.thread_handler import ManagedThreadHandler


class JobStatusBroadcaster(object):
    """ threading Queue worker that notifies dependant jobs about completion of their dependency-provider """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.timetable = scheduler.timetable
        self.logger = scheduler.logger
        self.queue = Queue()
        self.main_thread = None
        self.is_alive = True

    def broadcast(self, job_record):
        self.queue.put(job_record, block=False)

    def __broadcast(self, job_record):
        # step 1: identify dependant tree nodes
        tree_obj = self.timetable.get_tree(job_record.process_name)
        tree_node = tree_obj.get_node(job_record.process_name, job_record.timeperiod)
        dependant_nodes = self.timetable._find_dependant_tree_nodes(tree_node)

        # step 2: form list of handlers to trigger
        handlers_to_trigger = list()
        for node in dependant_nodes:
            process_entry = self.scheduler.managed_handlers[node.process_name].process_entry
            state_machine = self.timetable.state_machines[process_entry.state_machine_name]
            assert isinstance(state_machine, AbstractStateMachine)
            if state_machine.run_on_active_timeperiod:
                # ignore dependant processes whose state machine can run on an active timeperiod
                continue
            handlers_to_trigger.append(self.scheduler.managed_handlers[node.process_name])

        # step 3: iterate the list of handlers and trigger them
        for handler in handlers_to_trigger:
            assert isinstance(handler, ManagedThreadHandler)
            handler.trigger()

    def _run(self):
        while self.is_alive:
            job_record = self.queue.get()
            self.__broadcast(job_record)
            self.queue.task_done()

    def start(self, *_):
        self.main_thread = Thread(target=self._run)
        self.main_thread.daemon = True
        self.main_thread.start()

    def stop(self):
        self.is_alive = False
