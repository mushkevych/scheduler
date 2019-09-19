__author__ = 'Bohdan Mushkevych'

import json

from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.mx.base_request_handler import valid_action_request
from synergy.mx.abstract_action_handler import AbstractActionHandler
from synergy.scheduler.scheduler_constants import STATE_MACHINE_FREERUN


class FreerunActionHandler(AbstractActionHandler):
    def __init__(self, request, **values):
        super(FreerunActionHandler, self).__init__(request, **values)
        self.process_name = self.request_arguments.get('process_name')
        self.entry_name = self.request_arguments.get('entry_name')
        self.freerun_process_dao = FreerunProcessDao(self.logger)
        self.is_request_valid = True if self.process_name and self.entry_name else False

        if self.is_request_valid:
            self.process_name = self.process_name.strip()
            self.entry_name = self.entry_name.strip()
            self.is_requested_state_on = self.request_arguments.get('is_on') == 'on'

    @AbstractActionHandler.thread_handler.getter
    def thread_handler(self):
        handler_key = (self.process_name, self.entry_name)
        return self.scheduler.freerun_handlers[handler_key]

    @AbstractActionHandler.process_entry.getter
    def process_entry(self):
        return self.thread_handler.process_entry

    @AbstractActionHandler.uow_id.getter
    def uow_id(self):
        return self.process_entry.related_unit_of_work

    @valid_action_request
    def cancel_uow(self):
        freerun_state_machine = self.scheduler.timetable.state_machines[STATE_MACHINE_FREERUN]
        freerun_state_machine.cancel_uow(self.process_entry)
        return self.reply_ok()

    @valid_action_request
    def get_event_log(self):
        return {'event_log': self.process_entry.event_log}

    @valid_action_request
    def create_entry(self):
        process_entry = FreerunProcessEntry()
        process_entry.process_name = self.process_name
        process_entry.entry_name = self.entry_name

        if self.request_arguments['arguments']:
            arguments = self.request_arguments['arguments']
            if isinstance(arguments, bytes):
                arguments = arguments.decode('unicode-escape')
            process_entry.arguments = json.loads(arguments)
        else:
            process_entry.arguments = {}

        process_entry.description = self.request_arguments['description']
        process_entry.is_on = self.is_requested_state_on
        process_entry.trigger_frequency = self.request_arguments['trigger_frequency']
        self.freerun_process_dao.update(process_entry)

        self.scheduler._register_process_entry(process_entry, self.scheduler.fire_freerun_worker)
        return self.reply_ok()

    @valid_action_request
    def delete_entry(self):
        handler_key = (self.process_name, self.entry_name)
        self.thread_handler.deactivate()
        self.freerun_process_dao.remove(handler_key)
        del self.scheduler.freerun_handlers[handler_key]
        self.logger.info(f'MX: Deleted FreerunThreadHandler for {handler_key}')
        return self.reply_ok()

    @valid_action_request
    def update_entry(self):
        is_interval_changed = self.process_entry.trigger_frequency != self.request_arguments['trigger_frequency']

        if self.request_arguments['arguments']:
            arguments = self.request_arguments['arguments']
            if isinstance(arguments, bytes):
                arguments = arguments.decode('unicode-escape')
            self.process_entry.arguments = json.loads(arguments)
        else:
            self.process_entry.arguments = {}

        self.process_entry.description = self.request_arguments['description']
        self.process_entry.is_on = self.is_requested_state_on
        self.process_entry.trigger_frequency = self.request_arguments['trigger_frequency']
        self.freerun_process_dao.update(self.process_entry)

        if is_interval_changed:
            self.change_interval()

        if self.process_entry.is_on != self.is_requested_state_on:
            if self.is_requested_state_on:
                self.activate_trigger()
            else:
                self.deactivate_trigger()
        return self.reply_ok()
