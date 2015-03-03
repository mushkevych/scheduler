__author__ = 'Bohdan Mushkevych'

import json

from synergy.db.model import unit_of_work
from synergy.db.model.freerun_process_entry import FreerunProcessEntry, STATE_ON, STATE_OFF
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.mx.mx_decorators import valid_action_request
from synergy.mx.abstract_action_handler import AbstractActionHandler


class FreerunActionHandler(AbstractActionHandler):
    def __init__(self, mbean, request):
        super(FreerunActionHandler, self).__init__(mbean, request)
        self.process_name = self.request_arguments.get('process_name')
        self.entry_name = self.request_arguments.get('entry_name')
        self.freerun_process_dao = FreerunProcessDao(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.is_request_valid = self.mbean is not None \
                                and not not self.process_name \
                                and not not self.entry_name

        if self.is_request_valid:
            self.process_name = self.process_name.strip()
            self.entry_name = self.entry_name.strip()

    @AbstractActionHandler.scheduler_thread_handler.getter
    def scheduler_thread_handler(self):
        handler_key = (self.process_name, self.entry_name)
        return self.mbean.freerun_handlers[handler_key]

    @AbstractActionHandler.process_entry.getter
    def process_entry(self):
        return self.scheduler_thread_handler.process_entry

    @valid_action_request
    def action_cancel_uow(self):
        uow_id = self.process_entry.related_unit_of_work
        if uow_id is None:
            resp = {'response': 'no related unit_of_work'}
        else:
            uow = self.uow_dao.get_one(uow_id)
            uow.state = unit_of_work.STATE_CANCELED
            self.uow_dao.update(uow)
            resp = {'response': 'updated unit_of_work %r' % uow_id}
        return resp

    @valid_action_request
    def action_get_uow(self):
        uow_id = self.process_entry.related_unit_of_work
        if uow_id is None:
            resp = {'response': 'no related unit_of_work'}
        else:
            resp = self.uow_dao.get_one(uow_id).document
            for key in resp:
                resp[key] = str(resp[key])
        return resp

    @valid_action_request
    def action_get_log(self):
        return {'log': self.process_entry.log}

    @valid_action_request
    def action_update_entry(self):
        if 'insert_button' in self.request_arguments:
            process_entry = FreerunProcessEntry()
            process_entry.process_name = self.process_name
            process_entry.entry_name = self.entry_name

            if self.request_arguments['arguments']:
                arguments = self.request_arguments['arguments'].decode('unicode-escape')
                process_entry.arguments = json.loads(arguments)
            else:
                process_entry.arguments = {}

            process_entry.description = self.request_arguments['description']
            process_entry.state = self.request_arguments['state']
            process_entry.trigger_frequency = self.request_arguments['trigger_frequency']
            self.freerun_process_dao.update(process_entry)

            self.mbean._register_process_entry(process_entry, self.mbean.fire_freerun_worker)

        elif 'update_button' in self.request_arguments:
            is_interval_changed = self.process_entry.trigger_frequency != self.request_arguments['trigger_frequency']
            is_state_changed = self.process_entry.state != self.request_arguments['state']

            if self.request_arguments['arguments']:
                arguments = self.request_arguments['arguments'].decode('unicode-escape')
                self.process_entry.arguments = json.loads(arguments)
            else:
                self.process_entry.arguments = {}

            self.process_entry.description = self.request_arguments['description']
            self.process_entry.state = self.request_arguments['state']
            self.process_entry.trigger_frequency = self.request_arguments['trigger_frequency']
            self.freerun_process_dao.update(self.process_entry)

            if is_interval_changed:
                self.action_change_interval()

            if is_state_changed and self.request_arguments['state'] == STATE_ON:
                self.action_activate_trigger()
            elif is_state_changed and self.request_arguments['state'] == STATE_OFF:
                self.action_deactivate_trigger()

        elif 'delete_button' in self.request_arguments:
            handler_key = (self.process_name, self.entry_name)
            self.scheduler_thread_handler.deactivate()
            self.freerun_process_dao.remove(handler_key)
            del self.mbean.freerun_handlers[handler_key]

        elif 'cancel_button' in self.request_arguments:
            pass

        else:
            self.logger.error('Unknown action requested by schedulable_form.html')

        return {'status': 'OK'}
