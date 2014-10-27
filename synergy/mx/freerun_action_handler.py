__author__ = 'Bohdan Mushkevych'

import json

from synergy.db.model import unit_of_work
from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry, STATE_ON, STATE_OFF
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.mx.mx_decorators import valid_action_request
from synergy.mx.abstract_action_handler import AbstractActionHandler


class FreerunActionHandler(AbstractActionHandler):
    def __init__(self, mbean, request):
        super(FreerunActionHandler, self).__init__(mbean, request)
        self.process_name = self.request_arguments.get('process_name')
        self.entry_name = self.request_arguments.get('entry_name')
        self.se_freerun_dao = SchedulerFreerunEntryDao(self.logger)
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

    @AbstractActionHandler.scheduler_entry.getter
    def scheduler_entry(self):
        scheduler_entry_obj = self.scheduler_thread_handler.arguments.scheduler_entry_obj
        assert isinstance(scheduler_entry_obj, SchedulerFreerunEntry)
        return scheduler_entry_obj

    @valid_action_request
    def action_cancel_uow(self):
        uow_id = self.scheduler_entry.related_unit_of_work
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
        uow_id = self.scheduler_entry.related_unit_of_work
        if uow_id is None:
            resp = {'response': 'no related unit_of_work'}
        else:
            resp = self.uow_dao.get_one(uow_id).document
            for key in resp:
                resp[key] = str(resp[key])
        return resp

    @valid_action_request
    def action_get_log(self):
        return {'log': self.scheduler_entry.log}

    @valid_action_request
    def action_update_entry(self):
        handler_key = (self.process_name, self.entry_name)

        if 'insert_button' in self.request_arguments:
            scheduler_entry_obj = SchedulerFreerunEntry()
            scheduler_entry_obj.process_name = self.process_name
            scheduler_entry_obj.entry_name = self.entry_name

            if self.request_arguments['arguments']:
                arguments = self.request_arguments['arguments'].decode('unicode-escape')
                scheduler_entry_obj.arguments = json.loads(arguments)
            else:
                scheduler_entry_obj.arguments = {}

            scheduler_entry_obj.description = self.request_arguments['description']
            scheduler_entry_obj.state = self.request_arguments['state']
            scheduler_entry_obj.trigger_time = self.request_arguments['trigger_time']
            self.se_freerun_dao.update(scheduler_entry_obj)

            self.mbean._register_scheduler_entry(scheduler_entry_obj, self.mbean.fire_freerun_worker)

        elif 'update_button' in self.request_arguments:
            thread_handler = self.mbean.freerun_handlers[handler_key]
            scheduler_entry_obj = thread_handler.arguments.scheduler_entry_obj

            is_interval_changed = scheduler_entry_obj.trigger_time != self.request_arguments['trigger_time']
            is_state_changed = scheduler_entry_obj.state != self.request_arguments['state']

            if self.request_arguments['arguments']:
                arguments = self.request_arguments['arguments'].decode('unicode-escape')
                scheduler_entry_obj.arguments = json.loads(arguments)
            else:
                scheduler_entry_obj.arguments = {}

            scheduler_entry_obj.description = self.request_arguments['description']
            scheduler_entry_obj.state = self.request_arguments['state']
            scheduler_entry_obj.trigger_time = self.request_arguments['trigger_time']
            self.se_freerun_dao.update(scheduler_entry_obj)

            if is_interval_changed:
                self.action_change_interval()

            if is_state_changed and self.request_arguments['state'] == STATE_ON:
                self.action_activate_trigger()
            elif is_state_changed and self.request_arguments['state'] == STATE_OFF:
                self.action_deactivate_trigger()

        elif 'delete_button' in self.request_arguments:
            thread_handler = self.mbean.freerun_handlers[handler_key]
            thread_handler.deactivate()
            self.se_freerun_dao.remove(handler_key)
            del self.mbean.freerun_handlers[handler_key]

        elif 'cancel_button' in self.request_arguments:
            pass

        else:
            self.logger.error('Unknown action requested by schedulable_form.html')

        return {'status': 'OK'}
