__author__ = 'Bohdan Mushkevych'

import json

from synergy.db.model import unit_of_work
from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.scheduler.scheduler_constants import TYPE_FREERUN
from synergy.mx.mx_decorators import valid_action_request
from synergy.mx.abstract_action_handler import AbstractActionHandler


class FreerunActionHandler(AbstractActionHandler):
    def __init__(self, mbean, request):
        super(FreerunActionHandler, self).__init__(mbean, request)
        self.process_name = request.args.get('process_name')
        self.entry_name = request.args.get('entry_name')
        self.se_freerun_dao = SchedulerFreerunEntryDao(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.is_request_valid = self.mbean is not None \
                                and self.process_name is not None \
                                and self.entry_name is not None

        if self.is_request_valid:
            self.process_name = self.process_name.strip()
            self.entry_name = self.entry_name.strip()

    @valid_action_request
    def scheduler_thread_handler(self):
        handler_key = (self.process_name, self.entry_name)
        return self.mbean.freerun_handlers[handler_key]

    @valid_action_request
    def scheduler_entry(self):
        scheduler_entry_obj = self.scheduler_thread_handler.args[1]
        assert isinstance(scheduler_entry_obj, SchedulerFreerunEntry)
        return scheduler_entry_obj

    @valid_action_request
    def scheduler_entry_dao(self):
        return self.se_freerun_dao

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
        scheduler_entry_obj = self.scheduler_entry
        return {'log': scheduler_entry_obj.log}

    @valid_action_request
    def action_change_interval(self):
        handler_key = (self.process_name, self.entry_name)
        return self._action_change_interval(handler_key, TYPE_FREERUN)

    @valid_action_request
    def action_update_entry(self):
        handler_key = (self.process_name, self.entry_name)

        if 'insert_button' in self.request.args:
            scheduler_entry_obj = SchedulerFreerunEntry()
            scheduler_entry_obj.process_name = self.process_name
            scheduler_entry_obj.entry_name = self.entry_name

            if self.request.args['arguments']:
                arguments = self.request.args['arguments'].decode('unicode-escape')
                scheduler_entry_obj.arguments = json.loads(arguments)
            else:
                scheduler_entry_obj.arguments = {}

            scheduler_entry_obj.description = self.request.args['description']
            scheduler_entry_obj.state = self.request.args['state']
            scheduler_entry_obj.trigger_time = self.request.args['trigger_time']
            self.se_freerun_dao.update(scheduler_entry_obj)

            self.mbean._activate_handler(scheduler_entry_obj, self.process_name, self.entry_name,
                                         self.mbean.fire_freerun_worker, TYPE_FREERUN)
        elif 'update_button' in self.request.args:
            thread_handler = self.mbean.freerun_handlers[handler_key]
            scheduler_entry_obj = thread_handler.args[1]

            is_interval_changed = scheduler_entry_obj.trigger_time != self.request.args['trigger_time']
            is_state_changed = scheduler_entry_obj.state != self.request.args['state']

            if self.request.args['arguments']:
                arguments = self.request.args['arguments'].decode('unicode-escape')
                scheduler_entry_obj.arguments = json.loads(arguments)
            else:
                scheduler_entry_obj.arguments = {}

            scheduler_entry_obj.description = self.request.args['description']
            scheduler_entry_obj.state = self.request.args['state']
            scheduler_entry_obj.trigger_time = self.request.args['trigger_time']
            self.se_freerun_dao.update(scheduler_entry_obj)

            if is_interval_changed:
                self._action_change_interval(handler_key, TYPE_FREERUN)
            if is_state_changed:
                self._action_change_state(thread_handler)

        elif 'delete_button' in self.request.args:
            self.se_freerun_dao.remove(handler_key)
            thread_handler = self.mbean.freerun_handlers[handler_key]
            thread_handler.cancel()
            del self.mbean.freerun_handlers[handler_key]

        elif 'cancel_button' in self.request.args:
            pass
        else:
            self.logger.error('Unknown action requested by schedulable_form.html')

        return {'status': 'OK'}
