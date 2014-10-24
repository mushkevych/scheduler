from synergy.mx.mx_decorators import valid_action_request

__author__ = 'Bohdan Mushkevych'


class AbstractActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        # self.request = request
        self.request_arguments = request.args if request.args else request.form
        self.is_request_valid = False

    @property
    def scheduler_thread_handler(self):
        raise NotImplementedError('not implemented yet')

    @property
    def scheduler_entry(self):
        raise NotImplementedError('not implemented yet')

    def action_get_uow(self):
        raise NotImplementedError('not implemented yet')

    def action_get_log(self):
        raise NotImplementedError('not implemented yet')

    @valid_action_request
    def action_change_interval(self):
        resp = dict()
        new_interval = self.request_arguments['interval']
        if new_interval is not None:
            thread_handler = self.scheduler_thread_handler
            thread_handler.change_interval(new_interval)
            resp['status'] = 'changed interval for %r to %r' % (thread_handler.arguments.key, new_interval)

        return resp

    @valid_action_request
    def action_trigger_now(self):
        self.scheduler_thread_handler.trigger()
        return {'status': 'OK'}

    @valid_action_request
    def action_activate_trigger(self):
        self.scheduler_thread_handler.activate()
        return {'status': 'OK'}

    @valid_action_request
    def action_deactivate_trigger(self):
        self.scheduler_thread_handler.deactivate()
        return {'status': 'OK'}
