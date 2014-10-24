__author__ = 'Bohdan Mushkevych'


class AbstractActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request

    def scheduler_thread_handler(self):
        raise NotImplementedError('not implemented yet')

    def scheduler_entry(self):
        raise NotImplementedError('not implemented yet')

    def action_get_uow(self):
        raise NotImplementedError('not implemented yet')

    def action_get_log(self):
        raise NotImplementedError('not implemented yet')

    def action_change_interval(self):
        resp = dict()
        new_interval = self.request.args.get('interval')
        if new_interval is not None:
            thread_handler = self.scheduler_thread_handler()
            thread_handler.change_interval(new_interval)
            resp['status'] = 'changed interval for %r to %r' % (thread_handler.args[0], new_interval)

        return resp

    def action_trigger_now(self):
        self.scheduler_thread_handler().trigger()
        return {'status': 'OK'}

    def action_activate_trigger(self):
        thread_handler = self.scheduler_thread_handler()
        thread_handler.activate()
        return {'status': 'OK'}

    def action_deactivate_trigger(self):
        thread_handler = self.scheduler_thread_handler()
        thread_handler.deactivate()
        return {'status': 'OK'}
