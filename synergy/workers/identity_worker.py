__author__ = 'Bohdan Mushkevych'

from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker


class IdentityWorker(AbstractUowAwareWorker):
    """ Marks any incoming unit_of_work as <complete> """

    def __init__(self, process_name):
        super(IdentityWorker, self).__init__(process_name)

    def __del__(self):
        super(IdentityWorker, self).__del__()

    def _process_uow(self, uow):
        return 0
