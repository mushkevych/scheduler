__author__ = 'Bohdan Mushkevych'


class DuplicateKeyError(Exception):
    def __init__(self, process_name, timeperiod, start_id, end_id, *args, **kwargs):
        self.process_name = process_name
        self.timeperiod = timeperiod
        self.start_id = start_id
        self.end_id = end_id
        super(DuplicateKeyError, self).__init__(*args, **kwargs)
