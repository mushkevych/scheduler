__author__ = 'Bohdan Mushkevych'


class DuplicateKeyError(StandardError):
    def __init__(self, *args, **kwargs):
        super(DuplicateKeyError, self).__init__(*args, **kwargs)