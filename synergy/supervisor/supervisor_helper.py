__author__ = 'Bohdan Mushkevych'

from synergy.conf import settings


def get_box_id(logger):
    """ retrieves box id from the synergy_data configuration file """
    try:
        box_id = None
        config_file = settings.settings['config_file']
        with open(config_file) as a_file:
            for a_line in a_file:
                a_line = a_line.strip()
                if a_line.startswith('#'):
                    continue

                tokens = a_line.split('=')
                if tokens[0] == 'BOX_ID':
                    box_id = tokens[1]
                    return box_id

        if box_id is None:
            raise LookupError('BOX_ID is not defined in %r' % config_file)

    except EnvironmentError:  # parent of IOError, OSError
        logger.error('Can not read configuration file.', exc_info=True)
