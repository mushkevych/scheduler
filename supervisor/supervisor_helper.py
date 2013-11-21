"""
Created on 2011-06-15

@author: Bohdan Mushkevych
"""
from model.box_configuration_entry import BoxConfigurationEntry
from settings import settings
from system.collection_context import CollectionContext, COLLECTION_BOX_CONFIGURATION

def retrieve_configuration(logger, box_id):
    """ method reads box configuration from the MongoDB"""
    collection = CollectionContext.get_collection(logger, COLLECTION_BOX_CONFIGURATION)
    document = collection.find_one( { BoxConfigurationEntry.BOX_ID : box_id } )
    if document is None:
        raise LookupError('MongoDB has no process list for box_id = %r' % box_id)
    return BoxConfigurationEntry(document)


def update_configuration(logger, box_configuration):
    """ method updates box configuration in the MongoDB"""
    w_number = CollectionContext.get_w_number(logger, COLLECTION_BOX_CONFIGURATION)
    collection = CollectionContext.get_collection(logger, COLLECTION_BOX_CONFIGURATION)
    collection.save(box_configuration.document, safe=True, w=w_number)


def get_box_id(logger):
    """ retrieves box id from the synergy_data configuration file """
    try:
        box_id = None
        config_file = settings['config_file']
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

    except EnvironmentError: # parent of IOError, OSError
        logger.error('Can not read configuration file.', exc_info=True)
  