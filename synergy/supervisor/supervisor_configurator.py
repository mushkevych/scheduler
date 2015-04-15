__author__ = 'Bohdan Mushkevych'

import re
import pymongo

from synergy.db.dao.box_configuration_dao import BoxConfigurationDao, QUERY_PROCESSES_FOR_BOX_ID
from synergy.db.manager import ds_manager
from synergy.db.model.box_configuration import BoxConfiguration, BOX_ID, PROCESS_NAME
from synergy.conf import context, settings
from synergy.supervisor.supervisor_constants import PROCESS_SUPERVISOR, COLLECTION_BOX_CONFIGURATION
from synergy.system.data_logging import get_logger


CONFIG_FILE_TEMPLATE = (
    '# This file was auto-generated by Synergy Supervisor. Do not edit it manually\n'
    '# This file is expected to be located at: /etc/\n'
    '# BOX_ID identifies a box in a cluster supervised by Synergy Scheduler\n'
    'BOX_ID={0}\n'
)


def set_box_id(logger, box_id):
    config_file_name = settings.settings['config_file']
    try:
        with open(config_file_name, mode='w') as config_file:
            config_file.write(CONFIG_FILE_TEMPLATE.format(box_id))
    except Exception as e:
        logger.info('Unable to create BOX_ID file at: {0}, due to: {1}'.format(config_file_name, e))


def get_box_id(logger):
    """ retrieves box id from the /etc/synergy.conf configuration file """
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
            raise LookupError('BOX_ID is not defined in {0}'.format(config_file))

    except EnvironmentError:  # parent of IOError, OSError
        logger.error('Can not read configuration file.', exc_info=True)


class SupervisorEntry(object):
    def __init__(self, process_name):
        if process_name not in context.process_context:
            raise ValueError('SupervisorEntry: process {0} is not found in process_context. Aborting'
                             .format(process_name))

        self.logger = get_logger(PROCESS_SUPERVISOR, True)
        self.process_name = process_name
        self.re_boxes = context.process_context[process_name].present_on_boxes
        self.re_co_boxes = []  # compiled RE of boxes where the process should be present

        for re_box in self.re_boxes:
            try:
                if isinstance(re_box, str):
                    re_box = re_box.lower()
                    self.re_co_boxes.append(re.compile(re_box))
                elif isinstance(re_box, int):
                    self.re_co_boxes.append(re_box)
                else:
                    raise ValueError('SupervisorEntry support (string, integer) values. Type {0} unsupported'
                                     .format(type(re_box)))
            except TypeError:
                self.logger.warn('SupervisorEntry compilation error for {0}'.format(re_box))

    def is_present_on(self, box_id):
        box_id = box_id.lower()
        is_present = False
        for co_re_box in self.re_co_boxes:
            if isinstance(co_re_box, int):
                is_present = co_re_box == box_id

            else:
                if co_re_box.search(box_id):
                    is_present = True

            if is_present:
                break

        return is_present


class SupervisorConfigurator(object):
    def __init__(self):
        super(SupervisorConfigurator, self).__init__()
        self.logger = get_logger(PROCESS_SUPERVISOR, True)
        self.bc_dao = BoxConfigurationDao(self.logger)

        self.box_id = get_box_id(self.logger)
        self.process_map = dict()
        for process_name in context.process_context:
            try:
                self.process_map[process_name] = SupervisorEntry(process_name)
            except ValueError:
                continue

    def _change_state(self, process_name, new_state):
        self.logger.info('INFO: Supervisor configuration: setting state {0} for process {1} \n'
                         .format(new_state, process_name))
        box_config = self.bc_dao.get_one([self.box_id, process_name])
        box_config.is_on = new_state
        self.bc_dao.update(box_config)

    def mark_for_start(self, process_name):
        self._change_state(process_name, False)

    def mark_for_stop(self, process_name):
        self._change_state(process_name, False)

    def init_db(self):
        self.logger.info('Starting *synergy.box_configuration* table init')

        ds = ds_manager.ds_factory(self.logger)
        ds._db.drop_collection(COLLECTION_BOX_CONFIGURATION)
        self.logger.info('*synergy.box_configuration* table has been dropped')

        connection = ds.connection(COLLECTION_BOX_CONFIGURATION)
        connection.create_index([(BOX_ID, pymongo.ASCENDING), (PROCESS_NAME, pymongo.ASCENDING)], unique=True)

        for process_name, supervisor_entry in self.process_map.items():
            if supervisor_entry.is_present_on(self.box_id):
                box_config = BoxConfiguration(box_id=self.box_id,
                                              process_name=process_name,
                                              is_on=True)
                self.bc_dao.update(box_config)

        self.logger.info('*synergy.box_configuration* db has been recreated')

    def query(self):
        self.logger.info('\nSupervisor Snapshot for BOX_ID={0}:\n'.format(self.box_id))
        box_configurations = self.bc_dao.run_query(QUERY_PROCESSES_FOR_BOX_ID(self.box_id))

        for box_config in box_configurations:
            self.logger.info('{0}:\t{1}'.format(box_config.process_name, box_config.state))