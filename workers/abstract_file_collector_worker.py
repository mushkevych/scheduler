__author__ = 'Bohdan Mushkevych'

import os
import shutil
import tempfile

import fabric.operations
from synergy.system.utils import compute_gzip_md5
from synergy.conf import settings
from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker
from synergy.db.model import unit_of_work


class AbstractFileCollectorWorker(AbstractUowAwareWorker):
    """
    module holds common logic to process unit_of_work, access remote locations and copy files to temporary local folder
    individual files are later passed to child classes for processing
    """

    HEADER_FOLDER = "header"

    def __init__(self, process_name):
        super(AbstractFileCollectorWorker, self).__init__(process_name)
        self.tempdir_copying = None

    def __del__(self):
        self._clean_up()
        super(AbstractFileCollectorWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _get_source_folder(self):
        """ Abstract method: identifies a folder with source files """
        pass

    def _get_file_pattern(self, timeperiod):
        """ Abstract method: identifies file pattern"""
        pass

    def _get_header_file_pattern(self, timeperiod):
        """ Abstract method: identifies header file pattern"""
        pass

    def copy_header_files_from_source(self, timeperiod):
        """ method accesses remote location and copies files, specified by _get_source_folder
        and _get_header_file_pattern to local temporary folder
        @return: list of file names that were copied to local file system and are available for further processing
        @raise LookupError: in case no file names on remote location were found or copied requested date"""

        fabric.operations.env.warn_only = True
        fqsf = os.path.join(self._get_source_folder(), self.HEADER_FOLDER)
        for host_name in settings.settings['remote_source_host_list']:
            self.logger.info('Initiating header files copy procedure from source location %s:%s'
                             % (host_name, self._get_source_folder()))
            fabric.operations.env.host_string = host_name
            file_list = fabric.operations.get(os.path.join(fqsf, self._get_header_file_pattern(timeperiod)),
                                              self.tempdir_copying)

            if len(file_list) > 0:
                self.logger.info('Copied %d header files from remote location: %s' % (len(file_list), host_name))
                return file_list
            else:
                raise LookupError('No header files found at %s' % host_name + '/' + fqsf)

    def copy_archives_from_source(self, timeperiod):
        """ method accesses remote location and copies files, specified by _get_source_folder and _get_file_pattern
        to local temporary folder
        @return: list of file names that were copied to local file system and are available for further processing
        @raise LookupError: in case no file names on remote location were found or copied requested date"""

        fabric.operations.env.warn_only = True
        summary_file_list = []
        fqsf = os.path.join(self._get_source_folder(), timeperiod[:-2])
        for host_name in settings.settings['remote_source_host_list']:
            self.logger.info('Initiating copy procedure from source location %s:%s' % (host_name, fqsf))
            fabric.operations.env.host_string = host_name
            file_list = fabric.operations.get(os.path.join(fqsf, self._get_file_pattern(timeperiod)),
                                              self.tempdir_copying)

            if len(file_list) > 0:
                self.logger.info('Copied %d files from remote location: %s' % (len(file_list), host_name))
                summary_file_list.extend(file_list)
            else:
                self.logger.info('No data files found for %s at %s' % (timeperiod, host_name + fqsf))

        return summary_file_list

    def _parse_metadata(self, file_name):
        """ Abstract method: parses metadata from filename (such as hostname, timeperiod, client_id, etc)"""
        pass

    def _parse_header_metadata(self, file_name):
        """ Abstract method: parses metadata from header filename (such as hostname, timeperiod, client_id, etc)"""
        pass

    def process_report_archive(self, file_name, metadata):
        """ Abstract method: takes archived report and process it
        @return number of document processed in this report """
        pass

    def process_header_file(self, file_name, metadata):
        """ Abstract method: takes header file and process it
        @return None """
        pass

    def perform_post_processing(self, timeperiod):
        """ abstract method to perform post-processing """
        pass

    def _process_uow(self, uow):
        self._create_directories()
        number_of_aggregated_objects = 0

        processed_log = dict()
        fqsf = os.path.join(self._get_source_folder(), uow.start_timeperiod)
        list_of_archives = self.copy_archives_from_source(uow.start_timeperiod)
        list_of_headers = self.copy_header_files_from_source(uow.start_timeperiod)

        for file_name in list_of_headers:
            metadata = self._parse_header_metadata(file_name)
            self.process_header_file(os.path.join(fqsf, file_name), metadata)

        for file_name in list_of_archives:
            metadata = self._parse_metadata(file_name)
            number_of_processed_docs = self.process_report_archive(os.path.join(fqsf, file_name), metadata)
            number_of_aggregated_objects += number_of_processed_docs
            self.performance_ticker.increment()

            tiny_log = dict()
            if settings.settings['compute_gzip_md5']:
                tiny_log[unit_of_work.MD5] = compute_gzip_md5(os.path.join(fqsf, file_name))

            tiny_log[unit_of_work.FILE_NAME] = file_name
            tiny_log[unit_of_work.NUMBER_OF_PROCESSED_DOCUMENTS] = number_of_processed_docs
            processed_log[file_name.replace('.', '-')] = tiny_log

        self.perform_post_processing(uow.start_timeperiod)
        return number_of_aggregated_objects

    def _create_directories(self):
        """ method creates temporary directories:
          - to store files copied from remote locations to local filesystem
          - uncompressed files """
        self.tempdir_copying = tempfile.mkdtemp()

    def _clean_up(self):
        """ method verifies if temporary folder exists and removes it (and nested content) """
        if self.tempdir_copying:
            self.logger.info('Cleaning up %r' % self.tempdir_copying)
            shutil.rmtree(self.tempdir_copying, True)
            self.tempdir_copying = None
