__author__ = 'Bohdan Mushkevych'

import os
import gzip
import shutil
import tempfile
import hashlib
import fabric.operations
from datetime import datetime

from settings import settings
from workers.abstract_mq_worker import AbstractMqWorker
from system.performance_tracker import AggregatorPerformanceTicker

from db.model import unit_of_work
from db.model.worker_mq_request import WorkerMqRequest
from db.manager import ds_manager
from db.dao.unit_of_work_dao import UnitOfWorkDao


class AbstractFileCollectorWorker(AbstractMqWorker):
    """
    module holds common logic to process unit_of_work, access remote locations and copy files to temporary local folder
    individual files are later passed to child classes for processing
    """

    HEADER_FOLDER = "header"

    def __init__(self, process_name):
        super(AbstractFileCollectorWorker, self).__init__(process_name)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.ds = ds_manager.ds_factory(self.logger)
        self.tempdir_copying = None

    def __del__(self):
        self._clean_directories()
        super(AbstractFileCollectorWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = AggregatorPerformanceTicker(logger)
        self.performance_ticker.start()

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
        for host_name in settings['remote_source_host_list']:
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
        for host_name in settings['remote_source_host_list']:
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

    def _mq_callback(self, message):
        """ workflow looks like following:
        - read the unit_of_work from Data Source
        - finds the files on remote location base on requested timeperiod
        - copy files to local temporary folder
        - in case no files are found and copied - raise LookupError
        - for every file: un-archive it and process"""
        try:
            mq_request = WorkerMqRequest(message.body)
            uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            if uow.state in [unit_of_work.STATE_CANCELED, unit_of_work.STATE_PROCESSED]:
                # garbage collector might have reposted this UOW
                self.logger.warning('Skipping unit_of_work: id %s; state %s;' % (str(message.body), uow.state),
                                    exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            self.uow_dao.update(uow)
            self.performance_ticker.start_uow(uow)

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
                tiny_log[unit_of_work.FILE_NAME] = file_name
                tiny_log[unit_of_work.MD5] = self._get_md5(os.path.join(fqsf, file_name))
                tiny_log[unit_of_work.NUMBER_OF_PROCESSED_DOCUMENTS] = number_of_processed_docs
                processed_log[file_name.replace('.', '-')] = tiny_log

            self.perform_post_processing(uow.start_timeperiod)

            uow.number_of_aggregated_documents = number_of_aggregated_objects
            uow.number_of_processed_documents = self.performance_ticker.per_job
            uow.finished_at = datetime.utcnow()
            uow.state = unit_of_work.STATE_PROCESSED
            self.uow_dao.update(uow)
            self.performance_ticker.finish_uow()
        except Exception as e:
            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'
                              % (message.body, uow.timeperiod, e), exc_info=True)

            uow.state = unit_of_work.STATE_INVALID
            self.uow_dao.update(uow)
            self.performance_ticker.cancel_uow()

        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self._clean_directories()
            self.consumer.close()

    def _create_directories(self):
        """ method creates temporary directories:
          - to store files copied from remote locations to local filesystem
          - uncompressed files """
        self.tempdir_copying = tempfile.mkdtemp()

    def _clean_directories(self):
        """ method verifies if temporary folders exists and remove them (and nested content) """
        if self.tempdir_copying:
            self.logger.info('Cleaning up %r' % self.tempdir_copying)
            shutil.rmtree(self.tempdir_copying, True)
            self.tempdir_copying = None

    def _get_md5(self, file_name):
        """ method traverses compressed file and calculates its MD5 checksum """
        md5 = hashlib.md5()
        file_obj = gzip.open(file_name, 'rb')
        for chunk in iter(lambda: file_obj.read(8192), ''):
            md5.update(chunk)

        file_obj.close()
        return md5.hexdigest()
