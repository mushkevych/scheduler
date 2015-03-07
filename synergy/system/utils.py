from __future__ import print_function

__author__ = 'Bohdan Mushkevych'

import os
import sys
import gzip
import hashlib

from synergy.conf import settings
from synergy.conf import context


def fully_qualified_table_name(table_name):
    # fully qualified table name
    fqtn = settings.settings['aws_redshift_orca_prefix'] + table_name + settings.settings['aws_redshift_orca_suffix']
    return fqtn


def create_s3_file_uri(s3_bucket, timeperiod, file_name):
    return 's3://{0}/{1}/{2}'.format(s3_bucket, timeperiod, file_name)


def break_s3_file_uri(fully_qualified_file):
    """
    :param fully_qualified_file: in form s3://{0}/{1}/{2}
    where {0} is bucket name, {1} is timeperiod and {2} - file name
    :return: tuple (s3://{0}, {1}/{2})
    """
    tokens = fully_qualified_file.rsplit('/', 2)
    return tokens[0], '{0}/{1}'.format(tokens[1], tokens[2])


def unicode_truncate(s, length, encoding='utf-8'):
    encoded = s.encode(encoding)[:length]
    return encoded.decode(encoding, errors='ignore')


def compute_gzip_md5(file_name):
    """ method traverses compressed file and calculates its MD5 checksum """
    md5 = hashlib.md5()
    file_obj = gzip.open(file_name, 'rb')
    for chunk in iter(lambda: file_obj.read(8192), ''):
        md5.update(chunk)

    file_obj.close()
    return md5.hexdigest()


def increment_family_property(key, family):
    if key is None:
        return

    if not isinstance(key, basestring):
        key = str(key)

    if key in family:
        family[key] += 1
    else:
        family[key] = 1


def copy_and_sum_families(family_source, family_target):
    """ methods iterates thru source family and copies its entries to target family
    in case key already exists in both families - then the values are added"""
    for every in family_source:
        if every not in family_target:
            family_target[every] = family_source[every]
        else:
            family_target[every] += family_source[every]


def get_pid_filename(process_name):
    """method returns path for the PID FILENAME """
    return settings.settings['pid_directory'] + context.process_context[process_name].pid_filename


def create_pid_file(process_name):
    """ creates pid file and writes os.pid() in there """
    pid_filename = get_pid_filename(process_name)
    try:
        with open(pid_filename, mode='w') as pid_file:
            pid_file.write(str(os.getpid()))
    except Exception as e:
        print('Unable to create pid file at: {0}, because of: {1}'.format(pid_filename, e),
              file=sys.stderr)


def remove_pid_file(process_name):
    """ removes pid file """
    pid_filename = get_pid_filename(process_name)
    try:
        os.remove(pid_filename)
        print('Removed pid file at: {0}'.format(pid_filename), file=sys.stdout)
    except Exception as e:
        print('Unable to remove pid file at: {0}, because of: {1}'.format(pid_filename, e),
              file=sys.stderr)
