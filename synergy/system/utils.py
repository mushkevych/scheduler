__author__ = 'Bohdan Mushkevych'

import os
import sys
import gzip
import hashlib
from collections import deque

from synergy.conf import settings
from synergy.conf import context


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


def compute_gzip_md5(fqfn):
    """ method traverses compressed file and calculates its MD5 checksum """
    md5 = hashlib.md5()
    file_obj = gzip.open(fqfn, 'rb')
    for chunk in iter(lambda: file_obj.read(8192), ''):
        md5.update(chunk)

    file_obj.close()
    return md5.hexdigest()


def increment_family_property(key, family):
    if key is None:
        return

    if not isinstance(key, str):
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


def ensure_dir(fqdp):
    """
    :param fqdp: fully qualified directory path
    """
    if os.path.isdir(fqdp):
        # directory exists - nothing to do
        return

    try:
        print(f'Attempting to create a dirs: {fqdp}...', file=sys.stdout)
        os.makedirs(fqdp)
        print(f'Path {fqdp} created successfully', file=sys.stdout)
    except OSError as e:
        print(f'Unable to create path: {fqdp}, because of: {e}', file=sys.stderr)


def get_pid_filename(process_name):
    """method returns path for the PID FILENAME """
    return os.path.join(settings.settings['pid_directory'], context.process_context[process_name].pid_filename)


def create_pid_file(process_name):
    """ creates pid file and writes os.pid() in there """
    pid_filename = get_pid_filename(process_name)
    try:
        with open(pid_filename, mode='w') as pid_file:
            pid_file.write(str(os.getpid()))
    except Exception as e:
        print(f'Unable to create pid file at: {pid_filename}, because of: {e}', file=sys.stderr)


def remove_pid_file(process_name):
    """ removes pid file """
    pid_filename = get_pid_filename(process_name)
    if not os.path.exists(pid_filename):
        # pid file does not exist - nothing to do
        return

    try:
        os.remove(pid_filename)
        print(f'Removed pid file at: {pid_filename}', file=sys.stdout)
    except Exception as e:
        print(f'Unable to remove pid file at: {pid_filename}, because of: {e}', file=sys.stderr)


def tail_file(fqfn, num_lines=128):
    with open(fqfn) as log_file:
        dq = deque(log_file, maxlen=num_lines)
        return [l.replace('\n', '') for l in dq]
