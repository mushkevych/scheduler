__author__ = 'Bohdan Mushkevych'

import gzip
import hashlib
from synergy.conf import settings


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
