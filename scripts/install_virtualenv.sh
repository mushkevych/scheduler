#!/bin/bash

if [ -z "$1" ]; then
    echo "Need path to project root"
    exit 1
fi

if [ -z "$2" ]; then
    echo "Need path to virtual environment to install virtualenv"
    exit 1
fi

. $2/bin/activate

vendor=$1/vendors
cd $vendor
easy_install $vendor/amqp-1.4.5.tar.gz
easy_install $vendor/virtualenv-1.10.1.tar.gz
easy_install $vendor/setuptools-1.4.tar.gz
easy_install $vendor/pip-1.4.1.tar.gz
easy_install $vendor/ipython-1.1.0.tar.gz

easy_install $vendor/unittest2-0.5.1.tar.gz
easy_install $vendor/nose-1.3.0.tar.gz
easy_install $vendor/mock-1.0.1.tar.gz
easy_install $vendor/distribute-0.7.3.zip
easy_install $vendor/mockito-0.5.2.tar.gz
easy_install $vendor/logilab-common-0.60.0.tar.gz
easy_install $vendor/logilab-astng-0.24.3.tar.gz
easy_install $vendor/astroid-1.0.1.tar.gz
easy_install $vendor/pylint-1.0.0.tar.gz

easy_install $vendor/coverage-3.7.tar.gz
easy_install $vendor/unittest-xml-reporting-1.7.0.tar.gz

easy_install $vendor/pyrepl-0.8.4.tar.gz
easy_install $vendor/fancycompleter-0.4.tar.gz
easy_install $vendor/Pygments-1.6.tar.gz
easy_install $vendor/wmctrl-0.1.tar.gz
easy_install $vendor/pdbpp-0.7.2.tar.gz

easy_install $vendor/docutils-0.11.tar.gz
easy_install $vendor/pysmell-0.7.3.zip

# Fabric section
easy_install $vendor/ecdsa-0.10.tar.gz
easy_install $vendor/pycrypto-2.6.1.tar.gz
easy_install $vendor/paramiko-1.12.1.tar.gz
easy_install $vendor/Fabric-1.8.1.tar.gz

easy_install $vendor/pymongo-2.6.3.tar.gz
easy_install $vendor/ftputil-3.0.tar.gz
easy_install $vendor/setproctitle-1.1.8.tar.gz
easy_install $vendor/httplib2-0.8.tar.gz
easy_install $vendor/python_rest_client2.tar.gz
easy_install $vendor/MarkupSafe-0.18.tar.gz
easy_install $vendor/Jinja2-2.7.1.tar.gz
easy_install $vendor/Werkzeug-0.9.4.tar.gz
easy_install $vendor/psutil-1.2.0.tar.gz
