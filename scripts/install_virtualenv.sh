#!/bin/bash

# List of packages to install
packagelist=(
    "six-1.9.0.tar.gz"
    "pip-6.0.8.tar.gz"
    "ipython-3.0.0.tar.gz"

    # pylint section start
    "logilab-common-0.63.2.tar.gz"
    "logilab-astng-0.24.3.tar.gz"
    "astroid-1.3.5.tar.gz"
    "pylint-1.4.1.tar.gz"

    "coverage-4.0a5.tar.gz"
    "unittest-xml-reporting-1.11.0.tar.gz"
    "setproctitle-1.1.8.tar.gz"
    "psutil-2.2.1.tar.gz"
    "amqp-1.4.5.tar.gz"
    
    "pyrepl-0.8.4.tar.gz"
    "fancycompleter-0.4.tar.gz"
    "Pygments-1.6.tar.gz"
    "wmctrl-0.1.tar.gz"
    "pdbpp-0.7.2.tar.gz"
    "docutils-0.11.tar.gz"
    "pysmell-0.7.3.zip"

    # Fabric section
    "ecdsa-0.10.tar.gz"
    "pycrypto-2.6.1.tar.gz"
    "paramiko-1.12.1.tar.gz"
    "Fabric-1.8.1.tar.gz"

    "pymongo-2.6.3.tar.gz"
    "ftputil-3.0.tar.gz"
    "httplib2-0.8.tar.gz"
    "python_rest_client2.tar.gz"
    "MarkupSafe-0.23.tar.gz"
    "Jinja2-2.7.3.tar.gz"
    "Werkzeug-0.10.4.tar.gz"
    "synergy_odm-0.7.tar.gz"
)

if [ -z "$1" ]; then
    echo "Parameter #1 is missing: path to project root"
    exit 1
fi

if [ -z "$2" ]; then
    echo "Parameter #2 is missing: path to target virtual environment"
    exit 1
fi

if [ -z "$3" ]; then
    echo "Parameter #3 is missing: Python major version"
    exit 1
fi

if [[ $3 == 2* ]]; then
    easy_install_bin="easy_install"

    # adding python2 specific packages
    packagelist=("virtualenv-12.0.7.tar.gz" "setuptools-14.0.tar.gz" "distribute-0.7.3.zip"
                 "unittest2-1.0.1.tar.gz" "nose-1.3.4.tar.gz" "mock-1.0.1.tar.gz" "${packagelist[@]}")
elif [[ $3 == 3* ]]; then
    export PYTHONPATH="$2/lib/python$3/site-packages/"
    easy_install_bin="easy_install3 --prefix=$2"
else
    echo "Python version $3 is not yet supported"
    exit 1
fi

# ccache speeds up recompilation by caching previous compilations
which ccache > /dev/null 2>&1
if [ $? == 0 ]; then
    export CC="ccache gcc"
    export CXX="ccache g++"    
fi

# Ignore some CLANG errors on OSX else install will fail
if [ `uname` == "Darwin" ]; then
    export ARCHFLAGS="-arch i386 -arch x86_64"
    export CFLAGS=-Qunused-arguments
    export CPPFLAGS=-Qunused-arguments
fi

. $2/bin/activate

vendor=$1/vendors
cd ${vendor}

for package in "${packagelist[@]}"; do   # The quotes are necessary here
    ${easy_install_bin} ${vendor}/${package}
done
