#!/bin/bash

# List of packages to install
python2list=(
    "linecache2-1.0.0.tar.gz"
    "traceback2-1.4.0.tar.gz"
    "six-1.12.0.tar.gz"
    "virtualenv-15.1.0.tar.gz"
    "setuptools-28.8.1.tar.gz"

    "distribute-0.7.3.zip"
    "unittest2-1.1.0.tar.gz"
    "pbr-3.1.1.tar.gz"

    # mock section
    "funcsigs-1.0.2.tar.gz"
    "mock-2.0.0.tar.gz"

    # seems to be replaced by the Python3 libs
    "pyrepl-0.8.4.tar.gz"
    "fancycompleter-0.8.tar.gz"
    "wmctrl-0.3.tar.gz"
    "pdbpp-0.9.2.tar.gz"
    "docutils-0.14.tar.gz"
    "pysmell-0.7.3.zip"

    "nose-1.3.7.tar.gz"
)

python3list=(
    "six-1.12.0.tar.gz"
)

commonlist=(
    "pip-19.1.1.tar.gz"
    "ordereddict-1.1.tar.gz"

    # ipython
    "path.py-12.0.1.tar.gz"
    "ipython_genutils-0.1.0.tar.gz"
    "ptyprocess-0.6.0.tar.gz"
    "decorator-4.4.0.tar.gz"
    "pathlib2-2.3.4.tar.gz"
    "pickleshare-0.7.5.tar.gz"
    "simplegeneric-0.8.1.zip"
    "traitlets-4.3.2.tar.gz"
    "pexpect-4.7.0.tar.gz"
    "backports.shutil_get_terminal_size-1.0.0.tar.gz"
    "wcwidth-0.1.7.tar.gz"
    "prompt_toolkit-1.0.7.tar.gz"
    "Pygments-2.2.0.tar.gz"
    "ipython-7.6.1.tar.gz"

    # pylint section start
    "backports.functools_lru_cache-1.4.tar.gz"
    "configparser-3.7.4.tar.gz"
    "mccabe-0.6.1.tar.gz"
    "isort-4.3.21.tar.gz"
    "lazy-object-proxy-1.4.1.tar.gz"
    "wrapt-1.11.2.tar.gz"
    "astroid-2.2.5.tar.gz"
    "pylint-2.3.1.tar.gz"

    "coverage-4.5.3.tar.gz"
    "unittest-xml-reporting-2.5.1.tar.gz"
    "setproctitle-1.1.10.tar.gz"
    "psutil-5.6.3.tar.gz"

    # Amqp
    "vine-1.1.4.tar.gz"
    "amqp-2.5.0.tar.gz"

    "splunk-sdk-python-1.6.6.zip"
    "pymongo-3.8.0.tar.gz"
    "MarkupSafe-1.1.1.tar.gz"
    "Jinja2-2.10.1.tar.gz"
    "Werkzeug-0.15.4.tar.gz"
    "synergy_odm-0.9.tar.gz"
    "synergy_flow-0.9.tar.gz"
)

if [[ -z "$1" ]]; then
    echo "Parameter #1 is missing: path to project root"
    exit 1
fi

if [[ -z "$2" ]]; then
    echo "Parameter #2 is missing: path to target virtual environment"
    exit 1
fi

if [[ -z "$3" ]]; then
    echo "Parameter #3 is missing: Python major version"
    exit 1
fi

if [[ $3 == 2* ]]; then
    easy_install_bin="easy_install-$3"
    packagelist=("${python2list[@]}" "${commonlist[@]}")
elif [[ $3 == 3* ]]; then
    export PYTHONPATH="$2/lib/python$3/site-packages/"
    easy_install_bin="easy_install-$3 --prefix=$2"
    packagelist=("${python3list[@]}" "${commonlist[@]}")
else
    echo "Python version $3 is not yet supported"
    exit 1
fi

echo "DEBUG: PYTHONPATH=${PYTHONPATH}"
echo "DEBUG: easy_install_bin=${easy_install_bin}"

# ccache speeds up recompilation by caching previous compilations
which ccache > /dev/null 2>&1
if [[ $? == 0 ]]; then
    export CC="ccache gcc"
    export CXX="ccache g++"    
fi

# Ignore some CLANG errors on OSX else install will fail
if [[ `uname` == "Darwin" ]]; then
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
