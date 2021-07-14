#!/bin/bash

packagelist=(
    "six"
    "pip"
    "ordereddict"

    # ipython
    "more-itertools"
    "path.py"
    "ipython_genutils"
    "ptyprocess"
    "decorator"
    "pathlib2"
    "pickleshare"
    "simplegeneric"
    "traitlets"
    "pexpect"
    "wcwidth"
    "prompt_toolkit"
    "Pygments"
    "ipython"

    # pylint section start
    "configparser"
    "mccabe"
    "isort"
    "lazy-object-proxy"
    "wrapt"
    "astroid"
    "pylint"

    "coverage"
    "unittest-xml-reporting"
    "setproctitle"
    "psutil"

    # Amqp
    "vine"
    "amqp"

    "pymongo"
    "MarkupSafe"
    "Jinja2"
    "Werkzeug"
    "synergy_odm"
    "synergy_flow --no-deps"

    # fabric
    "invoke"
    "fabric"
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

if [[ $3 != 3* ]]; then
    echo "Python version $3 is not supported"
    exit 1
fi

pip_bin="pip3 install --prefix=$2"
echo "DEBUG: pip_bin=${pip_bin}"

# ccache speeds up recompilation by caching previous compilations
if command -v ccache > /dev/null 2>&1; then
    export CC="ccache gcc"
    export CXX="ccache g++"    
fi

# Ignore some CLANG errors on OSX else install will fail
if [[ $(uname) == "Darwin" ]]; then
    export ARCHFLAGS="-arch i386 -arch x86_64"
    export CFLAGS=-Qunused-arguments
    export CPPFLAGS=-Qunused-arguments
fi

. "${2}/bin/activate"

for package in "${packagelist[@]}"; do   # The quotes are necessary here

    NO_DEP_FLAG=''
    if [[ ${package} =~ .*"--no-deps".* ]]; then
        package=$(echo ${package} | awk '{print $1}')
        NO_DEP_FLAG='--no-deps'
    fi

    package=$(ls ${1}/vendors/${package}*)
    echo "DEBUG: ${package}"
    ${pip_bin} ${package} ${NO_DEP_FLAG}
done
