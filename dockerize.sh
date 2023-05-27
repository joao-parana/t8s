#!/bin/bash

set -e

# The `--editable` flag is used to install the package in "editable" mode, which
# means that the package is installed in such a way that changes to the source code
# will be reflected in the installed package without needing to reinstall it.
python -m pip install --editable .
python -m pip freeze --exclude-editable > constraints.txt
# Reformat Python code source
python -m black .

# Build the package and publish to PyPi.org
hatch clean
hatch build
hatch publish

# Get the version of the t8s package
T8S_VERSION=`hatch run python -c "import t8s ; print(t8s.__version__)"`
# Build the docker image with proper TAG
docker build -t $ARTIFACT_REGISTRY/t8s:$T8S_VERSION .

