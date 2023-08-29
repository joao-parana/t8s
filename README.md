# t8s

[![PyPI - Stable Version](https://img.shields.io/pypi/v/t8s.svg)](https://pypi.org/project/t8s)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/t8s.svg)](https://pypi.org/project/t8s)
[![Downloads](https://img.shields.io/pypi/dm/t8s)](https://pypistats.org/packages/t8s)
[![Build Status](https://github.com/joao-parana/t8s/actions/workflows/test.yml/badge.svg)](https://github.com/joao-parana/t8s/actions)
[![Documentation Status](https://readthedocs.org/projects/t8s/badge/?version=latest)](https://t8s.readthedocs.io/en/latest/?badge=latest)

-----

**Table of Contents**

- [Installation](#installation)
- [Testing](#testing)
- [Publishing](#publishing)
- [Graphics](#graphics)
- [Virtual Environment](#virtual-environment)
- [License](#license)

## Installation

```bash
pip install t8s
# Edit your code using t8s.ts.TimeSerie and others related classes
```

Check Linter rules using **PyRight** (https://microsoft.github.io/pyright)

```bash
pyright --level warning .
```

## Testing

```bash
# To configure the test environment, set the T8S_WORKSPACE_DIR environment variable, for example:
export T8S_WORKSPACE_DIR=/Volumes/dev/t8s
```

![BDD](docs/bdd.png)

See too [BDD](docs/behave.md)

```batch
# To inspect the test environment configuration:
hatch config show
hatch clean
hatch build
# Edit your main.py code
hatch run python3 main.py
./test-all.sh
# Using BDD with behave (https://behave.readthedocs.io/en/latest/)
rm logs/timeseries.log
hatch run python -m behave --logging-level INFO --no-capture --no-capture-stderr --no-skipped features
cat logs/timeseries.log
```

## Publishing

```bash
hatch publish
```

## Graphics

Execute the examples below:

```bash
alias st='streamlit run  --server.headless true --theme.base light '
# This start code below is referencing the file graph-01.py and graph-02.py
st graphics/graph-01.py
st graphics/graph-02.py
# You can use Raw URI from GitHub repository like this:
# st https://raw.githubusercontent.com//joao-parana/...whatever-file.py
```

And open the URI in browser

For information on **caching** see [https://docs.streamlit.io/library/advanced-features/caching](https://docs.streamlit.io/library/advanced-features/caching)
## Virtual Environment

To generate `requirements.txt` from zero, with **Python 3.10** and
**TensorFlow 2.10.1**, in Windows 10 for GPU support, use this:

```bash
curl -O https://www.python.org/ftp/python/3.10.11/python-3.10.11-embed-amd64.zip
# Move files to /e/usr/local/python-3.10/
alias python3.10=''
/e/usr/local/python-3.10/python.exe -m venv .venv
source .venv/Scripts/activate
alias python3.10=".venv/Scripts/python"
# Install packages
python3.10 -m pip install --upgrade pip
python3.10 -m pip install --upgrade pyright
python3.10 -m pip install tensorflow==2.10.1 # tensorflow-metadata tensorflow-datasets
python3.10 -c "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"
python3.10 -m pip install -e .
# Freeze instalation
python3.10 -m pip freeze > requirements.txt
# Do your job
# . . .
#
deactivate
rm -rf .venv/*
```

## License

`t8s` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
