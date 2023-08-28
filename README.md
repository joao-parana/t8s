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
st graphics/graph-01.py
st graphics/graph-02.py
```

And open the URI in browser

## Virtual Environment

To generate `requirements.txt` from zero, use this:

```bash
python3 -m venv .venv
source .venv/Scripts/activate
# Install packages
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pyright
python3 -m pip install tensorflow tensorflow-metadata tensorflow-datasets
python3 -m pip install -e .
# Freeze instalation
python3 -m pip freeze > requirements.txt
# Do your job
# . . .
#
deactivate
rm -rf .venv/*
```

## License

`t8s` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
