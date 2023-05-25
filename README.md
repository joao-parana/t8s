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

## Testing and publishing

```batch
# Para inspecionar a configuração do ambiente de testes:
hatch config show
hatch clean
hatch build
# Edit your main.py code
hatch run python3 main.py
./test-all.sh
hatch publish
```

## License

`t8s` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
