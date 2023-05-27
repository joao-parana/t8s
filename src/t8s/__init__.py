# SPDX-FileCopyrightText: 2023-present João Antonio Ferreira <joao.parana@gmail.com>
#
# SPDX-License-Identifier: MIT
from .__about__ import __version__

# from abc import ABC, abstractmethod
#
# class ITimeSerie(ABC):
#    pass

import re
from enum import Enum


def get_numeric_regex():
    # Definindo a expressão regular para identificar números de ponto flutuante
    pattern = r'[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?'
    return re.compile(pattern)
