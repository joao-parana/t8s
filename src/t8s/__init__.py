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
from datetime import datetime
import numpy as np
import pandas as pd


def get_numeric_regex():
    # Definindo a expressão regular para identificar números de ponto flutuante
    pattern = r'[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?'
    return re.compile(pattern)

def get_sample_df(first_ts) -> tuple[pd.DataFrame, datetime]:
    print('first_ts', first_ts)
    data = {
        'timestamp': [
            first_ts,
            first_ts + pd.Timedelta(hours=1),
            first_ts + pd.Timedelta(hours=2),
            first_ts + pd.Timedelta(hours=3)
        ],
        'temperatura': np.array([25.0, 26.0, 27.0, 23.2], dtype=np.float32),
        'velocidade': [3000, 1100, 1200, 4000],
    }
    # Convertendo os tipos de dado para temperatura e velocidade para
    # np.float32 e np.int32 respectivamente, pois o padrão é np.float64 e np.int64
    data['temperatura'] = np.array(data['temperatura'], dtype=np.float32)
    data['velocidade'] = np.array(data['velocidade'], dtype=np.int32)
    df = pd.DataFrame(data)
    return (df, data['timestamp'][-1])
