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

def get_sample_df(number_of_records: int, first_ts: datetime, time_interval: int) -> tuple[pd.DataFrame, datetime]:
    # Atualmente ignora-se o parâmetro number_of_records.abs
    # TODO: Implementar o uso do parâmetro number_of_records que atualmente é sempre 4
    # Atualmente ignora-se o parâmetro time_interval
    # TODO: Implementar o uso do parâmetro time_interval que atualmente é sempre 1 (hora)
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
    # np.float32 e np.float32 respectivamente, pois o padrão é np.float64 e np.int64

    temperature_typed_df: np.ndarray = np.array(data['temperatura'], dtype=np.float32)
    data['temperatura'] = temperature_typed_df
    velocity_typed_df: np.ndarray = np.array(data['velocidade'], dtype=np.float32)
    data['velocidade'] = velocity_typed_df
    df = pd.DataFrame(data)
    last_timestamp: datetime = data['timestamp'][-1] # type: ignore
    return (df, last_timestamp)
