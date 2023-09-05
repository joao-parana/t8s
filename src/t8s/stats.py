# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Protocol
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from pandas.core.series import Series
from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

class TSStats:
    def __init__(self, df: pd.DataFrame):
        assert isinstance(df, pd.DataFrame), "df must be a Pandas DataFrame"
        # Obtendo o resumo estatístico do DataFrame
        summary_en_us: pd.DataFrame = df.describe()
        self.summary_en_us = summary_en_us
        # Renomeando os índices do DataFrame resultante para PT_BR
        summary_pt_br = summary_en_us.rename(index={'count': 'Contagem', 'mean': 'Média', 'std': 'Desvio padrão', 'min': 'Mínimo', '25%': 'Primeiro quartil', '50%': 'Mediana', '75%': 'Terceiro quartil', 'max': 'Máximo'})
        self.summary_pt_br: pd.DataFrame = summary_pt_br
        logger.info(f'summary_pt_br =\n{self.summary_pt_br}\n')

    def __str__(self) -> str:
        return str(self.summary_pt_br)

    # obtem a contagem de elementos na coluna `column_name`
    def count(self, column_name:str) -> float:
        # Extraindo o valor da quantidade de elementos na coluna `column_name`
        return float(self.summary_en_us.loc['count', column_name]) # type: ignore

    # obtem a média dos elementos na coluna `column_name`
    def mean(self, column_name:str) -> float:
        # Extraindo o valor da média dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['mean', column_name]) # type: ignore

    # obtem o desvio padrão dos elementos na coluna `column_name`
    def std(self, column_name:str) -> float:
        # Extraindo o valor do desvio padrão dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['std', column_name]) # type: ignore

    # obtem o valor mínimo dos elementos na coluna `column_name`
    def min(self, column_name:str) -> float:
        # Extraindo o valor mínimo dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['min', column_name]) # type: ignore

    # obtem o primeiro quartil dos elementos na coluna `column_name`
    def q1(self, column_name:str) -> float:
        # Extraindo o valor do primeiro quartil dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['25%', column_name]) # type: ignore


    # obtem a mediana dos elementos na coluna `column_name`
    def median(self, column_name:str) -> float:
        # Extraindo o valor da mediana dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['50%', column_name]) # type: ignore

    # obtem o segundo quartil dos elementos na coluna `column_name`
    def q2(self, column_name:str) -> float:
        # Extraindo o valor do segundo quartil dos elementos na coluna `column_name`
        return self.median(column_name)

    # obtem o terceiro quartil dos elementos na coluna `column_name`
    def q3(self, column_name:str) -> float:
        # Extraindo o valor do terceiro quartil dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['75%', column_name]) # type: ignore

    # obtem o valor máximo dos elementos na coluna `column_name`
    def max(self, column_name:str) -> float:
        # Extraindo o valor máximo dos elementos na coluna `column_name`
        return float(self.summary_en_us.loc['max', column_name]) # type: ignore

    # obtem o valor da amplitude dos elementos na coluna `column_name`
    def amplitude(self, column_name:str) -> float:
        # Extraindo o valor da amplitude dos elementos na coluna `column_name`
        return self.max(column_name) - self.min(column_name)

