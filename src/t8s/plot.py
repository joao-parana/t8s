# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Protocol
import pandas as pd
import matplotlib.pyplot as plt # type: ignore
from t8s.interfaces import TimeSerie

class TSPlotting:
    def __init__(self, ts: TimeSerie, **kwargs):
        self.ts = ts
        self.__kwargs = kwargs

    def line(self, **kwargs):
        df = self.to_new_df()
        # use `self.__kwargs` and `args` to decide what and how to plot
        time_col = str(df.columns[0])
        # ax = df.plot(kind='line', x='t', y=['y1', 'y2'])
        for chave, valor in kwargs.items():
            print(f'chave = {chave}: valor = {valor} -> tipo = {type(valor)}')

        features = [ x for x in df.columns[1:] ]
        ax = df.plot(kind='line', x=time_col, y=features, figsize=(12, 5), grid=True)

        plt.show()

    def scatter(self, **kwargs):
        pass

    def bar(self, **kwargs):
        pass

    def hist(self, **kwargs):
        pass

    def box(self, **kwargs):
        pass

    def stackplot(self, **kwargs):
        pass

    # Retorna uma cópia do Dataframe para ser usada nos gráficos sem afetar
    # o objeto original.
    def to_new_df(self) -> pd.DataFrame:
        if self.ts.format == 'wide':
            return self.ts.df.copy()
        else:
            # Atenção: o método to_wide() altera o objeto ts original, por isso faço uma deep-copy antes.
            ts_copy = self.ts.copy()
            ts_copy.to_wide()
            result: TimeSerie = ts_copy
            return result.df
