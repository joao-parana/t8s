# -*- coding: utf-8 -*-

from __future__ import annotations

import copy
import types
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Protocol, Type, TypeVar

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import yaml
from pandas.core.series import Series
from sklearn.base import TransformerMixin  # type: ignore
from sklearn.preprocessing import (  # type: ignore
    MinMaxScaler,
    RobustScaler,
    StandardScaler,
)

from t8s.log_config import LogConfig
from t8s.plot import TSPlotting
from t8s.stats import TSStats

# TODO: Esclarecer duvida coceitual sobre o conceito de feture em séries temporais multivariadas

logger = LogConfig().get_logger()

TS = TypeVar('TS', bound='TimeSerie')

# ts = pd.read_parquet('lixo.parquet', engine='pyarrow')


# Classe abstrata que define a interface para os processamentos de séries temporais
class ITimeSeriesProcessor(ABC):
    def __init__(self):
        self.format: str
        self.features: str
        self.df: pd.DataFrame

    """Converte a série temporal para o formato Long"""

    @abstractmethod
    def to_long(self) -> bool:
        return False

    @abstractmethod
    def to_wide(self) -> None:
        # Converte a série temporal para o formato Wide
        return False

    @abstractmethod
    def is_univariate(self) -> bool:
        # Verifica se a série temporal é univariada
        return False

    @abstractmethod
    def is_multivariate(self) -> bool:
        # Verifica se a série temporal é multivariada
        return False

    @abstractmethod
    def split(self) -> list[Type['TimeSerie']]:
        # Alternativas para anotar o tipo de retorno:
        # list[TS], list['TimeSerie'] ou list[Optional['TimeSerie']]
        # Cria várias séries temporais univariadas à partir de uma série temporal multivariada
        pass

    @abstractmethod
    def normalize(
        self, scaler: TransformerMixin, columns: list[str], inplace: bool = False
    ) -> TimeSerie:
        pass

    @abstractmethod
    def get_statistics(self) -> TSStats:
        pass

    @staticmethod
    @abstractmethod
    def join(list_of_ts: list['TimeSerie']) -> TimeSerie:
        pass


"""Rastreia a proveniência dos ativos de informação, Série Temporal neste caso"""


class IProvenanceable(ABC):
    def __init__(self):
        self.format: str
        self.features: str
        self.df: pd.DataFrame

    """Este método adiciona informações de proveniência ao objeto TimeSerie para
    uma transformação específica. Ele recebe como parâmetros o nome da transformação
    e um dicionário com os parâmetros usados na transformação."""

    @abstractmethod
    def add_provenance(self, transformation: str, parameters: dict):
        pass

    @abstractmethod
    def get_provenances(self) -> list[dict[str, Any]]:
        """Este método retorna uma lista de dicionários com as informações de
        proveniência de todas as transformações realizadas no objeto TimeSerie."""
        pass

    @abstractmethod
    def get_provenance_by_transformation(self, transformation: str) -> dict[str, Any]:
        """Este método retorna as informações de proveniência de uma dada transformação
        realizadas no objeto TimeSerie."""
        pass

    @abstractmethod
    def get_last_transformation(self, transformation: str) -> dict[str, Any]:
        """Este método retorna informações da ultima transformação realizada no objeto TimeSerie."""
        pass

    @abstractmethod
    def apply_transformation(self, transformation: str, **kwargs) -> Any:
        """Este método aplica uma transformação ao objeto TimeSerie e retorna um novo
        objeto TimeSerie com as informações de proveniência atualizadas. Ele recebe como
        parâmetros o nome da transformação e os parâmetros específicos da transformação.
        """
        pass


class ITimeSerie(ITimeSeriesProcessor, IProvenanceable):
    """
    Interface que estende as outras interfaces. ë apenas uma Marker Interface
    """

    pass


class TimeSerie(ITimeSerie):
    """
    Num contexto de aprendizado de máquina, a coluna de tempo em um conjunto de dados
    de série temporal (univariada ou multivariada) também pode ser considerada uma feature.
    Isso porque a coluna de tempo pode ser usada como uma variável independente para prever
    valores futuros das outras variáveis dependentes no conjunto de dados. Assim features_qty
    deve contabilizar todas as features do conjunto de dados, incluindo a coluna de tempo e
    excluindo colunas alvo (caso existam).
    O formato esperado para o Dataset de origem é composto pela coluna de tempo na primeira
    coluna e as colunas alvo, ao final. Todas as features ficam no meio do Dataset. O motivo
    é simplesmente uma simplificação para facilitar a implementação.
    """

    def __init__(self, *args, format, features_qty, **kwargs):
        assert isinstance(format, str), "format must be a string"
        assert isinstance(features_qty, int), "features_qty must be a int"
        assert format in ['long', 'wide'], "format must be 'long' or 'wide'"
        self.format: str = format
        self.features: str = str(features_qty)
        self.df = pd.DataFrame()
        # scaler indica se dados já foram normalizados e qual foi a classe
        # da implementação do scaler usado na normalização.
        self.scaler: TransformerMixin | None = None
        if len(args) > 0:
            for idx, arg in enumerate(args):
                if idx == 0 and isinstance(arg, pd.DataFrame):
                    self.df = arg
                if idx == 0 and isinstance(arg, dict):
                    if 'content' in arg.keys() and arg['content'] == 'EMPTY':
                        return
                    self.df = pd.DataFrame(*args, **kwargs)
                break  # Apenas o primeiro parâmetro precisa de tratamento especial.
        else:
            for key, kwarg_value in kwargs.items():
                if key == 'data' and isinstance(kwarg_value, pd.DataFrame):
                    self.df = kwarg_value
                if key == 'data' and isinstance(kwarg_value, dict):
                    self.df = pd.DataFrame(*args, **kwargs)
                if (
                    key == 'data'
                    and isinstance(kwarg_value, str)
                    and kwarg_value == 'EMPTY'
                ):
                    return
                break  # Apenas o parâmetro 'data' precisa de tratamento especial.

        # TODO: garantir que a primeira coluna seja um Timestamp tanto para o formato long quanto wide
        # Por hora lanço Exception
        if self.df.empty:
            raise Exception(
                'Não foram fornecidos dados para criação da série temporal. Use o método TimeSerie.empty() se desejar criar uma série temporal vazia'
            )
        first_column_type = self.df[self.df.columns[0]].dtype
        if first_column_type not in [
            'datetime64[ns]',
            'datetime64[ns, America/Sao_Paulo]',
        ]:
            # TODO: Investigar forma de loggar os tipos de dados e detalhes do DataFrame
            logger.info(self.df.info())
            logger.error(self.df.info())
            raise Exception(
                'A primeira coluna deve ser um Timestamp.'
                + ' Experado '
                + 'datetime64[ns]'
                + ', recebido '
                + str(first_column_type)
                + f'\nself.df.info() =\n{self.df.info()}\n'
            )
        logger.debug('Objeto TimeSerie construido com sucesso')

    def __repr__(self):
        columns_types = [type(self.df[col][0]) for col in self.df.columns]
        # Retorna uma representação em string do objeto TimeSerie
        return (
            f'TimeSerie(format={self.format}, features={self.features}, '
            + f'df=\n{self.df.__repr__()}) + \ntypes: {columns_types}'
        )

    def copy(self):
        # cria uma cópia profunda do DataFrame
        df_copy = self.df.copy(deep=True)

        # clona o objeto TransformerMixin
        scaler_copy = copy.deepcopy(self.scaler)

        # cria uma nova instância do objeto TimeSerie com as cópias profundas
        result = TimeSerie(df_copy, format=self.format, features_qty=int(self.features))
        result.scaler = scaler_copy
        return result

    def to_long(self):
        # Converte a série temporal para o formato Long
        # Considerando que na série temporal a ser convertida a primeira coluna é o
        # timestamp e as outras são as features. A série transformada "inplace" terá
        # 3 colunas: `timestamp`, `ds` e `value`, com `ds` sendo o nome ou id do `datasource`.
        # Em algumas situações `ds` pode ser o id do par `datasource/indicator`.
        assert self.format == 'wide', 'A série temporal deve estar no formato wide'
        first_column_name = self.df.columns[0]
        df_long_format = pd.melt(
            self.df, id_vars=[first_column_name], var_name='ds', value_name='value'
        )
        # Ordena o DataFrame pela coluna 'timestamp' em ordem crescente
        df_long_format.sort_values(by=['timestamp', 'ds'], inplace=True)
        # logger.debug(df_long_format)
        self.df = df_long_format
        self.format = 'long'
        # logger.debug(self)

    # Atenção: este é um método mutável e não retirna nada pois modifica o objeto TimeSerie original
    def to_wide(self) -> None:
        # Converte a série temporal para o formato Wide
        # Converte o DataFrame do formato long para o formato wide
        df_wide_format = self.df.pivot(index='timestamp', columns='ds', values='value')
        # logger.debug(f'Conversão para formato wide: \n{df_wide_format}')
        self.df = df_wide_format
        self.format = 'wide'
        # logger.debug('\n' + str(self))

    def is_univariate(self) -> bool:
        # Verifica se a série temporal é univariada
        if self.format == 'long':
            # Obtém os valores distintos da coluna 'ds'
            distinct_ds_values = self.df['ds'].unique()
            # Se a quantidade de valores distintos for 1, então a série
            # temporal é univariada
            return distinct_ds_values.size == 1
        return self.df.columns.size == 2

    def is_multivariate(self):
        # Verifica se a série temporal é multivariada
        if self.format == 'long':
            # Obtém os valores distintos da coluna 'ds'
            distinct_ds_values = self.df['ds'].unique()
            # Se a quantidade de valores distintos for maior que 1, então a série
            # temporal é multivariada
            return distinct_ds_values.size > 1
        return self.df.columns.size > 2

    def split(self) -> list[TimeSerie]:  # Alternativa: list['TimeSerie']
        # TODO: garantir que a primeira coluna seja o indice no Dataframe quando o formato for long ou wide
        # TODO: garantir que a primeira coluna seja do tipo Timestamp (datetime) quando o formato for long ou wide
        # Cria várias séries temporais univariadas à partir de uma série temporal multivariada
        # As séries temporais univariadas estão sempre no formato wide
        result = []
        if self.format == 'long':
            # TODO: Implementar para o caso de formato longo.
            pass
        elif self.format == 'wide':
            # Por contrato a primeira coluna é sempre o timestamp
            timestamp_col = self.df.columns[0]
            for idx, col in enumerate(self.df.columns):
                if idx == 0:
                    continue
                my_df = self.df[[timestamp_col, col]]
                # Criaando um novo objeto TimeSerie
                my_ts = TimeSerie(data=my_df, format='wide', features_qty=2)
                logger.debug('---------------------------------------------------')
                logger.debug(f'univariate {idx}' + '\n ' + str(my_df))
                result.append(my_ts)
        else:
            raise Exception('Formato de série temporal não suportado')

        msg = 'O método split deve retornar uma lista de objetos TimeSerie'
        assert isinstance(result, list), msg
        for ts in result:
            assert isinstance(ts, TimeSerie), msg

        return result

    def get_numeric_column_names(self) -> list:
        ret: list = []
        for idx, c in enumerate(self.df.columns):
            if (
                self.df[c].dtype == float
                or self.df[c].dtype == int
                or self.df[c].dtype == np.float64
                or self.df[c].dtype == np.int64
                or self.df[c].dtype == np.float32
                or self.df[c].dtype == np.int32
            ):
                ret.append(c)
        ret.sort()  # sort é operação mutável !
        return ret

    def normalize(
        self,
        scaler: TransformerMixin,
        numeric_columns: list[str] | None = None,
        inplace: bool = False,
    ) -> TimeSerie:
        column_list: list[str] = []
        if numeric_columns is None:
            column_list = self.get_numeric_column_names()
        else:
            column_list = numeric_columns
        logger.info(f'columns -> {self.df.columns} -> column_list: {column_list}')
        if len(column_list) == 0:
            # TODO: escolher uma Exception mais adequada, tipo InvalidStateError
            raise Exception('Não há colunas numéricas para normalizar')

        df_norm = self.df.copy(deep=True)

        df_norm[column_list] = pd.DataFrame(scaler.fit_transform(self.df[column_list]))
        # Restaura a coluna timestamp
        timestamp_col = self.df.columns[0]
        df_norm[timestamp_col] = self.df[timestamp_col]
        # Trata o parâmetro inplace para o caso de imutabilidade.
        if inplace:
            self.df = df_norm
            self.scaler = scaler
            logger.info(f'Time Serie normalized -> scaler type: {type(self.scaler)}')
            return self
        else:
            ret = TimeSerie(
                data=df_norm, format=self.format, features_qty=int(self.features)
            )
            ret.scaler = scaler
            scaler_name = str(type(scaler)).split('.')[-1]
            logger.info(
                f'Time Serie normalized -> scaler type: {type(scaler)} -> {scaler_name}'
            )
            return ret

    def denormalize(self, inplace: bool = False) -> TimeSerie | None:
        def denormalize_row():
            if isinstance(self.scaler, MinMaxScaler):
                return lambda x: (x + 1) / 2 * (self.scaler.data_max_ - self.scaler.data_min_) + self.scaler.data_min_  # type: ignore
            elif isinstance(self.scaler, RobustScaler):
                return lambda x: (x * self.scaler.scale_) + self.scaler.center_  # type: ignore
            else:
                raise ValueError('Unsupported scaler')

        # Reverte a normalização de um conjunto de valores. Observe que podem haver diferenças de arredondamento
        # entre os valores originais e os valores revertidos devido a questões intrinsecas do formato float.
        # Considere epsilon = 1e-6 para valores float32 e epsilon = 1e-8 para valores float64
        numeric_columns = self.get_numeric_column_names()
        for idx, row in self.df.iterrows():
            row_numeric_values: pd.Series = row[numeric_columns]
            row_denormalized = denormalize_row()(row_numeric_values)
            logger.info('-----------------------------------------------------')
            logger.info(f'idx = {idx}, row =\n{row}')
            logger.info(f'row[numeric_columns] =\n{row[numeric_columns]}')
            logger.info(
                f'denormalize(scaler)(row[numeric_columns]) = {row_denormalized}'
            )

    def get_statistics(self) -> TSStats:
        result = TSStats(self.df)
        return result

    """
    Neste método adicionanos uma feature que contém os valores NaN da série temporal corrigidos,
    segundo uma interpolação especificada pelo parâmetro `method`. O método retorna um novo objeto
    TimeSerie com a feature adicionada. Se o parâmetro `inplace` for True, então o método modificará
    o objeto TimeSerie original, adicionando a feature. O parâmetro `plot` indica se o gráfico da
    série temporal corrigida deve ser desenhado.
    """

    def add_nan_mask(
        self, inplace=False, plot=False, method='interpolate'
    ) -> TimeSerie:
        if self.is_multivariate():
            raise Exception(
                'A série temporal deve ser univariada. Use o método split() para obter uma lista de séries temporais univariadas'
            )
        df_interpolated: pd.DataFrame
        if inplace:
            df_interpolated = self.df
        else:
            df_interpolated = self.df.copy()

        time_column = self.df.columns[0]
        feature_column = self.df.columns[1]
        interpolated_serie: Series = df_interpolated[feature_column].interpolate(
            method='spline', order=3
        )
        masked_values = TimeSerie.build_mask_serie(self.df[feature_column])
        inverse_serie = interpolated_serie * masked_values
        nan_column_name = f'{feature_column}_nan'
        df_interpolated[nan_column_name] = inverse_serie

        result = TimeSerie.empty()
        if inplace:
            result = self
        else:
            result = TimeSerie(
                df_interpolated,
                format=self.format,
                features_qty=int(df_interpolated.columns.size),
            )

        logger.info(f'result =\n{result}')
        if plot:
            result.plot.line(figsize=(12, 5), grid=True)
            # .plot.line(figsize=(12, 5), grid=True)

        return TimeSerie(
            df_interpolated, format=self.format, features_qty=int(self.features)
        )

    """
    Implementa-se aqui um método plot que retorna um objeto TSPlotting,
    definindo-o como `@property`.

    O clinte usa assim:
    ts = TimeSerie(...) # constroi o objeto TimeSerie
    ts.plot.line(...)   # desenha-se o gráfico escolhido
    ts.plot.stackplot(...)
    ts.plot.scatter(...)
    ts.plot.hist(...)
    ts.plot.bar(...)
    ts.plot.box(...)
    O formato assumido é `wide` e a primeira coluna é tratada sempre com timestatmps.
    As demais colunas são tratadas como features da série temporal.
    """

    @property
    def plot(self):
        return TSPlotting(self)

    @staticmethod
    def join(list_of_ts: list['TimeSerie']) -> 'TimeSerie':
        if len(list_of_ts) == 0:
            raise Exception('A lista de séries temporais não pode estar vazia')

        multivariate_ts = list_of_ts[0]
        multivariate_df = multivariate_ts.df
        timestamp_column_name = multivariate_df.columns[0]

        if len(list_of_ts) == 1:
            # Garantindo que a primeira coluna seja um Timestamp (datetime) quando o formato for long ou wide
            assert isinstance(
                list_of_ts[0].df[timestamp_column_name], datetime
            ), 'timestamp deve ser do tipo datetime'
            assert (
                list_of_ts[0].format == 'wide'
            ), 'A série temporal deve estar no formato wide'
            assert (
                list_of_ts[0].features == 2
            ), 'A série temporal univariada deve ter apenas 2 features'
            assert list_of_ts[0].is_univariate(), 'A série temporal deve ser univariada'
            # TODO: Assume-se que a primeira coluna é o timestamp e não existe indice no DataFrame. Ver implicações !
            # Garantindo que a primeira coluna seja o indice no Dataframe quando o formato for long ou wide
            # if (list_of_ts[0].df).index.name == 'None':
            #     (list_of_ts[0].df).set_index(timestamp_column_name, inplace=True)
            return list_of_ts[0]

        # Junta os dataframes em uma série temporal multivariada
        if not isinstance(multivariate_df[timestamp_column_name][0], datetime):
            # Se a coluna timestamp não for do tipo datetime, converter para datetime.
            # Alternativamente podemeos usar o método astype(datetime) que faz o cast.
            multivariate_df[timestamp_column_name] = pd.to_datetime(
                multivariate_df[timestamp_column_name]
            )

        for idx, ts in enumerate(list_of_ts):
            if idx == 0:
                continue
            if not isinstance(ts.df['timestamp'][0], datetime):
                # Se a coluna timestamp não for do tipo datetime, converter para datetime
                ts.df['timestamp'] = pd.to_datetime(ts.df['timestamp'])
                # Faz o merge dos dois Datasets, sobre a coluna timestamp
            multivariate_df = pd.merge(multivariate_df, ts.df, on='timestamp')

        # Ao final crio a série temporal multivariada usando a lista de univariadas
        features_qty = multivariate_df.columns.size
        multivariate_ts = TimeSerie(
            format='wide', features_qty=features_qty, data=multivariate_df
        )

        # Garantindo que a primeira coluna seja o indice no Dataframe quando o formato for long ou wide
        # Define a primeira coluna como índice do dataframe, caso já não seja
        # TODO: Assume-se que a primeira coluna é o timestamp e não existe indice no DataFrame. Ver implicações !
        # if (multivariate_ts.df).index.name == 'None':
        #     (multivariate_ts.df).set_index(timestamp_column_name, inplace=True)
        # multivariate_ts.df.set_index(timestamp_column_name, inplace=True)

        # Imprime a série temporal multivariada
        logger.debug(multivariate_ts)
        return multivariate_ts

    """
    Este método cria uma máscara para os valores nulos de uma série temporal univariada.
    Ela é construida de tal forma que se multimplicada pela série temporal corrigida com
    imputação de valores nulos, ela elimina os valores originais, mantendo apenas os
    valores imputados
    """

    @staticmethod
    def build_mask_serie(s: pd.Series) -> pd.Series:
        mask = np.full(len(s), 1.0)
        print(f'len(s) = {len(s)}, len(mask) = {len(mask)}')
        previus = s[0]
        for idx, v in enumerate(mask):
            value = s[idx]
            if not np.isnan(value):  # operador == não funciona com NaN
                mask[idx] = np.nan
            if idx < len(mask) - 2 and np.isnan(s[idx + 1]):
                mask[idx] = 1.0
            if idx > 0 and not np.isnan(value) and np.isnan(previus):
                mask[idx] = 1.0
            previus = value

        print(f'mask = {mask}')
        print(f's    = {s.to_numpy()}')
        return pd.Series(mask)

    ### ----------------------------- Métodos de IProvenanceable ----------------------------------

    def add_provenance(self, transformation: str, parameters: dict):
        # Este método adiciona informações de proveniência ao objeto TimeSerie para
        # uma transformação específica. Ele recebe como parâmetros o nome da transformação
        # e um dicionário com os parâmetros usados na transformação.
        # TODO: Implementar add_provenance()
        pass

    def get_provenances(self) -> list[dict[str, Any]]:
        # Este método retorna uma lista de dicionários com as informações de
        # proveniência de todas as transformações realizadas no objeto TimeSerie.
        # TODO: Implementar get_provenances()
        return []

    def get_provenance_by_transformation(self, transformation: str) -> dict[str, Any]:
        # Este método retorna as informações de proveniência de uma dada transformação
        # realizadas no objeto TimeSerie.
        # TODO: Implementar get_provenance_by_transformation()
        return {}

    def get_last_transformation(self, transformation: str) -> dict[str, Any]:
        # TODO: Implementar get_provenance_by_transformation()
        return {}

    def apply_transformation(
        self, transformation: str, **kwargs
    ) -> Optional['TimeSerie']:
        """Este método aplica uma transformação ao objeto TimeSerie e retorna um novo
        objeto TimeSerie com as informações de proveniência atualizadas. Ele recebe como
        parâmetros o nome da transformação e os parâmetros específicos da transformação.
        """
        # TODO: Implementar apply_transformation()
        return TimeSerie.empty()

    ### ------------------------- Outros Métodos Estáticos da classe -----------------------------

    @staticmethod
    def empty() -> Any:
        return TimeSerie(data='EMPTY', format='long', features_qty=0)
