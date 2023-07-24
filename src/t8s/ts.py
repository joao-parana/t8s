# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
from datetime import datetime
import types
import yaml
from abc import ABC, abstractmethod
from typing import Any, Optional, Type, TypeVar

import pandas as pd
import pyarrow as pa         # type: ignore
import pyarrow.parquet as pq # type: ignore

from t8s.log_config import LogConfig

# TODO: Esclarecer duvida coceitual sobre o conceito de feture em séries temporais multivariadas

logger = LogConfig().getLogger()

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
    def to_wide(self) -> bool:
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

    # @staticmethod
    # @abstractmethod
    # def join(list_of_ts: list['TimeSerie']) -> TS:
    #     pass


"""Rastreia a proveniência dos ativos de informação, Série Temporal neste caso"""


class IProvenancable(ABC):
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
        parâmetros o nome da transformação e os parâmetros específicos da transformação."""
        pass


class ITimeSerie(ITimeSeriesProcessor, IProvenancable):
    """
    Interface que estende as outras interfaces. ë apenas uma Marker Interface
    """
    pass


class TimeSerie(ITimeSerie):
    def __init__(self, *args, format, features_qty, **kwargs):
        assert isinstance(format, str), "format must be a string"
        assert isinstance(features_qty, int), "features_qty must be a int"
        assert format in ['long', 'wide'], "format must be 'long' or 'wide'"
        self.format: str = format
        self.features: str = str(features_qty)
        self.df = pd.DataFrame()
        if len(args) > 0:
            for idx, arg in enumerate(args):
                if idx == 0 and isinstance(arg, pd.DataFrame):
                    self.df = arg
                if idx == 0 and isinstance(arg, dict):
                    if 'content' in arg.keys() and arg['content'] == 'EMPTY':
                        return
                    self.df = pd.DataFrame(*args, **kwargs)
                break # Apenas o primeiro parâmetro precisa de tratamento especial.
        else:
           for key, kwarg_value in kwargs.items():
                if key == 'data' and isinstance(kwarg_value, pd.DataFrame):
                    self.df = kwarg_value
                if key == 'data' and isinstance(kwarg_value, dict):
                    self.df = pd.DataFrame(*args, **kwargs)
                if key == 'data' and isinstance(kwarg_value, str) and kwarg_value == 'EMPTY':
                    return
                break # Apenas o parâmetro 'data' precisa de tratamento especial.

        # TODO: garantir que a primeira coluna seja um Timestamp tanto para o formato long quanto wide
        # Por hora lanço Exception
        if self.df.empty:
            raise Exception('Não foram fornecidos dados para criação da série temporal. Use o método TimeSerie.empty() se desejar criar uma série temporal vazia')
        first_column_type = self.df[self.df.columns[0]].dtype
        if first_column_type != 'datetime64[ns]':
            print(self.df.info())
            raise Exception('A primeira coluna deve ser um Timestamp (datetime)' +
            ' experado ' + 'datetime64[ns]' + ' recebido ' + str(first_column_type))
        logger.debug('Objeto TimeSerie construido com sucesso')

    def __repr__(self):
        columns_types = [type(self.df[col][0]) for col in self.df.columns]
        # Retorna uma representação em string do objeto TimeSerie
        return f'TimeSerie(format={self.format}, features={self.features}, ' + \
               f'df=\n{self.df.__repr__()}) + \ntypes: {columns_types}'

    def to_long(self):
        # Converte a série temporal para o formato Long
        # Implementação aqui
        raise NotImplementedError('Not implemented for long format')

    def to_wide(self):
        # Converte a série temporal para o formato Wide
        # Implementação aqui
        raise NotImplementedError('Not implemented for long format')

    def is_univariate(self):
        # Verifica se a série temporal é univariada
        if self.format == 'long':
            raise NotImplementedError('Not implemented for long format')
        return self.df.columns.size == 2

    def is_multivariate(self):
        # Verifica se a série temporal é multivariada
        if self.format == 'long':
            raise NotImplementedError('Not implemented for long format')
        return self.df.columns.size > 2

    def split(self) -> list[TimeSerie]:  # Alternativa: list['TimeSerie']
        # TODO: garantir que a primeira coluna seja o indice no Dataframe quando o formato for long ou wide
        # TODO: garantir que a primeira coluna seja do tipo Timesamp (datetime) quando o formato for long ou wide
        # Cria várias séries temporais univariadas à partir de uma série temporal multivariada
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
                # TODO: criar um novo objeto TimeSerie
                my_ts = TimeSerie(data=my_df, format='wide', features_qty=2)
                logger.debug('---------------------------------------------------')
                logger.debug(f'univariate {idx}' + '\n ' + str(my_df))
                result.append(my_ts)
        else:
            raise Exception('Formato de série temporal não suportado')

        msg = 'O método split deve retornar uma lista de objetos TimeSerie'
        for ts in result:
            assert isinstance(ts, TimeSerie), msg

        return result

    @staticmethod
    def join(list_of_ts: list['TimeSerie']) -> 'TimeSerie':
        if len(list_of_ts) == 0:
            raise Exception('A lista de séries temporais não pode estar vazia')

        multivariate_ts = list_of_ts[0]
        multivariate_df = multivariate_ts.df
        timestamp_column_name = multivariate_df.columns[0]

        if len(list_of_ts) == 1:
            # Garantindo que a primeira coluna seja um Timestamp (datetime) quando o formato for long ou wide
            assert isinstance(list_of_ts[0].df[timestamp_column_name], datetime), 'timestamp deve ser do tipo datetime'
            assert list_of_ts[0].format == 'wide', 'A série temporal deve estar no formato wide'
            assert list_of_ts[0].features == 2, 'A série temporal univariada deve ter apenas 2 features'
            assert list_of_ts[0].is_univariate(), 'A série temporal deve ser univariada'
            # Garantindo que a primeira coluna seja o indice no Dataframe quando o formato for long ou wide
            if (list_of_ts[0].df).index.name == 'None':
                (list_of_ts[0].df).set_index(timestamp_column_name, inplace=True)
            return list_of_ts[0]

        # Junta os dataframes em uma série temporal multivariada
        if not isinstance(multivariate_df[timestamp_column_name][0], datetime):
            # Se a coluna timestamp não for do tipo datetime, converter para datetime.
            # Alternativamente podemeos usar o método astype(datetime) que faz o cast.
            multivariate_df[timestamp_column_name] = pd.to_datetime(multivariate_df[timestamp_column_name])

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
        multivariate_ts = TimeSerie(format='wide', features_qty=features_qty, data=multivariate_df)

        # Garantindo que a primeira coluna seja o indice no Dataframe quando o formato for long ou wide
        # Define a primeira coluna como índice do dataframe, caso já não seja
        if (multivariate_ts.df).index.name == 'None':
            (multivariate_ts.df).set_index(timestamp_column_name, inplace=True)
        multivariate_ts.df.set_index(timestamp_column_name, inplace=True)

        # Imprime a série temporal multivariada
        logger.debug(multivariate_ts)
        return multivariate_ts

    ### ----------------------------- Métodos de IProvenancable ----------------------------------

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

    def apply_transformation(self, transformation: str, **kwargs) -> Optional['TimeSerie']:
        """Este método aplica uma transformação ao objeto TimeSerie e retorna um novo
        objeto TimeSerie com as informações de proveniência atualizadas. Ele recebe como
        parâmetros o nome da transformação e os parâmetros específicos da transformação."""
        # TODO: Implementar apply_transformation()
        return TimeSerie.empty()

    ### ------------------------- Outros Métodos Estáticos da classe -----------------------------

    @staticmethod
    def empty() -> Any:
        return TimeSerie(data='EMPTY', format='long', features_qty=0)
