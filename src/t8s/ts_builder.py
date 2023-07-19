# -*- coding: utf-8 -*-

# from __future__ import annotations usado apenas nas versões anteriores a 3.7
from abc import ABC, abstractmethod
from typing import Any, Optional
from pathlib import Path
from datetime import datetime
from t8s.log_config import LogConfig
from t8s.ts import TimeSerie  # , ITimeSerie, ITimeSeriesProcessor, IProvenancable
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = LogConfig().getLogger()


class ReadStrategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def do_read(self, data: Any) -> TimeSerie:
        pass


"""
Concrete Strategies implement the algorithm while following the base Strategy
interface. The interface makes them interchangeable in the Context.
"""


class TSBuilder:
    """
    The Context defines the interface of interest to clients.
    """

    def __init__(self, strategy: ReadStrategy) -> None:
        """
        Usually, the Context accepts a strategy through the constructor, but
        also provides a setter to change it at runtime.
        """
        logger.info('TSBuilder constructor received strategy: %s', strategy)
        self._strategy = strategy

    @property
    def strategy(self) -> ReadStrategy:
        """
        The Context maintains a reference to one of the Strategy objects. The
        Context does not know the concrete class of a strategy. It should work
        with all strategies via the Strategy interface.
        """

        return self._strategy

    @strategy.setter
    def strategy(self, strategy: ReadStrategy) -> None:
        """
        Usually, the Context allows replacing a Strategy object at runtime.
        """

        self._strategy = strategy

    def build_from_file(self, path: Path) -> TimeSerie:
        # TODO: garantir que a primeira coluna seja um Timestamp quando o formato for long ou wide

        """
        The Context delegates some work to the Strategy object instead of
        implementing multiple versions of the algorithm on its own.
        """

        # ...

        logger.info(f"Context: build_from_file {path}")
        result = self._strategy.do_read(path)
        logger.info('result type from build_from_file() is: ' + str(type(result)))

        # ...
        return result

    def build_from_socket(self, uri) -> Optional['TimeSerie']:
        # TODO: garantir que a primeira coluna seja um Timestamp quando o formato for long ou wide

        """
        The Context delegates some work to the Strategy object instead of
        implementing multiple versions of the algorithm on its own.
        """

        # ...

        print("Context: build_from_socket")
        result = self._strategy.do_read(None)
        logger.info('result type from build_from_socket() is: ' + str(type(result)))

        # ...
        return result

    @staticmethod
    def empty() -> TimeSerie:
        return TimeSerie.empty()


class ReadParquetFile(ReadStrategy):
    def do_read(self, data: Path) -> Optional['TimeSerie']:
        logger.info('Using ReadParquetFile strategy to read data from: ' + str(data))
        assert isinstance(data, Path), "path must be a Path object"
        assert (str(data)).endswith('.parquet'), "path must be a Path object"
        # Lê os metadados do arquivo Parquet
        metadata: pq.FileMetaData = pq.read_metadata(data)
        logger.debug('\nParquet file metadata:\n' + str(metadata.to_dict()) + '\n' + str(metadata.metadata))
        assert isinstance(metadata, pq.FileMetaData), "metadata must be a pq.FileMetaData object"
        dict_meta: dict = metadata.to_dict()
        logger.debug('\n-------------------------------')
        logger.debug('created_by: ' + str(dict_meta['created_by']))
        # Imprime o valor do metadado 'format'
        format = metadata.metadata[b'format'].decode()
        features = metadata.metadata[b'features'].decode()
        logger.info('format: ' + format + ' type(format) ' + str(type(format)))
        logger.info('features: ' + str(features) + ' type(features): ' + str(type(features)))
        assert isinstance(format, str), "format metadada must be a string"
        assert isinstance(features, str), "features metadada must be a string"
        features_qty = int(features)
        assert features_qty > 1, "features_qty must be greater than one"
        # print('format', dict_meta['format'])
        # print('features', dict_meta['features'])
        # Imprime o esquema do arquivo Parquet
        logger.debug('\ntype(metadata.schema): ' + str(type(metadata.schema)) + '\t' + str(metadata.schema))
        # Imprime as colunas do arquivo Parquet
        # print('metadata.column_names', metadata.column_names)
        # Imprime as estatísticas do arquivo Parquet
        if metadata.num_row_groups > 0:
            for idx in range(metadata.num_columns):
                logger.debug(metadata.row_group(0).column(idx).statistics)
                logger.debug('-----------------------------')
        # logger.debug(metadata.row_group(0).column(0).statistics)
        # Leia o arquivo Parquet
        parquet_file = pq.ParquetFile(data)
        logger.debug('\ntype(parquet_file): ' + str(type(parquet_file)) + '\n' + str(parquet_file))
        logger.debug('\n-------------------------------')

        # ATENÇÃO: o método read_parquet() do Pandas não gera o Dataframe com os tipos corretos.
        # Em vez de criar float32 para o physical_type FLOAT do Parquet, ele cria float64.
        # df = pd.read_parquet(data)
        df = parquet_file.read().to_pandas()
        print(df.info())

        # TODO: garantir que a primeira coluna seja um Timestamp quando o formato for long ou wide
        logger.debug('\ndf:\n' + str(df))
        # Cria o objeto
        ts = TimeSerie(df, format=format, features_qty=features_qty)
        logger.debug('\nts:\n' + str(ts))
        return ts


class ReadCsvFile(ReadStrategy):
    def do_read(self, data: list) -> Optional['TimeSerie']:
        logger.info('Using ReadCsvFile strategy')
        return TSBuilder.empty()
