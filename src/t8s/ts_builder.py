from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Any
from pathlib import Path
from datetime import datetime
from t8s.log_config import LogConfig
from t8s import TimeSerie
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = LogConfig().getLogger()

class TSBuilder():
    """
    The Context defines the interface of interest to clients.
    """

    def __init__(self, strategy: Strategy) -> None:
        """
        Usually, the Context accepts a strategy through the constructor, but
        also provides a setter to change it at runtime.
        """
        logger.info('TSBuilder constructor received strategy: %s', strategy)
        self._strategy = strategy

    @property
    def strategy(self) -> Strategy:
        """
        The Context maintains a reference to one of the Strategy objects. The
        Context does not know the concrete class of a strategy. It should work
        with all strategies via the Strategy interface.
        """

        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        """
        Usually, the Context allows replacing a Strategy object at runtime.
        """

        self._strategy = strategy

    def build_from_file(self, path: Path) -> TimeSerie:
        """
        The Context delegates some work to the Strategy object instead of
        implementing multiple versions of the algorithm on its own.
        """

        # ...

        logger.info("Context: build_from_file")
        result = self._strategy.do_algorithm(path)
        logger.info('result type is: ' + str(type(result)))

        # ...
        return result

    def build_from_socket(self, uri) -> TimeSerie:
        """
        The Context delegates some work to the Strategy object instead of
        implementing multiple versions of the algorithm on its own.
        """

        # ...

        print("Context: build_from_socket")
        result = self._strategy.do_algorithm(None)
        logger.info('result type is: ' + str(type(result)))

        # ...
        return result

class Strategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def do_algorithm(self, data: Any) -> TimeSerie:
        pass


"""
Concrete Strategies implement the algorithm while following the base Strategy
interface. The interface makes them interchangeable in the Context.
"""


class ReadParquetFile(Strategy):
    def do_algorithm(self, data: Path) -> TimeSerie:
        logger.info('Using ReadParquetFile strategy')
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
        # print('format', dict_meta['format'])
        # print('features', dict_meta['features'])
        # Imprime o esquema do arquivo Parquet
        logger.debug('\ntype(metadata.schema): ' + str(type(metadata.schema)) + '\t' + str(metadata.schema))
        # Imprime as colunas do arquivo Parquet
        # print('metadata.column_names', metadata.column_names)
        # Imprime as estatísticas do arquivo Parquet
        logger.debug(metadata.row_group(0).column(0).statistics)
        # Leia os metadados do arquivo Parquet
        features_qty = int(features)
        # Leia o arquivo Parquet
        parquet_file: pa.parquet.core.ParquetFile = pq.ParquetFile(data)
        logger.debug('\ntype(parquet_file): ' + str(type(parquet_file)) + '\n' + str(parquet_file))
        logger.debug('\n-------------------------------')
        df = pd.read_parquet(data)
        logger.debug('\ndf:\n' + str(df))
        # Cria o objeto 
        ts = TimeSerie(data=df, format=format, features_qty=features_qty)
        logger.info('\nts:\n' + str(ts))
        return ts


class ReadCsvFile(Strategy):
    def do_algorithm(self, data: List) -> TimeSerie:
        logger.info('Using ReadCsvFile strategy')
        return TimeSerie(format = '', features_qty = 0)