# from __future__ import annotations usado apenas nas versões anteriores a 3.7
from abc import ABC, abstractmethod
from typing import Any, Optional
from pathlib import Path
from datetime import datetime
from t8s.ts import TimeSerie
from t8s.log_config import LogConfig
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = LogConfig().getLogger()

class Strategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def do_write(self, data: Any, ts: Any) -> Any:
        pass


"""
Concrete Strategies implement the algorithm while following the base Strategy
interface. The interface makes them interchangeable in the Context.
"""

class TSWriter():
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

    def write(self, path: Path, ts: TimeSerie) -> None:
        """
        The Context delegates some work to the Strategy object instead of
        implementing multiple versions of the algorithm on its own.
        """

        # ...

        logger.info("Context: write")
        result = self._strategy.do_write(path, ts)
        # logger.info('result type from do_write() is: ' + str(type(result)))

        # ...
        return result

class WriteParquetFile(Strategy):
    # O método do_write é VOID, pois o resultado é gravado em disco. Caso ocorra
    # algum problema uma `Exception` é lançada.
    def do_write(self, path: Path, ts: TimeSerie) -> None:
        logger.info('Using WriteParquetFile strategy')
        # Grava os dados em formato Parquet com metadados do objeto TimeSerie
        # to_parquet(path, df, self.format, self.features)
        table = pa.Table.from_pandas(ts.df)
        # table = table.replace_schema_metadata({'format': self.format, 'features': self.features})
        table = table.replace_schema_metadata({
            b'format': str(ts.format).encode(),
            b'features': str(ts.features).encode()})
        result = pq.write_table(table, path)
        return result      

class WriteCsvFile(Strategy):
    # O método do_write é VOID, pois o resultado é gravado em disco. Caso ocorra
    # algum problema uma `Exception` é lançada.
    def do_write(self, path: Path, ts: TimeSerie) -> None:
        logger.info('Using WriteCsvFile strategy')
        return None
