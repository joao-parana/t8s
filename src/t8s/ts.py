from pathlib import Path
from datetime import datetime
import yaml
from abc import ABC, abstractmethod
from typing import List, Dict, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

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
    def to_parquet(self, path:Path):
        pass

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
    def get_provenances(self) -> List[Dict[str, Any]]: 
        """Este método retorna uma lista de dicionários com as informações de 
        proveniência de todas as transformações realizadas no objeto TimeSerie."""
        pass

    @abstractmethod
    def get_provenance_by_transformation(self, transformation: str) -> Dict[str, Any]: 
        """Este método retorna as informações de proveniência de uma dada transformação 
        realizadas no objeto TimeSerie."""
        pass

    @abstractmethod
    def get_last_transformation(self, transformation: str) -> Dict[str, Any]: 
        """Este método retorna informações da ultima transformação realizada no objeto TimeSerie."""
        pass

    def apply_transformation(self, transformation: str, **kwargs) -> ITimeSeriesProcessor: 
        """Este método aplica uma transformação ao objeto TimeSerie e retorna um novo 
        objeto TimeSerie com as informações de proveniência atualizadas. Ele recebe como 
        parâmetros o nome da transformação e os parâmetros específicos da transformação."""
        pass

class TimeSerie(ITimeSeriesProcessor, IProvenancable):
    def __init__(self, *args, format, features_qty, **kwargs):
        assert isinstance(format, str), "format must be a string"
        assert isinstance(features_qty, int), "features_qty must be a int"
        assert format in ['long', 'wide'], "format must be 'long' or 'wide'"
        self.format: str = format
        self.features: str = str(features_qty)
        self.df = pd.DataFrame(*args, **kwargs)

    def __repr__(self):
        # Retorna uma representação em string do objeto TimeSerie
        return f'TimeSerie(format={self.format}, features={self.features}, df=\n{self.df.__repr__()})'
    
    def to_long(self):
        # Converte a série temporal para o formato Long
        # Implementação aqui
        pass

    def to_wide(self):
        # Converte a série temporal para o formato Wide
        # Implementação aqui
        pass

    def is_univariate(self):
        # Verifica se a série temporal é univariada
        # Implementação aqui
        pass

    def is_multivariate(self):
        # Verifica se a série temporal é multivariada
        # Implementação aqui
        pass

    def to_parquet(self, path):
        # Grava os dados em formato Parquet com metadados do objeto TimeSerie
        # to_parquet(path, df, self.format, self.features)
        table = pa.Table.from_pandas(self.df)
        # table = table.replace_schema_metadata({'format': self.format, 'features': self.features})
        table = table.replace_schema_metadata({
            b'format': str(self.format).encode(),
            b'features': str(self.features).encode()})
        pq.write_table(table, path)

    ### Métodos de IProvenancable
    def add_provenance(self, transformation: str, parameters: dict):
        # Este método adiciona informações de proveniência ao objeto TimeSerie para 
        # uma transformação específica. Ele recebe como parâmetros o nome da transformação 
        # e um dicionário com os parâmetros usados na transformação.
        pass

    def get_provenances(self) -> List[Dict[str, Any]]:
        # Este método retorna uma lista de dicionários com as informações de 
        # proveniência de todas as transformações realizadas no objeto TimeSerie.
        return []

    def get_provenance_by_transformation(self, transformation: str) -> Dict[str, Any]:
        # Este método retorna as informações de proveniência de uma dada transformação 
        # realizadas no objeto TimeSerie.
        return {}

    def get_last_transformation(self, transformation: str) -> Dict[str, Any]:
        return {}

    """
    @staticmethod
    . . .
    """