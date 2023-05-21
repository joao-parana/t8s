from pathlib import Path
from datetime import datetime

import yaml
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ts = pd.read_parquet('lixo.parquet', engine='pyarrow')

class ITimeSeriesProcessor:
    def to_long(self) -> bool:
        # Converte a série temporal para o formato Long
        return False

    def to_wide(self) -> bool:
        # Converte a série temporal para o formato Wide
        return False

    def is_univariate(self) -> bool:
        # Verifica se a série temporal é univariada
        return False

    def is_multivariate(self) -> bool:
        # Verifica se a série temporal é multivariada
        return False

    def to_parquet(self, path:Path) -> str:
        return str(path)  

class TimeSerie(ITimeSeriesProcessor):
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
        table = table.replace_schema_metadata({'format': self.format, 'features': self.features})
        table = table.replace_schema_metadata({
            b'format': str(self.format).encode(),
            b'features': str(self.features).encode()})
        pq.write_table(table, path)

    @staticmethod
    def build_from_file(path: Path): # -> TimeSerie:
        # Leia os metadados do arquivo Parquet
        format = ''
        features_qty = 0
        # Leia o arquivo Parquet
        # Cria o objeto 
        ts = TimeSerie(format=format, features_qty=features_qty)
        return ts
