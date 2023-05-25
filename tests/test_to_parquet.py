import yaml
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile

def test_to_parquet():
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    data = {
        'timestamp': [
            datetime(2022, 1, 1, 0, 0, 0), 
            datetime(2022, 1, 1, 1, 0, 0),
            datetime(2022, 1, 1, 2, 0, 0), 
            datetime(2022, 1, 1, 3, 0, 0)],
        'temperatura': [25.0, 26.0, 27.0, 23.2],
        'velocidade': [2000, 1100, 1200, 4000]
    }
    # Convertendo os tipos de dado para temperatura e velocidade para 
    # np.float32 e np.int32 respectivamente, pois o padrão é np.float64 e np.int64
    data['temperatura'] = np.array(data['temperatura'], dtype=np.float32)
    data['velocidade'] = np.array(data['velocidade'], dtype=np.int32)
    ts = TimeSerie(data, format='wide', features_qty=len(data))
    df = ts.df
    cols_str = [name for name in sorted(df.columns)]
    print('cols_str :', ', '.join(cols_str))
    print(f'Dataframe com {len(df.columns)} colunas: {df.columns}')
    # Imprime a série temporal
    print(ts)
    # Grava a série temporal em parquet
    print(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path}')
    context = TSWriter(WriteParquetFile())
    print("Client: Strategy was seted to write Parquet file.")
    context.write(Path(path_str), ts)
    print(f'\nArquivo {str(path)} gerado à partir da TimeSerie fornecida')
