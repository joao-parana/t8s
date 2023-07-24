# -*- coding: utf-8 -*-

from t8s.log_config import LogConfig
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import t8s
from t8s import get_sample_df
from t8s.io import IO
from t8s.ts import TimeSerie
from t8s.ts_builder import TSBuilder, ReadParquetFile, ReadCsvFile
from t8s.log_config import LogConfig
from t8s.ts_writer import TSWriter, WriteParquetFile, WriteCsvFile
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

if __name__ == "__main__":
    LogConfig().initialize_logger(DEBUG, log_file = 'tests/data_seed.log')
    logger = LogConfig().getLogger()
    logger.info('t8s package version:' + t8s.__version__)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    number_of_records = 4
    time_interval = 1 # hour
    df, last_ts = get_sample_df(number_of_records, datetime(2022, 1, 1, 0, 0, 0), time_interval)
    # Gravando o arquivo CSV
    # df.to_csv(, index=False)
    IO.dataframe_to_csv_file(df, Path('data/csv/ts_01.csv'))
    # Gerando a série temporal
    ts1 = TimeSerie(df, format='wide', features_qty=len(df.columns))
    # Gravando a série temporal em parquet
    path_str: str = 'data/parquet/ts_01.parquet'
    path = Path(path_str)
    print(f'Grava a série temporal (formato {ts1.format}) em um arquivo parquet {path}')
    context = TSWriter(WriteParquetFile())
    print("Client: Strategy was seted to write Parquet file.")
    context.write(Path(path_str), ts1)
    # ---------------------------------------------------------------------------------------------
    # Outro caso de uso
    number_of_records = 4
    time_interval = 1 # hour
    df, last_ts = get_sample_df(number_of_records, last_ts + pd.Timedelta(hours=1), time_interval)
    print('Original:', df)
    # altera a série temporal incluindo NaN e inválidos
    df.iloc[0, 1] = 'Missing'
    df.iloc[1, 1] = np.nan
    df.iloc[2, 2] = np.nan # ATENÇÃO: ao atribuir NaN, o Pandas converte o tipo da coluna de int32 para float64
    df.iloc[3, 1] = 'Bad'
    # df.to_csv('data/csv/ts_02.csv', index=False)
    IO.dataframe_to_csv_file(df, Path('data/csv/ts_02.csv'))

    print('Antes:', df)
    # Gerando a série temporal
    # Filtro valores inválidos, trocando pra NaN
    df.replace(['Configure', 'Missing', 'Bad'], np.nan, inplace=True)
    # Converta os dados numéricos para numpy.float32
    float_cols = df.select_dtypes(include=['float']).columns
    print('float_cols = ', float_cols)
    df[float_cols] = df[float_cols].astype(np.float32)
    print('Depois:', df)
    ts2 = TimeSerie(df, format='wide', features_qty=len(df.columns))
    # Gravando a série temporal em parquet
    path_str: str = 'data/parquet/ts_02.parquet'
    path = Path(path_str)
    print(f'Grava a série temporal (formato {ts2.format}) em um arquivo parquet {path}')
    context = TSWriter(WriteParquetFile())
    print("Client: Strategy was seted to write Parquet file.")
    context.write(Path(path_str), ts2)
