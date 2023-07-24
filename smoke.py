# -*- coding: utf-8 -*-

from t8s.log_config import LogConfig
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import t8s
from t8s import get_sample_df
from t8s.ts import TimeSerie
from t8s.ts_builder import TSBuilder, ReadParquetFile, ReadCsvFile
from t8s.log_config import LogConfig
from t8s.ts_writer import TSWriter, WriteParquetFile, WriteCsvFile
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

if __name__ == "__main__":
    # print('globals:', globals())
    # The client code.
    LogConfig().initialize_logger(DEBUG, log_file = 'logs/smoke.log')
    logger = LogConfig().getLogger()
    number_of_records = 4
    time_interval = 1 # hour
    df, last_ts = get_sample_df(number_of_records, datetime(2022, 1, 1, 0, 0, 0), time_interval)
    ts = TSBuilder.empty()
    # initialize_logger(INFO)
    logger.info('t8s package version:' + t8s.__version__)
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)

    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    number_of_records = 4
    time_interval = 1 # hour
    dataframe, last_ts = get_sample_df(number_of_records, datetime(2022, 1, 1, 0, 0, 0), time_interval)
    ts = TimeSerie(dataframe, format='wide', features_qty=3)
    cols_str = [name for name in sorted(ts.df.columns)]
    cols_str = ', '.join(cols_str)
    logger.info(f'Dataframe com {len(ts.df.columns)} colunas: {cols_str}')
    # Faz o display da série temporal no terminal
    print(ts)

    # --------------------------------------------------------------------------------

    # Grava a série temporal em parquet
    print(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path}')
    context = TSWriter(WriteParquetFile())
    print("Client: Strategy was seted to write Parquet file.")
    context.write(Path(path_str), ts)
    print(f'\nArquivo {str(path)} gerado à partir da TimeSerie fornecida')

    # --------------------------------------------------------------------------------

    # Lê a série temporal gravada no arquivo parquet e gera uma nova série temporal
    # com os respectivos Metadados e Schema. Ao final verifica se os tipos de dados
    # foram lidos corretamente com o módulo assert.

    # Limpando objeto ts para garantir que será lido corretamente.
    ts: TimeSerie = TSBuilder.empty()
    # Lê a série temporal gravada no arquivo parquet
    print(f'\nLendo path {str(path)} e gerando TimeSerie')

    # The client code picks a concrete strategy and passes it to the context.
    # The client should be aware of the differences between strategies in order
    # to make the right choice.
    assert isinstance(path, Path), "path must be a Path object"
    if (str(path)).endswith('.parquet'):
        context = TSBuilder(ReadParquetFile())
        print("Client: ReadStrategy is set to read Parquet file.")
        ts = context.build_from_file(Path(path_str))
    else:
        assert str(path).endswith('.csv'), "If path is not a Parquet file the path must be a CSV file"
        print("Client: ReadStrategy is set to read CSV file.")
        context = TSBuilder(ReadCsvFile())
        ts = context.build_from_file(Path(path_str))

    assert int(ts.features) == 3
    assert ts.format == 'wide'
    assert len(ts.df) == 4
    print('Tipos das colunas no Dataframe da Série Temporal:')
    for col in ts.df.columns:
        print(col, '\t', type(ts.df[col][0]))

    # pd._libs.tslibs.timestamps.Timestamp é privado e devo usar pd.Timestamp
    assert type(ts.df['timestamp'][0]) == pd.Timestamp
    assert type(ts.df['temperatura'][0]) == np.float32
    assert type(ts.df['velocidade'][0]) == np.int32

    print('---------------------------------------------------')
    univariate_list = ts.split()
    for idx, ts_uni in enumerate(univariate_list):
        print(f'TimeSerie univariada {idx+1}:')
        print(ts_uni)
        print('---------------------------------------------------')

    print('\nAgora posso fazer o join das séries temporais univariadas')
    ts = TimeSerie.join(univariate_list)
    print("\n\nTimeSerie multivariada, ts:\n")
    print(ts)

    # --------------------------------------------------------------------------------
