# -*- coding: utf-8 -*-

from datetime import datetime
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING
from pathlib import Path

import numpy as np
import pandas as pd

import t8s
from t8s import get_sample_df
from t8s.log_config import LogConfig
from t8s.ts import TimeSerie
from t8s.ts_builder import ReadCsvFile, ReadParquetFile, TSBuilder
from t8s.ts_writer import TSWriter, WriteCsvFile, WriteParquetFile

if __name__ == "__main__":
    # logger.debug('globals:', globals())
    # The client code.
    LogConfig().initialize_logger(DEBUG, log_file='logs/smoke.log')
    logger = LogConfig().get_logger()
    number_of_records = 4
    time_interval = 1  # hour
    df, last_ts = get_sample_df(
        number_of_records, datetime(2022, 1, 1, 0, 0, 0), time_interval
    )
    ts = TSBuilder.empty()
    # initialize_logger(INFO)
    logger.info('t8s package version:' + t8s.__version__)
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)

    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    number_of_records = 4
    time_interval = 1  # hour
    dataframe, last_ts = get_sample_df(
        number_of_records, datetime(2022, 1, 1, 0, 0, 0), time_interval
    )
    ts = TimeSerie(dataframe, format='wide', features_qty=3)
    cols_str = [name for name in sorted(ts.df.columns)]
    cols_str = ', '.join(cols_str)
    logger.info(f'Dataframe com {len(ts.df.columns)} colunas: {cols_str}')
    # Faz o display da série temporal no terminal
    logger.debug(ts)

    # --------------------------------------------------------------------------------

    # Grava a série temporal em parquet
    logger.debug(
        f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path}'
    )
    ctx = TSWriter(WriteParquetFile())
    logger.debug("Client: Strategy was seted to write Parquet file.")
    ctx.write(Path(path_str), ts)
    logger.debug(f'\nArquivo {str(path)} gerado à partir da TimeSerie fornecida')

    # --------------------------------------------------------------------------------

    # Lê a série temporal gravada no arquivo parquet e gera uma nova série temporal
    # com os respectivos Metadados e Schema. Ao final verifica se os tipos de dados
    # foram lidos corretamente com o módulo assert.

    # Limpando objeto ts para garantir que será lido corretamente.
    ts: TimeSerie = TSBuilder.empty()
    # Lê a série temporal gravada no arquivo parquet
    logger.debug(f'\nLendo path {str(path)} e gerando TimeSerie')

    # The client code picks a concrete strategy and passes it to the context.
    # The client should be aware of the differences between strategies in order
    # to make the right choice.
    assert isinstance(path, Path), "path must be a Path object"
    if (str(path)).endswith('.parquet'):
        ctx = TSBuilder(ReadParquetFile())
        logger.debug("Client: ReadStrategy is set to read Parquet file.")
        ts = ctx.build_from_file(Path(path_str))
    else:
        assert str(path).endswith(
            '.csv'
        ), "If path is not a Parquet file the path must be a CSV file"
        logger.debug("Client: ReadStrategy is set to read CSV file.")
        ctx = TSBuilder(ReadCsvFile())
        ts = ctx.build_from_file(Path(path_str))

    assert int(ts.features) == 3
    assert ts.format == 'wide'
    assert len(ts.df) == 4
    logger.debug('Tipos das colunas no Dataframe da Série Temporal:')
    for col in ts.df.columns:
        logger.debug(col, '\t', type(ts.df[col][0]))

    # pd._libs.tslibs.timestamps.Timestamp é privado e devo usar pd.Timestamp
    assert type(ts.df['timestamp'][0]) == pd.Timestamp
    assert type(ts.df['temperatura'][0]) == np.float32
    assert type(ts.df['velocidade'][0]) == np.int32

    logger.debug('---------------------------------------------------')
    univariate_list = ts.split()
    for idx, ts_uni in enumerate(univariate_list):
        logger.debug(f'TimeSerie univariada {idx+1}:')
        logger.debug(ts_uni)
        logger.debug('---------------------------------------------------')

    logger.debug('\nAgora posso fazer o join das séries temporais univariadas')
    ts = TimeSerie.join(univariate_list)
    logger.debug("\n\nTimeSerie multivariada, ts:\n")
    logger.debug(ts)

    # --------------------------------------------------------------------------------
