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
    LogConfig().initialize_logger(DEBUG)
    logger = LogConfig().get_logger()
    ts = TSBuilder.empty()
    # initialize_logger(INFO)
    logger.info('t8s package version:' + t8s.__version__)
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    start_timestamp = datetime(2022, 1, 1, 0, 0, 0)
    number_of_records = 4
    time_interval = 1  # hour
    logger.info(f'@given: -> Building a sample Dataframe')
    df, last_ts = get_sample_df(number_of_records, start_timestamp, time_interval)
    df.to_csv('data/csv/ts_01.csv', index=False)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e
    # velocidade para o proposito de teste
    ts = TimeSerie(df, format='wide', features_qty=3)
    logger.debug('ts.df.index.name:', "'" + str(ts.df.index.name) + "'")
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
    assert type(ts.df['velocidade'][0]) == np.float32

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

    # Verifica se um conjunto de arquivos CSV tem o mesmo Schema
    from t8s.io import IO

    directory: Path = Path('/NullPathObject')
    assert directory.exists() == False, "directory must be a valid Path object"
    path = Path('../fpp3-python/data')
    # informando o diretório onde estão os arquivos CSV
    IO.check_schema_in_csv_files(directory=path)
    # ou a propria lista de arquivos CSV
    path = Path('../fpp3-python/data')
    my_csv_files: list[Path] = list(path.glob('*.csv'))
    IO.check_schema_in_csv_files(csv_files=my_csv_files)

    # --------------------------------------------------------------------------------
    # Faz a leitura de arquivo CSV e gera uma série temporal multivariada com colunas
    # numéricas corretamente tipadas (datetime, float32 e int32). Os tipos de dados
    # são inferidos pelo Pandas e corrigidos no caso de discrepâncias, onde o Pandas
    # não consegue inferir o tipo de dado corretamente e deixa como `object`.
    # No futuro tipos str e bool serão suportados também, para os casos envolvendo
    # agregação, classificação e regressão.

    # Preparando uma Série Temporal Multivariada para teste, com alguns NaNs e
    # alguns dados inválidos
    ts_whith_nans: TimeSerie = TimeSerie(data=ts.df, format='wide', features_qty=3)
    path = Path('ts_01.csv')
    # Coluna de indice não é considerada no atributo 'iloc'
    logger.debug(
        'ts_whith_nans.df.index.name:', "'" + str(ts_whith_nans.df.index.name) + "'"
    )
    if ts_whith_nans.df.index.name == 'None':
        ts_whith_nans.df.iloc[0, 2] = 'Missing'
        ts_whith_nans.df.iloc[1, 1] = np.nan
        ts_whith_nans.df.iloc[2, 2] = np.nan
        ts_whith_nans.df.iloc[3, 2] = 'Bad'
    else:
        ts_whith_nans.df.iloc[0, 1] = 'Missing'
        ts_whith_nans.df.iloc[1, 0] = np.nan
        ts_whith_nans.df.iloc[2, 1] = np.nan
        ts_whith_nans.df.iloc[3, 1] = 'Bad'
    # Gero um CSV com alguns NaNs para teste de leitura
    IO.dataframe_to_csv_file(ts_whith_nans.df, path)

    # --------------------------------------------------------------------------------

    # Lê o arquivo CSV com NaNs e gera um Dataframe sem alterar os tipos de dados
    path = Path('ts_01.csv')
    column_types: list[type] = []
    df_whith_nans = IO.read_csv_file(path, column_types)
    logger.debug(df_whith_nans)
    logger.debug('Tipos das colunas no Dataframe da Série Temporal:')
    for col in df_whith_nans.columns:
        for value in df_whith_nans[col]:
            logger.debug(col, '\t', value, type(value))

    # --------------------------------------------------------------------------------

    # Verifica se a conversão para tipo Number das colunas é trivial ou se é necessário
    # fazer mapeamento de valores não numéricos, tais como, 'Bad', 'Missing', etc.
    numeric_columns: list[str] = list(df_whith_nans.columns[1:])
    all_problems = IO.check_convertion_to_float(numeric_columns, df_whith_nans)
    logger.debug('all_problems:', all_problems)

    # --------------------------------------------------------------------------------

    # Lê o arquivo CSV e gera um Dataframe com os tipos de dados corretos informados
    # na lista column_types que deve corresponder na mesma ordem com as colunas.
    path = Path('ts_01.csv')
    # TODO: Ver como resolver a conversão para pd.Timestamp, pois o Pandas não está fazendo isso automaticamente
    column_types: list[type] = [np.float32, np.float32]
    df_whith_nans = IO.read_csv_file(path, column_types)
    logger.debug('df_whith_nans:\n', df_whith_nans)
    logger.debug('Tipos das colunas no Dataframe da Série Temporal:')
    for col in df_whith_nans.columns:
        for value in df_whith_nans[col]:
            logger.debug(col, '\t', value, type(value))

    # --------------------------------------------------------------------------------
