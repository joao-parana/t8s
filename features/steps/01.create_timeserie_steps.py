"""
Feature: Create a time series set using Dataframe, CSV and Parquet

Value Statement:
  As a data analyst
  I want the hability to create a timeseries set using Dataframe, CSV and Parquet
  So that I can start studying the data right away and propose solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analise

  Create 2 time series with sample data using Dataframes and save in CSV files at T8S_WORKSPACE/data/csv directory
    Given a start timestamp, a number of records and a time interval
    When I create 2 time series using Dataframes with sample data
    Then I have a time series with the `correct` number of rows and columns, schema and time interval
    And I have a CSV file in T8S_WORKSPACE/data/csv correctelly formated
    # Constraint: The first  Dataframe doesn't have nulls or invalid values, but the second does
"""

import os
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from t8s import get_sample_df
from t8s.log_config import LogConfig
from t8s.util import Util
from t8s.io import IO
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile
from behave import given, when, then, step
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

LogConfig().initialize_logger(DEBUG)
logger = LogConfig().getLogger()

def clean_data_dir():
    logger.info(f'clean_data_dir() called ...')
    pass

@given(u'that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analyze')
def setup(context):
    logger.info(f'-------------------------------------------------')
    logger.info(f'Background @given: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'Background@given:  CSV_PATH = {context.CSV_PATH}')
    logger.info(f'Background@given:  PARQUET_PATH = {context.PARQUET_PATH}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context

@given(u'a start timestamp, a number of records and a time interval')
def create_dataframe(context):
    start_timestamp = datetime(2022, 1, 1, 0, 0, 0)
    number_of_records = 4
    time_interval = 1 # hour
    logger.info(f'@given: -> Building a sample Dataframe')
    dataframe1, last_ts = get_sample_df(number_of_records, start_timestamp, time_interval)
    context.time_interval = time_interval
    context.number_of_records = number_of_records
    context.dataframe1 = dataframe1
    context.last_ts = last_ts
    # altera a série temporal incluindo NaN e inválidos
    dataframe2, last_ts = get_sample_df(number_of_records, last_ts + pd.Timedelta(hours=1), time_interval)
    # print('Original:', dataframe2)
    # altera a série temporal incluindo NaN e inválidos
    dataframe2.iloc[0, 1] = 'Missing'
    dataframe2.iloc[1, 1] = np.nan
    dataframe2.iloc[2, 2] = np.nan # ATENÇÃO: ao atribuir NaN, o Pandas converte o tipo da coluna de int32 para float64
    dataframe2.iloc[3, 1] = 'Bad'
    context.dataframe2 = dataframe2
    context.last_ts = last_ts
    logger.info(f'@given: -> CSV_PATH = {context.CSV_PATH}')

@when(u'I create 2 time series using Dataframes with sample data')
def create_time_series(context):
    logger.info(f'@when: => CSV_PATH = {context.CSV_PATH}')
    # logger.info(f'@when: => context.dataframe1 = \n{context.dataframe1}')
    # logger.info(f'@when: => context.dataframe2 = \n{context.dataframe2}')
    # Gerando a primeira série temporal
    df1 = context.dataframe1
    context.ts1 = TimeSerie(df1, format='wide', features_qty=len(df1.columns))
    df2 = context.dataframe2
    context.ts2 = TimeSerie(df2, format='wide', features_qty=len(df1.columns))

@then(u'I have a time series with the `correct` number of rows and columns, schema and time interval')
def check_schema(context):
    logger.info(f'@then: -> CSV_PATH = {context.CSV_PATH}')
    logger.info(f'@then: -> context.ts1 = \n{context.ts1}')
    logger.info(f'@then: -> context.ts2 = \n{context.ts2}')
    logger.info('------------------------------------------------------------')
    def check_time_interval_equals(ctx, value):
        last_ts = None
        for idx, row in enumerate(ctx.dataframe1.itertuples()):
            if idx == 0:
                last_ts = row[1]
            else:
                interval: pd.Timedelta = row[1] - last_ts
                interval_seconds:int = interval.seconds
                time_interval = interval_seconds / 3600
                assert time_interval == value
                last_ts = row[1]
        logger.info(f'@then: -> time_interval checked')
    check_time_interval_equals(context, context.time_interval)
    assert len(context.ts1.df) == context.number_of_records, 'context.ts1.df must have the correct number of records'
    assert len(context.ts2.df) == context.number_of_records, 'context.ts2.df must have the correct number of records'

@then(u'I have a CSV file in T8S_WORKSPACE/data/csv correctelly formated')
def save_csv(context):
    logger.info(f'@then: => CSV_PATH = {context.CSV_PATH}')
    logger.info(f'@then: => len(context.dataframe1) = \n{len(context.dataframe1)}')
    csv_file_path_str_1 = str(context.CSV_PATH) + '/ts_01.csv'
    IO.dataframe_to_csv_file(context.dataframe1, Path(csv_file_path_str_1))
    csv_file_path_str_2 = str(context.CSV_PATH) + '/ts_02.csv'
    IO.dataframe_to_csv_file(context.dataframe2, Path(csv_file_path_str_2))

@then(u'I have a Parquet file in T8S_WORKSPACE/data/parquet correctelly formated with metadata annotations')
def save_parquet(context):
    logger.info(f'@then: => PARQUET_PATH = {context.PARQUET_PATH}')
    logger.info(f'@then: => len(context.dataframe1) = \n{len(context.dataframe1)}')
    logger.info(f'@then: => len(context.dataframe2) = \n{len(context.dataframe2)}')

    def write_ts_to_parquet_file(ts, parquet_path, filename: str):
        parquet_file_path_str: str = str(parquet_path) + '/' + filename
        path_ts = Path(parquet_file_path_str)
        # Devido a problemas de 'circular import' tivemos que usar a classe Util
        Util.to_parquet(ts, path_ts)
        # print(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path_ts}')
        # context = TSWriter(WriteParquetFile())
        # print("Client: Strategy was seted to write Parquet file.")
        # context.write(Path(path_ts), ts)
        # print(f'\nArquivo {str(path_ts)} gerado à partir da TimeSerie fornecida')

    # Grava a série temporal ts1 em parquet
    write_ts_to_parquet_file(context.ts1, context.PARQUET_PATH, 'ts_01.parquet')

    # Grava a série temporal ts2 em parquet
    # write_ts_to_parquet_file(context.ts2, context.PARQUET_PATH, 'ts_02.parquet')
