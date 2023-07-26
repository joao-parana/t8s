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
from behave import given, when, then, use_step_matcher, step
from behave.model import Table
from behave_pandas import table_to_dataframe, dataframe_to_table
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

LogConfig().initialize_logger(DEBUG)
logger = LogConfig().getLogger()

"""
Feature: Create a time series set using Dataframe, CSV and Parquet

Value Statement:
  As a data analyst
  I want the hability to create a timeseries set using Dataframe, CSV and Parquet
  So that I can start studying the data right away and propose solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analise
"""

@given(u'that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analyze')
def setup(context):
    def clean_data_dir():
        logger.info(f'clean_data_dir() called ...')
        pass

    clean_data_dir()
    logger.info(f'-------------------------------------------------')
    logger.info(f'Background @given: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'Background@given:  CSV_PATH = {context.CSV_PATH}')
    logger.info(f'Background@given:  PARQUET_PATH = {context.PARQUET_PATH}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context

"""
  Scenario: Create 2 time series with sample data using Dataframes and save in CSV files at T8S_WORKSPACE/data/csv directory
    Given a start timestamp, a number of records and a time interval
    When I create 2 time series using Dataframes with sample data
    Then I have a time series with the `correct` number of rows and columns, schema and time interval
    And I have a CSV file in T8S_WORKSPACE/data/csv correctelly formated
    # Constraint: The first  Dataframe doesn't have nulls or invalid values, but the second does
"""

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
    context.ts2 = TimeSerie(df2, format='wide', features_qty=len(df2.columns))

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

@then(u'I have a text representation for the first time serie like this below')
def check_text(context):
    logger.debug(f'check_text : context.text -> \n{str(context.text)}')
    logger.debug(f'\ncheck_text : context.ts1 -> \n{str(context.ts1)}')
    def assert_line_by_line(context):
        ts1_line = str(context.ts1).splitlines()
        for idx, line in enumerate(context.text.splitlines()):
            logger.info(f'check_text : line {idx} -> \n"{line}" == "{ts1_line[idx]}"')
            msg =f'context.text must be equal to str(ts1). Failed in line {idx} -> "{line}" != "{ts1_line[idx]}"'
            assert line.strip() == ts1_line[idx].strip(), msg
    assert_line_by_line(context)
    logger.info('\ncheck_text passed !')

# -----------------------------------------------------------------------------------------------

"""
  Scenario: Second, I create 1 time series at T8S_WORKSPACE/data directory using a literal sample data
    Given a the table below as input
      | datetime            | float       | float       |
      | timestamp           | temperatura | velocidade  |
      | 2022-01-01 00:00:00 | 3.0         | 30.0        |
      | 2022-01-01 01:00:00 | 4.1         | 1124        |
      | 2022-01-01 02:00:00 | 5.2         | 3276        |
    When converted to a data frame using 1 row as column names and 1 column as index
    And printed using data_frame_to_table function
    Then I build a time series with the `correct` number of rows and columns
"""

use_step_matcher("parse")

@given(u'a the table below as input')
def the_table(context):
    table: Table = context.table
    context.input = table
    table.headings
    logger.info('the_table() called ...\n' + str(context.input) + ' ->' + str(type(context.input)))
    logger.info(f'{table.headings}')
    for row in table.rows:
        logger.info(f'{row}')
    logger.info(f'the_table() passed !')

@when(u'converted to a data frame using {column_levels:d} row as column names and {index_levels:d} column as index')
def to_dataframe(context, column_levels, index_levels):
    parsed: pd.DataFrame = table_to_dataframe(context.input, column_levels=column_levels)
    for idx, col in enumerate(parsed.columns):
        logger.info(f'{idx}\t-> {col}, {type(parsed[col])}, {type(parsed[col][0])}')
    context.parsed = parsed
    logger.info(f'context.parsed =\n{context.parsed}\nType of context.parsed =\n{type(context.parsed)}')
    logger.info(f'context.parsed.columns = {context.parsed.columns}')
    logger.info(f'Test of to_dataframe passed !')

@when(u'printed using data_frame_to_table function')
def print_to_table(context):
    table = dataframe_to_table(context.parsed)
    assert isinstance(table, str)
    logger.info(f'table =\n{table}\nType of table = {type(table)}')
    logger.info(f'Test of print_to_table passed !')

@then(u'I build a time series with the `correct` number of rows and columns')
def table_to_ts(context):
    df3: pd.DataFrame = context.parsed
    logger.info(f'parsed =\n{df3}\nType of parsed = {type(df3)}')
    context.ts3 = TimeSerie(df3, format='wide', features_qty=len(df3.columns))
    logger.info(f'context.ts3 =\n{context.ts3}')
    logger.info(f'Test of table_to_ts passed !')
