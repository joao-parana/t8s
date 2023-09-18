import os
from datetime import datetime
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING
from pathlib import Path

import numpy as np
import pandas as pd
from behave import given, step, then, use_step_matcher, when  # type: ignore
from behave.model import Table  # type: ignore
from behave_pandas import dataframe_to_table, table_to_dataframe  # type: ignore

from t8s import get_sample_df
from t8s.io import IO
from t8s.log_config import LogConfig
from t8s.ts import TimeSerie, TSStats
from t8s.ts_builder import ReadParquetFile, TSBuilder  # , ReadCsvFile
from t8s.ts_writer import TSWriter, WriteParquetFile
from t8s.util import Util

logger = LogConfig().get_logger()

epsilon = 1e-6

"""
Feature: Create a time series set using Dataframe, CSV and Parquet

Value Statement:
  As a data analyst
  I want the hability to create a timeseries set using Dataframe, CSV and Parquet
  So that I can start studying the data right away and propose solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analise
"""


@given(
    u'that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analyze'
)
def setup(context):
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
    time_interval = 1  # hour
    logger.info(f'@given: -> Building a sample Dataframe')
    dataframe1, last_ts = get_sample_df(
        number_of_records, start_timestamp, time_interval
    )
    context.time_interval = time_interval
    context.number_of_records = number_of_records
    context.dataframe1 = dataframe1
    context.last_ts = last_ts
    # altera a série temporal incluindo NaN e inválidos
    dataframe2, last_ts = get_sample_df(
        number_of_records, last_ts + pd.Timedelta(hours=1), time_interval
    )
    dataframe2_str = ''
    logger.debug(f'Original: {dataframe2_str}')
    # altera a série temporal incluindo NaN e inválidos
    dataframe2.iloc[0, 1] = 'Missing'
    dataframe2.iloc[1, 1] = np.nan
    dataframe2.iloc[
        2, 2
    ] = (
        np.nan
    )  # ATENÇÃO: ao atribuir NaN, o Pandas converte o tipo da coluna de int32 para float64
    dataframe2.iloc[3, 1] = 'Bad'
    context.dataframe2 = dataframe2
    try:
        dataframe2_str = str(dataframe2)
    except Exception as e:
        logger.error(f'@given: -> dataframe2_str = str(dataframe2) -> {e}')

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


@then(
    u'I have a time series with the `correct` number of rows and columns, schema and time interval'
)
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
                interval_seconds: int = interval.seconds
                time_interval = interval_seconds / 3600
                assert time_interval == value
                last_ts = row[1]
        logger.info(f'@then: -> time_interval checked')

    check_time_interval_equals(context, context.time_interval)
    assert (
        len(context.ts1.df) == context.number_of_records
    ), 'context.ts1.df must have the correct number of records'
    assert (
        len(context.ts2.df) == context.number_of_records
    ), 'context.ts2.df must have the correct number of records'


@then(u'I have a CSV file in T8S_WORKSPACE/data/csv correctelly formated')
def save_csv(context):
    logger.info(f'@then: => CSV_PATH = {context.CSV_PATH}')
    logger.info(f'@then: => len(context.dataframe1) = \n{len(context.dataframe1)}')
    csv_file_path_str_1 = str(context.CSV_PATH) + '/ts_01.csv'
    IO.dataframe_to_csv_file(context.dataframe1, Path(csv_file_path_str_1))
    csv_file_path_str_2 = str(context.CSV_PATH) + '/ts_02.csv'
    IO.dataframe_to_csv_file(context.dataframe2, Path(csv_file_path_str_2))


@then(
    u'I have a Parquet file in T8S_WORKSPACE/data/parquet correctelly formated with metadata annotations'
)
def save_parquet(context):
    logger.info(f'@then: => PARQUET_PATH = {context.PARQUET_PATH}')
    logger.info(f'@then: => len(context.dataframe1) = \n{len(context.dataframe1)}')
    logger.info(f'@then: => len(context.dataframe2) = \n{len(context.dataframe2)}')

    def write_ts_to_parquet_file(ts, parquet_path, filename: str):
        parquet_file_path_str: str = str(parquet_path) + '/' + filename
        path_ts = Path(parquet_file_path_str)
        # Devido a problemas de 'circular import' tivemos que usar a classe Util
        Util.to_parquet(ts, path_ts)

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
            msg = f'context.text must be equal to str(ts1). Failed in line {idx} -> "{line}" != "{ts1_line[idx]}"'
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
    logger.info(
        'the_table() called ...\n'
        + str(context.input)
        + ' ->'
        + str(type(context.input))
    )
    logger.info(f'{table.headings}')
    for row in table.rows:
        logger.info(f'{row}')
    logger.info(f'the_table() passed !')


@when(
    u'converted to a data frame using {column_levels:d} row as column names and {index_levels:d} column as index'
)
def to_dataframe(context, column_levels, index_levels):
    parsed: pd.DataFrame = table_to_dataframe(
        context.input, column_levels=column_levels
    )
    for idx, col in enumerate(parsed.columns):
        logger.info(f'{idx}\t-> {col}, {type(parsed[col])}, {type(parsed[col][0])}')
    context.parsed = parsed
    logger.info(
        f'context.parsed =\n{context.parsed}\nType of context.parsed =\n{type(context.parsed)}'
    )
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


# -----------------------------------------------------------------------------------------------

"""
  Scenario: Third, I create a time series using a datafusion query and save as a parquet file
    Given a Datafusion SQL query
    When I convert a Datafusion Table to a Pandas Dataframe using the Datafusion API and the query mentioned above
    And I create a time series
    Then I have a time series with the `correct` number of rows and columns, schema and time interval to be checked
    # https://github.com/apache/arrow-datafusion-python/blob/main/examples/sql-on-pandas.py
"""


@given(u'a Datafusion SQL query')
def step_impl1(context):
    logger.info(u'STEP: Given a Datafusion SQL query')
    logger.info(context.ts1)


@when(
    u'I convert a Datafusion Table to a Pandas Dataframe using the Datafusion API and the query mentioned above'
)
def step_impl2(context):
    logger.info(
        u'STEP: When I convert a Datafusion Table to a Pandas Dataframe using the Datafusion API and the query mentioned above'
    )


@when(u'I create a time series')
def step_impl3(context):
    logger.info(u'STEP: When I create a time series')


@then(
    u'I have a time series with the `correct` number of rows and columns, schema and time interval to be checked'
)
def step_impl4(context):
    logger.info(
        u'STEP: Then I have a time series with the `correct` number of rows and columns, schema and time interval to be checked'
    )


# -----------------------------------------------------------------------------------------------


@given(u'a time series')
def step_impl_5(context):
    logger.info(u'STEP: Given a time series')
    logger.info(context.ts1)


@when(u'I call the get_statistics function')
def step_impl_6(context):
    logger.info(u'STEP: When I call the get_statistics function')
    logger.info(context.ts1)
    stats: TSStats = context.ts1.get_statistics()
    context.stats = stats


@then(u'I have a descriptive statistics object for the time series')
def step_impl_7(context):
    logger.info(
        u'STEP: Then I have a descriptive statistics object for the time series'
    )
    print(f'ts1 statistics =\n{context.stats}')
    assert context.stats is not None
    assert context.stats.count('timestamp') == 4
    assert context.stats.mean('velocidade') - 2325 <= epsilon
    assert context.stats.mean('temperatura') - 25.299999 <= epsilon
    assert context.stats.min('velocidade') - 1100 <= epsilon
    assert context.stats.min('temperatura') - 23.2 <= epsilon
    assert context.stats.q1('velocidade') - 1175 <= epsilon
    assert context.stats.q1('temperatura') - 24.55 <= epsilon
    assert context.stats.q2('velocidade') - 2100 <= epsilon
    assert context.stats.q2('temperatura') - 25.50 <= epsilon
    assert context.stats.q3('velocidade') - 3250 <= epsilon
    assert context.stats.q3('temperatura') - 26.25 <= epsilon
    assert context.stats.max('velocidade') - 4000 <= epsilon
    assert context.stats.max('temperatura') - 27 <= epsilon
    assert context.stats.std('velocidade') - 1417.450806 <= epsilon
    assert context.stats.std('temperatura') - 1.620699 <= epsilon
    """
    ts1 statistics =
                                timestamp  temperatura   velocidade
    Contagem                            4     4.000000     4.000000
    Média             2022-01-01 01:30:00    25.299999  2325.000000
    Mínimo            2022-01-01 00:00:00    23.200001  1100.000000
    Primeiro quartil  2022-01-01 00:45:00    24.550000  1175.000000
    Mediana           2022-01-01 01:30:00    25.500000  2100.000000
    Terceiro quartil  2022-01-01 02:15:00    26.250000  3250.000000
    Máximo            2022-01-01 03:00:00    27.000000  4000.000000
    Desvio padrão                     NaN     1.620699  1417.450806
    """


# -----------------------------------------------------------------------------------------------


@when(u'I call the get_min_max_variation_factors function in the Util class')
def step_impl_8(context):
    logger.info(
        u'STEP: When I call the get_min_max_variation_factors function in the Util class'
    )
    min_max_variation_factors = Util.get_min_max_variation_factors(context.ts1.df)
    context.min_max_variation_factors = min_max_variation_factors


@then(
    u'I have a dictionary object with minimum and maximum multiplication factor for each feature'
)
def step_impl_9(context):
    logger.info(
        u'STEP: Then I have a dictionary object with minimum and maximum multiplication factor for each feature'
    )
    logger.info(f'min_max_variation_factors =\n{context.min_max_variation_factors}')


@then(u'I can use this information to check for outliers using a naive method')
def step_impl_10(context):
    logger.info(
        u'STEP: Then I can use this information to check for outliers using a naive method'
    )
    logger.info(
        f'If min_max_variation_factors is greater than 3 in at least one column, we say that there is an outlier in the time series'
    )


# -----------------------------------------------------------------------------------------------


@when(u'I pass select_features as a list of feature names to the TimeSerie constructor')
def read_ts_for_selected_features(context):
    logger.info(
        u'STEP: When I pass select_features as a list of feature names to the TimeSerie constructor'
    )
    ctx = TSBuilder(ReadParquetFile())
    logger.debug("Client: ReadStrategy is set to read Parquet file.")
    path = Path(context.PARQUET_PATH) / 'ts_01.parquet'
    start = datetime.now()
    select_features = ['timestamp', 'velocidade']
    ts_for_selected_features = ctx.build_from_file(path, select_features)
    end = datetime.now()
    context.ts_for_selected_features = ts_for_selected_features
    context.elapsed_time_for_read_only_selected_features = end - start


@then(u'I have a time series with only a subset of features as defined in the list')
def check_time_serie_with_two_features(context):
    logger.info(
        u'STEP: Then I have a time series with only a subset of features as defined in the list'
    )
    logger.info(f'context.ts_for_select_features =\n{context.ts_for_selected_features}')
    assert context.ts_for_selected_features.format == 'wide', 'format must be wide'
    assert (
        context.ts_for_selected_features.features == '2'
    ), 'features_qty must be 2 in this case'
    assert (
        context.ts_for_selected_features.df.columns[0] == 'timestamp'
    ), 'first column must be timestamp'
    assert (
        context.ts_for_selected_features.df.columns[1] == 'velocidade'
    ), 'second column must be velocidade'


@then(
    u'I can read from the file system only the resources I need, improving performance when reading data.'
)
def time_for_read_from_parquet_file_only_2_coluns(context):
    logger.info(
        u'STEP: Then I can read from the file system only the resources I need, improving performance when reading data.'
    )
    logger.info(
        f'context.elapsed_time_for_read_only_selected_features = {context.elapsed_time_for_read_only_selected_features}'
    )
