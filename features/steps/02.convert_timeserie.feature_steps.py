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
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile
from t8s.util import Util

logger = LogConfig().get_logger()

"""
Feature: Convert a time series from wide format to long format and vice versa

Value Statement:
    As a data analyst
    I want the ability to convert time series format ['long', 'wide'] for use in different applications
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a Parquet file to analyze
"""


@given(u'that I have a T8S_WORKSPACE_DIR and a Parquet file to analyze')
def background(context):
    logger.info(
        u'STEP: Given that I have a T8S_WORKSPACE_DIR and a Parquet file to analyze'
    )

    if context.status == 'data directory empty':
        context.create_sample_ts_and_save_as_parquet(context)
        context.status = 'data directory with parquet file'

    # O método before_feature() em features/environment.py atualiza o contexto
    logger.info(f'-------------------------------------------------')
    logger.info(f'Background @given: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'Background@given:  CSV_PATH = {context.CSV_PATH}')
    logger.info(f'Background@given:  PARQUET_PATH = {context.PARQUET_PATH}')
    context.list_files(f'Background@given:  ', context)
    logger.info(f'Background : context -> \ncontext.ts1 = \n{str(context.ts1)}')
    logger.info(f'Background : context -> dir(context) = \n{dir(context)})')
    logger.info(
        f'Background : context -> context.__dict__.keys() = \n{context.__dict__.keys()})'
    )
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context


"""
  Scenario:
    Given I create a time series using the selected parquet file at T8S_WORKSPACE/data/parquet directory
    When I convert the time series from the original wide format to long format
    Then I have a time series with 3 columns and the `correct` number of rows
    And I have a text representation for the time serie like this below
"""


@given(
    'I create a time series using the selected parquet file at T8S_WORKSPACE/data/parquet directory'
)
def create_time_serie(context):
    # Não preciso criar pois já tem uma série temporal no contexto
    if context.ts1:
        logger.info(f'create_time_serie: context.ts1 -> \n{str(context.ts1)}')
    else:
        raise Exception('context.ts1 is None')


@when('I convert the time series from the original wide format to long format')
def convert_time_serie_from_wide_to_long_format(context):
    logger.info(f'context.ts1 BEFORE -> \n{str(context.ts1)}')
    assert context.ts1 is not None, 'context.ts1 is None'
    assert context.ts1.format == 'wide', 'context.ts1.format is not wide'
    context.ts1.to_long()
    logger.info(f'context.ts1 AFTER  -> \n{str(context.ts1)}')


@then('I have a time series with 3 columns and the `correct` number of rows')
def check_time_serie(context):
    # logger.info(f'context.ts1 AFTER  -> \n{str(context.ts1)}')
    assert context.ts1 is not None, 'context.ts1 is None'
    assert context.ts1.format == 'long', 'context.ts1.format is not long'
    assert int(context.ts1.features) == 3, 'context.ts1.features is not 3'
    assert len(context.ts1.df) == 8, 'len(context.ts1.df) is not 8'
    assert (
        context.ts1.is_multivariate() == True
    ), 'context.ts1.is_multivariate() is not True'


@then('I have a text representation for the time serie like this below')
def check_time_serie_text_representation(context):
    # logger.info(f'context.ts1 AFTER  -> \n{str(context.ts1)}')
    pass


@then(
    'can I save this long format time series to a parquet file in the T8S_WORKSPACE_DIR/data/parquet directory'
)
def save_time_serie_to_parquet(context):
    # logger.info(f'context.ts1 AFTER  -> \n{str(context.ts1)}')
    def write_ts_to_parquet_file(ts, parquet_path, filename: str):
        parquet_file_path_str: str = str(parquet_path) + '/' + filename
        path_ts = Path(parquet_file_path_str)
        # Devido a problemas de 'circular import' tivemos que usar a classe Util
        Util.to_parquet(ts, path_ts)

    # Grava a série temporal ts1 em parquet
    write_ts_to_parquet_file(context.ts1, context.PARQUET_PATH, 'ts_long_01.parquet')
    context.list_files(f'save_time_serie_to_parquet:  ', context)


@then(
    u'finally I can create a time series using the long format parquet file created to check the schema and data.'
)
def create_ts_long_from_parquet(context):
    logger.info(
        u'STEP: Then finally I can create a time series using the long format parquet file created to check the schema and data.'
    )
