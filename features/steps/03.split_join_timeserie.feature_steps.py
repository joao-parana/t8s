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
from t8s.ts_builder import TSBuilder
from t8s.ts_builder import ReadParquetFile
from behave import given, when, then, use_step_matcher, step # type: ignore
from behave.model import Table # type: ignore
from behave_pandas import table_to_dataframe, dataframe_to_table # type: ignore
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

logger = LogConfig().getLogger()

"""
Feature: Convert a multivariate Timeseries to list of univariate Timeseries and vice versa

Value Statement:
    As a data analyst
    I want the ability to convert between Timeseries types ['univariate', 'multivariate'] for use in different situations
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a long format time series persisted to a Parquet file
"""

@given(u'that I have a T8S_WORKSPACE_DIR and a long format time series persisted to a Parquet file')
def background(context):
    logger.info(u'STEP: Given that I have a T8S_WORKSPACE_DIR and a long format time series persisted to a Parquet file')

    if context.status == 'data directory empty':
        context.create_sample_ts_and_save_as_parquet(context)
        context.status = 'data directory with parquet file'

    # O método before_feature() em features/environment.py atualiza o contexto
    logger.info(f'-------------------------------------------------')
    logger.info(f'Background @given: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'Background@given:  CSV_PATH = {context.CSV_PATH}')
    logger.info(f'Background@given:  PARQUET_PATH = {context.PARQUET_PATH}')
    context.list_files(f'Background@given:  ', context)
    # logger.info(f'\background : context.ts1 -> \n{str(context.ts1)}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context

"""
  Scenario: Conversion of Timeseries types ['univariate', 'multivariate'] for use in different situations
    Given that I create a Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory
    When I convert Timeseries from long format to wide format and check the convertion
    Then I can convert the Timeseries from multivariate to a list of univariate Timeseries
    And I convert the list of univariate Timeseries into a single multivariate Timeseries
    And I check the result.
    # Constraint: The Timeseries has no invalid values
"""

@given(u'that I create a Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory')
def create_time_serie_from_parquet_file(context):
    file_name_of_time_series_in_long_format = 'ts_long_01.parquet'
    path_str: str = str(context.PARQUET_PATH) + '/' + file_name_of_time_series_in_long_format
    path = Path(path_str)
    logger.debug('path: ' + str(path))
    ctx = TSBuilder(ReadParquetFile())
    ts1: TimeSerie = ctx.build_from_file(Path(path_str))
    assert int(ts1.features) == 3
    assert ts1.format == 'long'
    assert len(ts1.df) == 8
    context.ts1 = ts1
    context.list_files(f'create_time_serie_from_parquet_file() \n', context)

@when(u'I convert Timeseries from long format to wide format and check the convertion')
def convert_time_serie_from_long_to_wide_format(context):
    logger.info(f'context.ts1 BEFORE -> \n{str(context.ts1.format)}')
    ts1: TimeSerie = context.ts1
    ts1.to_wide()
    logger.info(f'context.ts1 AFTER  -> \n{str(context.ts1.format)}\n{str(context.ts1.df.head())}')

@then(u'I can convert the Timeseries from multivariate to a list of univariate Timeseries')
def convert_time_serie_from_multivariate_to_list_of_univariate(context):
    # logger.info(f'context.ts1 BEFORE -> \n{str(context.ts1)}')
    pass

@then(u'I convert the list of univariate Timeseries into a single multivariate Timeseries')
def convert_list_of_univariate_to_multivariate(context):
    # logger.info(f'context.ts1 BEFORE -> \n{str(context.ts1)}')
    pass

@then(u'I check the result.')
def check_result(context):
    # logger.info(f'context.ts1 BEFORE -> \n{str(context.ts1)}')
    logger.info(f'split/join test passed')
