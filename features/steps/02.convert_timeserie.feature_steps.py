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
    logger.info(u'STEP: Given that I have a T8S_WORKSPACE_DIR and a Parquet file to analyze')

    if context.status == 'data directory empty':
        context.create_sample_ts_and_save_as_parquet(context)
        context.status = 'data directory with parquet file'

    # O método before_feature() em features/environment.py atualiza o contexto
    logger.info(f'-------------------------------------------------')
    logger.info(f'Background @given: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'Background@given:  CSV_PATH = {context.CSV_PATH}')
    logger.info(f'Background@given:  PARQUET_PATH = {context.PARQUET_PATH}')
    context.list_files(f'Background@given:  ', context)
    logger.info(f'\background : context.ts1 -> \n{str(context.ts1)}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context

"""
  Scenario:
    Given I create a time series using the selected parquet file at T8S_WORKSPACE/data/parquet directory
    When I convert the time series from the original wide format to long format
    Then I have a time series with 3 columns and the `correct` number of rows
    And I have a text representation for the time serie like this below
"""

@given('I create a time series using the selected parquet file at T8S_WORKSPACE/data/parquet directory')
def create_time_serie(context):
    # Não preciso criar pois já tem uma série temporal no contexto
    if context.ts1:
        logger.info(f'create_time_serie: context.ts1 -> \n{str(context.ts1)}')
    else:
        raise Exception('context.ts1 is None')

@when('I convert the time series from the original wide format to long format')
def convert_time_serie_from_wide_to_long_format(context):
    assert context.ts1 is not None, 'context.ts1 is None'
    assert context.ts1.format == 'wide', 'context.ts1.format is not wide'
    context.ts1 = context.ts1.to_long()

@then('I have a time series with 3 columns and the `correct` number of rows')
def check_time_serie(context):
    pass

@then('I have a text representation for the time serie like this below')
def check_time_serie_text_representation(context):
    pass
