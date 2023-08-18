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
Feature: Identify NaN values in multivariate and univariate Timeseries on wide format
  Value Statement:
    As a data analyst
    I want the ability to identify NaN values in multivariate and univariate Timeseries on wide format
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a wide format time series persisted to a Parquet file
"""
# my_dict_for_all_columns, last_idx = Util.identify_all_start_and_end_of_nan_block(dados)

@given(u'that I have a T8S_WORKSPACE_DIR and a wide format time series persisted to a Parquet file')
def check_for_wide_ts_as_parquet(context):
    logger.debug(u'STEP: Given that I have a T8S_WORKSPACE_DIR and a wide format time series persisted to a Parquet file')
    logger.info(f'@given: => PARQUET_PATH = {context.PARQUET_PATH}')
    def create_ts(filename):
        file_name_of_time_series_in_wide_format = filename
        path_str: str = str(context.PARQUET_PATH) + '/' + file_name_of_time_series_in_wide_format
        path = Path(path_str)
        logger.debug('path: ' + str(path))
        ctx = TSBuilder(ReadParquetFile())
        ts1: TimeSerie = ctx.build_from_file(Path(path_str))
        assert int(ts1.features) == 3
        assert ts1.format == 'wide'
        assert len(ts1.df) == 4
        return ts1

    ts1 = create_ts('ts_01.parquet')

    # Adiciono NaNs em alguns pontos
    # Coluna de indice não é considerada no atributo 'iloc'
    logger.debug(f'ts1.df.index.name: {str(ts1.df.index.name)}')
    if ts1.df.index.name == 'None':
        ts1.df.iloc[1, 1] = np.nan
        ts1.df.iloc[1, 2] = np.nan
        ts1.df.iloc[2, 2] = np.nan
    else:
        ts1.df.iloc[1, 1] = np.nan
        ts1.df.iloc[1, 2] = np.nan
        ts1.df.iloc[2, 2] = np.nan
    logger.debug(f'ts1 =\n{str(ts1)}')
    context.ts1 = ts1

"""
  Scenario: Identify NaN values in multivariate Timeseries on wide format
    Given that I create a multivariate Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory
    When I check the multivariate Timeseries for NaN values
    Then I build a dictionary of NaN values blocks to use elsewhere
    And I check the result of NaNs blocks.
    # Constraint: The Timeseries has no invalid values
"""

@given(u'that I create a multivariate Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory')
def create_multivariate_ts(context):
    logger.debug(u'STEP: Given that I create a univariate Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory')
    ts_multivariate: TimeSerie = context.ts1
    ts_list: list[TimeSerie] = context.ts1.split()
    context.ts_list = ts_list
    # Aqui temos context.ts1 () e context.ts_list ()

@when(u'I check the multivariate Timeseries for NaN values')
def check_ts_for_nans(context):
    logger.debug(u'STEP: When I check the multivariate Timeseries for NaN values')
    ts_dict: list[dict[str, pd.DataFrame]] = {} # type: ignore
    ts1 = context.ts1
    logger.debug(f'ts_multivariate =\n{ts1}')
    my_dict_for_multivariate_ts, _ = Util.identify_all_start_and_end_of_nan_block(ts1.df)
    logger.debug(f'my_dict_for_multivariate_ts =\n{my_dict_for_multivariate_ts}')
    context.ts_nan_dict = ts_dict

@then(u'I build a dictionary of NaN values blocks to use elsewhere')
def build_dict_for_nans_blocks(context):
    my_dict_for_all_columns, last_idx = Util.identify_all_start_and_end_of_nan_block(context.ts1.df)
    logger.debug(u'STEP: Then I build a dictionary of NaN values blocks to use elsewhere')
    ts_nan_dict = context.ts_nan_dict

#
@then(u'I check the result of NaNs blocks.')
def check_the_result_of_nans_blocks(context):
    logger.debug(u'STEP: Then I check the result of NaNs blocks.')


"""
  Scenario: Identify NaN values in univariate Timeseries on wide format
    Given that I create a univariate Timeseries set using the selected parquet file in the T8S_WORKSPACE/data/parquet directory
    When I check the univariate Timeseries for NaN values
    Then I build a dictionary list of NaN values blocks to use elsewhere
    And I check the result of NaNs blocks of univariate Timeseries.
    # Constraint: The Timeseries has no invalid values
"""

@given(u'that I create a univariate Timeseries set using the selected parquet file in the T8S_WORKSPACE/data/parquet directory')
def create_univariate_ts(context):
    logger.debug(u'STEP: Given that I create a univariate Timeseries set using the selected parquet file in the T8S_WORKSPACE/data/parquet directory')
    ts_multivariate: TimeSerie = context.ts1
    ts_list: list[TimeSerie] = context.ts1.split()
    context.ts_list = ts_list
    # Aqui temos context.ts1 () e context.ts_list ()

@when(u'I check the univariate Timeseries for NaN values')
def check_the_result_for_nans(context):
    logger.debug(u'STEP: When I check the univariate Timeseries for NaN values')
    ts_dict_list: list[dict[str, pd.DataFrame]] = [] # type: ignore
    ts_list = context.ts_list
    for ts_univariate in ts_list:
        logger.debug(f'ts_univariate =\n{ts_univariate}')
        my_dict_for_univariate_ts, _ = Util.identify_all_start_and_end_of_nan_block(ts_univariate.df)
        logger.debug(f'my_dict_for_univariate_ts =\n{my_dict_for_univariate_ts}')
        ts_dict_list.append(my_dict_for_univariate_ts)

    context.ts_nan_dict_list = ts_dict_list

@then(u'I build a dictionary list of NaN values blocks to use elsewhere')
def build_dict_list_for_nans_blocks(context):
    # my_dict_for_all_columns, last_idx = Util.identify_all_start_and_end_of_nan_block(context.ts1.df)
    logger.debug(u'STEP: Then I build a dictionary list of NaN values blocks to use elsewhere')
    # ts_nan_dict = context.ts_nan_dict

@then(u'I check the result of NaNs blocks of univariate Timeseries.')
def check_the_result_of_nans(context):
    logger.debug(u'STEP: Then I check the result of NaNs blocks of univariate Timeseries.')
