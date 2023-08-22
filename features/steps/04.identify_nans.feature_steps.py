import os
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
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
    Given that I have a Dataframe with a bunch of NaNs blocks and saved as a TimeSerie object in a parquet file in T8S_WORKSPACE_DIR
"""
# my_dict_for_all_columns, last_idx = Util.identify_all_start_and_end_of_nan_block(dados)

def create_df_with_nans() -> pd.DataFrame:
    # Criando um DataFrame de exemplo
    values_a = [8.0, 5.0, 4.0, 3.0, 6.0, 18.0, np.nan, np.nan, np.nan, 14.0, 4.0, 4.0, 8.0, 20.0,
                7.0, 10.0, np.nan, np.nan, np.nan, np.nan, np.nan, 16.0, 8.0, 12.0, np.nan, 9.0,
                10.0, 4.0, 6.0, 5.0, np.nan, 6.0, 17.0, 4.0, np.nan, np.nan, 9.0, 4.0, 5.0, 7.0]
    p = len(values_a)
    values_b = [x + 1.1 for x in values_a]
    df = pd.DataFrame({
        't': pd.date_range('2023-07-26', periods=p, freq='D'),
        'a': values_a,
        'b': values_b
    })
    return df

def show_my_plot(df):
    # df.set_index('t', inplace=True)
    df.plot(x='t', y=['a', 'b'], figsize=(12, 5), grid=True)
    plt.show()

@given(u'that I have a Dataframe with a bunch of NaNs blocks and saved as a TimeSerie object in a parquet file in T8S_WORKSPACE_DIR')
def check_for_wide_ts_as_parquet(context):
    logger.debug(u'STEP: Given that I have a T8S_WORKSPACE_DIR and a wide format time series persisted to a Parquet file')
    logger.info(f'@given: => PARQUET_PATH = {context.PARQUET_PATH}')

    df = create_df_with_nans()

    # Exibe um grafico de linha com os valores de a e b variando no tempo. NaNs não são exibidos.
    show_my_plot(df)

    def create_ts_and_save(df, filename) -> TimeSerie:
        path_str: str = str(context.PARQUET_PATH) + '/' + filename
        path_ts = Path(path_str)
        logger.debug('path_ts: ' + str(path_ts))
        ts = TimeSerie(df, format='wide', features_qty=len(df.columns))
        # Grava a série temporal ts1 em parquet
        Util.to_parquet(ts, path_ts)
        return ts

    ts3 = create_ts_and_save(df, 'ts_03.parquet')

    logger.debug(f'ts3 =\n{str(ts3)}')
    context.ts3 = ts3

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
