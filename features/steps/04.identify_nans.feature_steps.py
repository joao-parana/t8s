import os
from datetime import datetime
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from behave import given, step, then, use_step_matcher, when  # type: ignore
from behave.model import Table  # type: ignore
from behave_pandas import dataframe_to_table, table_to_dataframe  # type: ignore
from pandas.core.series import Series

from t8s import get_sample_df
from t8s.io import IO
from t8s.log_config import LogConfig
from t8s.ts import TimeSerie
from t8s.ts_builder import ReadParquetFile, TSBuilder
from t8s.ts_writer import TSWriter, WriteParquetFile
from t8s.util import Util

logger = LogConfig().get_logger()


def create_df_with_nans() -> pd.DataFrame:
    # Criando um DataFrame de exemplo
    values_a = [
        8.0,
        5.0,
        4.0,
        3.0,
        6.0,
        18.0,
        np.nan,
        np.nan,
        np.nan,
        14.0,
        4.0,
        4.0,
        8.0,
        20.0,
        7.0,
        10.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        16.0,
        8.0,
        12.0,
        np.nan,
        9.0,
        10.0,
        4.0,
        6.0,
        5.0,
        np.nan,
        6.0,
        17.0,
        4.0,
        np.nan,
        np.nan,
        9.0,
        4.0,
        5.0,
        7.0,
    ]
    p = len(values_a)
    values_b = [x + 1.1 for x in values_a]
    df = pd.DataFrame(
        {
            't': pd.date_range('2023-07-26', periods=p, freq='D'),
            'a': values_a,
            'b': values_b,
        }
    )
    return df


"""
Feature: Identify NaN values in multivariate and univariate Timeseries on wide format
  Value Statement:
    As a data analyst
    I want the ability to identify NaN values in multivariate and univariate Timeseries on wide format
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a TimeSerie with a bunch of NaNs blocks saved as a parquet file in T8S_WORKSPACE_DIR
"""
# my_dict_for_all_columns, last_idx = Util.identify_all_start_and_end_of_nan_block(dados)


@given(
    u'that I have a TimeSerie with a bunch of NaNs blocks saved as a parquet file in T8S_WORKSPACE_DIR'
)
def check_for_wide_ts_as_parquet(context):
    logger.info(
        u'STEP: Given that I have a TimeSerie with a bunch of NaNs blocks saved as a parquet file in T8S_WORKSPACE_DIR'
    )
    logger.info(f'@given: => PARQUET_PATH = {context.PARQUET_PATH}')

    df = create_df_with_nans()

    def create_ts_and_save(df, filename) -> TimeSerie:
        path_str: str = str(context.PARQUET_PATH) + '/' + filename
        path_ts = Path(path_str)
        logger.info('path_ts: ' + str(path_ts))
        ts = TimeSerie(df, format='wide', features_qty=len(df.columns))
        # Grava a série temporal ts1 em parquet
        Util.to_parquet(ts, path_ts)
        return ts

    ts3 = create_ts_and_save(df, 'ts_03.parquet')

    logger.info(f'ts3 =\n{str(ts3)}')
    context.ts3 = ts3


"""
  Scenario: Identify NaN values in univariate Timeseries on wide format
    Given that I read a multivariate Timeseries and convert to univariate timeseries list
    When I check the first univariate Timeseries from list for NaN values
    Then I build a dataframe describing blocks of NaN values to use elsewhere
    And I check the result of NaNs blocks of univariate Timeseries.
    # Constraint: The Timeseries has no invalid values
"""


@given(
    u'that I read a multivariate Timeseries and convert to univariate timeseries list'
)
def read_multivariate_ts(context):
    logger.info(
        u'STEP: Given that I read a multivariate Timeseries and convert to univariate timeseries list'
    )
    # posso pegar direto do contexto, já que salvei depois de criar o arquivo Parquet
    ts3: TimeSerie = context.ts3

    # Exibe um grafico de linha com os valores de a e b variando no tempo. NaNs não são exibidos.
    # A visualização permite ter uma ideia de como os dados estão distribuídos.
    ts3.plot.line(figsize=(12, 5), grid=True)

    # Gero a lista de univariadas e salvo no contexto
    ts_list: list[TimeSerie] = ts3.split()
    context.ts_list = ts_list
    # Aqui temos context.ts3 (série multivariada) e context.ts_list (lista de séries univariadas)


@when(u'I check the first univariate Timeseries from list for NaN values')
def check_the_first_univariate_ts(context):
    logger.info(
        u'STEP: When I check the first univariate Timeseries from list for NaN values'
    )
    ts_list: list[TimeSerie] = context.ts_list
    first_univariate_ts: TimeSerie = ts_list[0]
    logger.info(f'first_univariate_ts =\n{first_univariate_ts}')
    # The first univariate Timeseries from list is for 'a' column
    # Verificando se existe NaNs na série temporal univariada
    assert (
        first_univariate_ts.df['a'].isnull().values.any() == True
    ), "There must be NaNs in the univariate Timeseries"
    context.first_univariate_ts = first_univariate_ts
    first_univariate_ts.plot.line(figsize=(12, 5), grid=True)


@then(u'I build a dataframe describing blocks of NaN values to use elsewhere')
def build_dict_for_nans_blocks(context):
    logger.info(
        u'STEP: Then I build a dictionary list of NaN values blocks to use elsewhere'
    )
    # The first univariate Timeseries from list is for 'a' column
    first_univariate_ts = context.first_univariate_ts
    start_end_of_nan_blocks: pd.DataFrame | None = (
        Util.identify_start_and_end_of_nan_block(first_univariate_ts.df, 'a')
    )
    logger.info(f'start_end_of_nan_blocks =\n{start_end_of_nan_blocks}')
    context.ts_nan_dict = start_end_of_nan_blocks


@then(u'I check the result of NaNs blocks of univariate Timeseries')
def check_the_result_of_nans_blocks(context):
    logger.info(
        u'STEP: Then I check the result of NaNs blocks of univariate Timeseries'
    )
    assert context.ts_nan_dict is not None, "ts_nan_dict must not be None"
    ts_nan_dict = context.ts_nan_dict
    value: Series = ts_nan_dict['Value']
    counts: Series = ts_nan_dict['Counts']
    start: Series = ts_nan_dict['Start']
    # Verificar se o primeiro elemento é False
    assert value[0] == False, "First element of Value must be False"
    # Verificar se os elementos pares são False
    assert all(value[::2] == False), "Even elements of Value must be False"
    # Verificar se os elementos ímpares são True
    assert all(value[1::2] == True), "Odd elements of Value must be True"
    # Verificar se a soma dos elementos de Counts é igual ao tamanho do DataFrame
    assert counts.sum() == len(
        context.ts3.df
    ), "Sum of Counts must be equal to DataFrame length"
    logger.debug(f'type(start) = {type(start)}')
    # Verifica se o ultimo elemento de start é igual ao tamanho do DataFrame
    last_start = start[len(start) - 1]
    last_count = counts[len(counts) - 1]
    ts_length = len(context.ts3.df)
    assert (
        last_start + last_count == ts_length
    ), "Last element of Start and Count is related to DataFrame length"
    """
    start_end_of_nan_blocks =
        Value  Counts  Start
    0   False       6      0
    1    True       3      6
    2   False       7      9
    3    True       5     16
    4   False       3     21
    5    True       1     24
    6   False       5     25
    7    True       1     30
    8   False       3     31
    9    True       2     34
    10  False       4     36
    """


@then(
    u'I can also add a column with the corrections indicated by the imputation and see the result graphically.'
)
def add_imputation_column_and_show_graphically(context):
    logger.info(
        u'STEP: Then I can also add a column with the corrections indicated by the imputation and see the result graphically.'
    )
    ts_list: list[TimeSerie] = context.ts_list
    first_univariate_ts: TimeSerie = ts_list[0]
    # Faço uma cópia da  Série Temporal original
    # Adiciono uma coluna no Dataframe
    # first_univariate_ts.plot.line(figsize=(12, 5), grid=True)
    ts4 = first_univariate_ts.add_nan_mask(
        inplace=False, plot=True, method='interpolate'
    )
    context.ts4 = ts4
    logger.info(f'ts4 =\n{ts4}')
