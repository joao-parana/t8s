import os
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler # type: ignore
from sklearn.base import TransformerMixin   # type: ignore
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

def main():
    # ----------------- Exemplo de seleção em uma série temporal -----------------
    # Cria um DataFrame de exemplo com três colunas e 10 linhas
    df = pd.DataFrame(columns=['timestamp', 'velocidade', 'temperatura'])
    # Cria 10 registros de série temporal com intervalo de 1 minuto
    start_time = pd.Timestamp.now()
    for i in range(10):
        timestamp = start_time + pd.Timedelta(minutes=i)
        velocidade = np.random.randint(40, 100)
        temperatura = np.random.randint(17, 35)
        df.loc[i] = [timestamp, velocidade, temperatura] # type: ignore

    # Cria uma date_range de 6 minutos no meio da série temporal
    middle_time = start_time + pd.Timedelta(minutes=4)
    date_range = pd.date_range(middle_time - pd.Timedelta(minutes=3), middle_time + pd.Timedelta(minutes=3), freq='1min')

    print(f'date_range = {date_range}')

    # Seleciona as linhas dentro da date_range
    df_selecionado = df[df['timestamp'].isin(date_range)]

    # Exibe o DataFrame selecionado
    print(f'df_selecionado =\n{df_selecionado}')

    #----------------- Exemplo de normalização e denormalização de dados -----------------
    # Função para reverter a normalização
    # https://stackoverflow.com/questions/26414913/normalize-columns-of-pandas-data-frame
    # def denormalize(scaler):
    #     if isinstance(scaler, MinMaxScaler):
    #         return lambda x: (x + 1) / 2 * (scaler.data_max_ - scaler.data_min_) + scaler.data_min_
    #     elif isinstance(scaler, RobustScaler):
    #         return lambda x: (x * scaler.scale_) + scaler.center_
    #     else:
    #         raise ValueError('Unsupported scaler')

    def create_ts():
        path_str: str = 'data/parquet/ts_01.parquet'
        path = Path(path_str)
        logger.debug('path: ' + str(path))
        ctx = TSBuilder(ReadParquetFile())
        ts1: TimeSerie = ctx.build_from_file(Path(path_str))
        return ts1

    # Cria um DataFrame de exemplo
    ts1 = create_ts()
    df = ts1.df
    print(df)

    # Cria um scaler para normalizar os dados
    scaler: TransformerMixin = MinMaxScaler(feature_range=(-1, 1))
    numeric_columns: list[str] = ['temperatura', 'velocidade']
    # Normaliza os dados do DataFrame
    ts1_normalized: TimeSerie = ts1.normalize(scaler, numeric_columns, inplace=False)
    # print(f'df_norm =\n{df_norm}\n{type(df_norm)}')
    logger.info(f'ts1_normalized =\n{ts1_normalized}\n{type(ts1_normalized)}')
    # Sem opção inplace=True, o método denormalize() retorna um novo objeto TimeSerie
    ts1_normalized.denormalize()
    # Obtém os parâmetros do scaler para reverter a normalização
    # min_max = scaler.data_min_, scaler.data_max_


    # # Reverte a normalização de um conjunto de valores. Observe que podem haver diferenças de arredondamento
    # # entre os valores originais e os valores revertidos devido a questões intrinsecas do formato float.
    # # Considere epsilon = 1e-6 para valores float32 e epsilon = 1e-8 para valores float64
    # for idx, row in df_norm.iterrows():
    #     print('-----------------------------------------------------')
    #     print(f'idx = {idx}, row =\n{row}')
    #     print(f'row[numeric_columns] =\n{row[numeric_columns]}')
    #     print(f'denormalize(scaler)(row[numeric_columns]) = {denormalize(scaler)(row[numeric_columns])}')

    # # Altera o DataFrame de exemplo inserindo outliers
    # df = pd.DataFrame({'Coluna1': [1, 2, 23, 4, 5], 'Coluna2': [10, 20, 130, 40, 50]})
    #
    # # Cria um scaler para normalizar os dados
    # scaler = RobustScaler()
    #
    # # Normaliza os dados do DataFrame
    # df_norm = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
    #
    # # Exibe o DataFrame normalizado
    # print(df_norm, type(df_norm))
    #
    # ret = denormalize(scaler)([1/3, -2/3])
    # print(ret, type(ret))


@given(u'that I have a T8S_WORKSPACE_DIR and a wide format time series available as Parquet file')
def setup(context):
    logger.info(u'STEP: Given that I have a T8S_WORKSPACE_DIR and a wide format time series available as Parquet file')
    logger.info(f'-------------------------------------------------')
    logger.info(f'Background @given: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'Background@given:  CSV_PATH = {context.CSV_PATH}')
    logger.info(f'Background@given:  PARQUET_PATH = {context.PARQUET_PATH}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context


@given(u'that I create a multivariate Timeseries using the selected parquet file')
def create_time_series(context):
    logger.info(u'STEP: Given that I create a multivariate Timeseries using the selected parquet file')
    logger.info(f'create_ts: PARQUET_PATH = {context.PARQUET_PATH}')
    def create_ts(filename):
        path_str: str = str(context.PARQUET_PATH) + '/' + filename
        path = Path(path_str)
        logger.debug('path: ' + str(path))
        ctx = TSBuilder(ReadParquetFile())
        ts1: TimeSerie = ctx.build_from_file(Path(path_str))
        return ts1
    file_name_of_time_series_in_wide_format = 'ts_01.parquet'
    ts1 = create_ts(file_name_of_time_series_in_wide_format)
    assert int(ts1.features) == 3
    assert ts1.format == 'wide'
    assert len(ts1.df) == 4
    context.ts1 = ts1
    logger.info(context.ts1)

@when(u'I normalize the multivariate time series data using the chosen methods below')
def normalize(context):
    logger.info(u'STEP: When I normalize the multivariate time series data using the chosen methods below')
    ts1 = context.ts1
    scaler: TransformerMixin = MinMaxScaler(feature_range=(-1, 1))
    assert isinstance(scaler, TransformerMixin)
    # TODO: corrigir problema KeyError: None que ocorre no Pandas. OBS: O código está funcionando no SmokeTest
    context.ts1 = ts1.normalize(scaler, numeric_columns = None, inplace = True)
    logger.info(context.ts1)

@then(u'I check the result of normalization running the inverse operation (denormalize) for some values')
def check_normalization(context):
    logger.info(u'STEP: Then I check the result of normalization running the inverse operation (denormalize) for some values')

@when(u'I normalize only some of the features in the multivariate time series data using the methods below')
def normalize_some_features(context):
    logger.info(u'STEP: When I normalize only some of the features in the multivariate time series data using the methods below')

if __name__ == '__main__':
    main()
