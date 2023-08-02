import os
import sys
from datetime import datetime
from pathlib import Path
from t8s.log_config import LogConfig
from behave import given, when, then, model, step # type: ignore
from behave.model import Feature, Scenario # type: ignore
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL, log
from behave import use_step_matcher # type: ignore
import t8s
from t8s.util import Util
from t8s.ts import TimeSerie
from t8s import get_sample_df

# use_step_matcher("re")
use_step_matcher("parse")

LogConfig().initialize_logger(INFO)
logger = LogConfig().getLogger()
print(f'\n\n• The script "environment.py" was loaded ...\n')
print(f'\n• The python version used is "{sys.version_info}"\n')
print(f'\n• t8s package version is {t8s.__version__}\n\n')

def list_files(prefix: str, context):
    l: list = Util.list_all_files(context.PARQUET_PATH)
    for item in l:
        logger.info(prefix + item)

def before_all(context):
    # Inicializa o contexto do behave
    context.status = 'initializing . . .'
    context.list_files = list_files
    logger.info(f'\n\nbefore_all() called . . .\n')
    logger.info(f'-------------------------------------------------')
    T8S_WORKSPACE_DIR = os.environ.get('T8S_WORKSPACE_DIR', '/Volumes/dev/t8s')
    CSV_PATH_STR:str = os.path.join(T8S_WORKSPACE_DIR, 'data', 'csv')
    CSV_PATH: Path = Path(CSV_PATH_STR)
    PARQUET_PATH_STR:str = os.path.join(T8S_WORKSPACE_DIR, 'data', 'parquet')
    PARQUET_PATH: Path = Path(PARQUET_PATH_STR)
    context.T8S_WORKSPACE_DIR = T8S_WORKSPACE_DIR
    context.CSV_PATH = CSV_PATH
    context.PARQUET_PATH = PARQUET_PATH
    logger.info(f'before_all: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'before_all: CSV_PATH          = {context.CSV_PATH}')
    logger.info(f'before_all: PARQUET_PATH      = {context.PARQUET_PATH}')
    create_sample_ts_and_save_as_parquet(context)
    context.list_files(f'before_all: ', context)
    logger.info(f'-------------------------------------------------')

def write_ts_to_parquet_file(ts, parquet_path, filename: str):
    parquet_file_path_str: str = str(parquet_path) + '/' + filename
    path_ts = Path(parquet_file_path_str)
    # Devido a problemas de 'circular import' tivemos que usar a classe Util
    Util.to_parquet(ts, path_ts)

def create_sample_ts_and_save_as_parquet(context):
    start_timestamp = datetime(2022, 1, 1, 0, 0, 0)
    number_of_records = 4
    time_interval = 1 # hour
    dataframe, last_ts = get_sample_df(number_of_records, start_timestamp, time_interval)
    context.ts1 = TimeSerie(dataframe, format='wide', features_qty=len(dataframe.columns))
    # Grava a série temporal ts1 em parquet
    write_ts_to_parquet_file(context.ts1, context.PARQUET_PATH, 'ts_01.parquet')


def clean_data_dir(CSV_PATH, PARQUET_PATH):
    logger.info(f'clean_data_dir() called ...')
    for diretory in [CSV_PATH, PARQUET_PATH]:
        logger.info(f'clean_data_dir() -> diretory = {diretory}')
        for root, dirs, files in os.walk(diretory):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        for root, dirs, files in os.walk(diretory):
            for file in files:
                print(os.path.join(root, file))
            for dir in dirs:
                print(os.path.join(root, dir))
    return 'data directory empty'

# HOOK
def before_feature(context, feature: Feature):
    # Inicializa o status no contexto
    context.status = context.status if hasattr(context, 'status') else 'not defined'
    # Passo funções utilitárias via contexto para serem usadas nas steps.
    context.create_sample_ts_and_save_as_parquet = create_sample_ts_and_save_as_parquet
    # Execute um método uma única vez para todos os cenários de teste da Feature
    logger.info(f'-------------------------------------------------')
    logger.info(f'before_feature: T8S_WORKSPACE_DIR = {context.T8S_WORKSPACE_DIR}')
    logger.info(f'before_feature:  CSV_PATH         = {context.CSV_PATH}')
    logger.info(f'before_feature:  PARQUET_PATH     = {context.PARQUET_PATH}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context

    # Atualiza o status no contexto
    if feature.filename == 'feature/01.create_timeserie.feature':
        pass

    logger.info(f'-------------------------------------------------')

# HOOK
def before_scenario(context, scenario: Scenario):
    logger.info(f'-------------------------------------------------')
    logger.info(f'before scenario: {scenario.name}')
    logger.info(f' context.status: {context.status}')
    logger.info(f'-------------------------------------------------')
