import os
from pathlib import Path
from t8s.log_config import LogConfig
from behave import given, when, then, model, step
from behave.model import Feature, Scenario
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL, log

LogConfig().initialize_logger(DEBUG)
logger = LogConfig().getLogger()
print(f'\n\n• The script "environment.py" was loaded ...\n')

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

def before_feature(context, feature: Feature):
    # Execute um método uma única vez para todos os cenários de teste da Feature
    logger.info(f'-------------------------------------------------')
    T8S_WORKSPACE_DIR = os.environ.get('T8S_WORKSPACE_DIR', '/Volumes/dev/t8s')
    CSV_PATH_STR:str = os.path.join(T8S_WORKSPACE_DIR, 'data', 'csv')
    CSV_PATH: Path = Path(CSV_PATH_STR)
    PARQUET_PATH_STR:str = os.path.join(T8S_WORKSPACE_DIR, 'data', 'parquet')
    PARQUET_PATH: Path = Path(PARQUET_PATH_STR)
    logger.info(f'before_feature: T8S_WORKSPACE_DIR = {T8S_WORKSPACE_DIR}')
    logger.info(f'before_feature:  CSV_PATH         = {CSV_PATH}')
    logger.info(f'before_feature:  PARQUET_PATH     = {PARQUET_PATH}')
    logger.info(f'-------------------------------------------------')
    # A forma de passar estes dados para os steps seguintes é usando o objeto context
    context.T8S_WORKSPACE_DIR = T8S_WORKSPACE_DIR
    context.CSV_PATH = CSV_PATH
    context.PARQUET_PATH = PARQUET_PATH
    # Atualiza o status no contexto
    context.status = clean_data_dir(context.CSV_PATH, context.PARQUET_PATH)
    logger.info(f'-------------------------------------------------')

def before_scenario(context, scenario: Scenario):
    logger.info(f'-------------------------------------------------')
    logger.info(f'before scenario: {scenario.name}')
    logger.info(f'context.status: {context.status}')
    logger.info(f'-------------------------------------------------')
