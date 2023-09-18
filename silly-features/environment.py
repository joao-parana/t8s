import os
import sys
from datetime import datetime
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, log
from pathlib import Path

from behave import (  # type: ignore
    given,
    model,
    step,
    then,
    use_step_matcher,  # type: ignore
    when,
)
from behave.model import Feature, Scenario  # type: ignore

import t8s
from t8s.log_config import LogConfig
from t8s.util import Util

# use_step_matcher("re")
use_step_matcher("parse")

LogConfig().initialize_logger(INFO)
logger = LogConfig().get_logger()
print(f'\n\n• The script "environment.py" was loaded ...\n')
print(f'\n• The python version used is "{sys.version_info}"\n')
print(f'\n• t8s package version is {t8s.__version__}\n\n')


def list_files(prefix: str, path: Path):
    l: list = Util.list_all_files(path)
    for item in l:
        logger.info(prefix + item)


def before_all(context):
    # Inicializa o contexto do behave
    context.status = 'initializing . . .'
    context.list_files = list_files
    logger.info(f'\n\nbefore_all() called . . .\n')
    logger.info(f'-------------------------------------------------')
    context.list_files(f'before_all: ', Path('/tmp/'))
    logger.info(f'-------------------------------------------------')


# HOOK
def before_feature(context, feature: Feature):
    # Inicializa o status no contexto
    context.status = context.status if hasattr(context, 'status') else 'not defined'
    # Execute um método uma única vez para todos os cenários de teste da Feature
    logger.info(f'-------------------------------------------------')
    logger.info(f'vars(feature): {vars(feature)}')
    logger.info(f'-------------------------------------------------')


# HOOK
def before_scenario(context, scenario: Scenario):
    logger.info(f'-------------------------------------------------')
    logger.info(f'vars(scenario): {vars(scenario)}')
    logger.info(f'-------------------------------------------------')
