# -*- coding: utf-8 -*-
# -- FILE: features/steps/example_steps.py

from t8s.log_config import LogConfig
from behave import given, when, then, step
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

LogConfig().initialize_logger(DEBUG)
logger = LogConfig().getLogger()

@given('we have behave installed')
def step_impl(context):
    pass

@when('we implement {number:d} tests')
def step_impl(context, number):  # -- NOTE: number is converted into integer
    assert number > 1 or number == 0
    context.tests_count = number

@then('behave will test them for us!')
def step_impl(context):
    # assert context.failed is False
    assert context.tests_count >= 0

@given(u'que temos o behave instalado')
def step_impl(context):
    logger.info('STEP: Given que temos o behave instalado')
    logger.info('context' + str(context) + str(type(context)))


@when(u'implementamos {qty} testes')
def step_impl(context, qty):
    logger.info(f'STEP: When implementamos {qty} testes')
    logger.info('context' + str(context) + str(type(context)))


@then(u'o behave vai testar pra gente!')
def step_impl(context):
    logger.info('STEP: Then o behave vai testar pra gente!')
    logger.info('context' + str(context) + str(type(context)))
