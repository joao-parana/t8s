# -*- coding: utf-8 -*-
# -- FILE: features/steps/example_steps.py

import os
from t8s.log_config import LogConfig
from behave import given, when, then, step
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

LogConfig().initialize_logger(DEBUG)
logger = LogConfig().getLogger()

@given('we have behave installed')
def step_impl(context):
    pass

@when('we implement {number} tests')
def step_impl(context, number):  # -- NOTE: number is converted into integer
    assert int(number) > 1 or int(number) == 0
    context.tests_count = int(number)
    logger.info(f'STEP: WHEN we implement {number} tests')

@then('behave will test them for us!')
def step_impl(context):
    assert context.failed is False
    assert context.tests_count >= 0

# --------------------------------------------------------------------------------

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

# --------------------------------------------------------------------------------

@given(u'a workspace {workspace}')
def step_impl(context, workspace):
    workspace_dir = os.environ.get('WORKSPACE_DIR', '/Volumes/dev/t8s')
    logger.info(f'STEP: Given a workspace ${workspace} ({workspace_dir})')
    context_workspace = os.environ.get(workspace)
    logger.info(f'STEP: Given a workspace -> context.workspace {context_workspace}')
    context.workspace = context_workspace

@given(u'a number {left}')
def step_impl(context, left):
    logger.info(f'STEP: Given a number {left}, context.workspace = {context.workspace}')
    context.left = left

@when(u'add a number {right}')
def step_impl(context, right):
    logger.info(f'STEP: When add a number {right} value')
    context.right = right

@then(u'the sum is {result}')
def step_impl(context, result):
    logger.info(f'STEP: Then the sum is {result}')
    # for idx in range(len(context.left)):
    assert int(result) == (int(context.left) + int(context.right))
