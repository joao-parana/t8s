# -*- coding: utf-8 -*-
# -- FILE: features/steps/example_steps.py

import os
from t8s.log_config import LogConfig
from behave import given, when, then, model, step
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

LogConfig().initialize_logger(DEBUG)
logger = LogConfig().getLogger()

@given('we have behave installed')
def check_behave_istalled(context):
    pass

@when('we implement {number} tests')
def check_number(context, number):  # -- NOTE: number is converted into integer
    assert int(number) > 1 or int(number) == 0
    context.tests_count = int(number)
    logger.info(f'STEP: WHEN we implement {number} tests')

@then('behave will test them for us!')
def test(context):
    assert context.failed is False
    assert context.tests_count >= 0

# --------------------------------------------------------------------------------

@given(u'que temos o behave instalado')
def check_behave_istalled_again(context):
    logger.info('STEP: Given que temos o behave instalado')
    logger.info('context' + str(context) + str(type(context)))


@when(u'implementamos {qty} testes')
def check_qty(context, qty):
    logger.info(f'STEP: When implementamos {qty} testes')
    logger.info('context' + str(context) + str(type(context)))

@then(u'o behave vai testar pra gente!')
def teste(context):
    logger.info('STEP: Then o behave vai testar pra gente!')
    logger.info('context' + str(context) + str(type(context)))

# --------------------------------------------------------------------------------

@given(u'a workspace {workspace}')
def set_workspace(context, workspace):
    workspace_dir = os.environ.get('WORKSPACE_DIR', '/Volumes/dev/t8s')
    logger.info(f'STEP: Given a workspace ${workspace} ({workspace_dir})')
    context_workspace = os.environ.get(workspace)
    logger.info(f'STEP: Given a workspace -> context.workspace {context_workspace}')
    context.workspace = context_workspace

@given(u'a valid user logged in')
def select_user(context):
    user = 'fulano'
    logger.info(f'STEP: Given a valid user logged in ({user})')
    context.user = 'fulano'

@given(u'a number {left}')
def a_number(context, left):
    logger.info(f'STEP: Given a number {left}, context.workspace = {context.workspace}')
    context.left = left

@when(u'add a number {right}')
def another_number(context, right):
    logger.info(f'STEP: When add a number {right} value')
    context.right = right

@then(u'the sum is {result}')
def sum(context, result):
    logger.info(f'STEP: Then the sum is {result}')
    # for idx in range(len(context.left)):
    assert int(result) == (int(context.left) + int(context.right))

@given(u'a simple silly step')
def a_silly_step(context):
    logger.info(u'STEP: Given a simple silly step')

@then(u'the last step has a final table')
def final_table(context):
    assert isinstance(context.table, model.Table)
    table: model.Table = context.table
    headings: list[str] = table.headings
    rows: list[list[str]] = [ list(row) for row in table.rows ]
    logger.info(f'STEP: Then the last step has a final table: {table}, {type(table)}')
    logger.info(f'STEP: Then the last step has a final table -> headings: {headings}, {type(headings)}')
    logger.info(f'STEP: Then the last step has a final table -> rows:{rows}, {type(rows)}')
    for idx, row in enumerate(table.rows):
        actual: model.Row = row
        v1 = actual.get(headings[0])
        v2 = actual.get(headings[1])
        v3 = actual.get(headings[2])
        logger.info(f'STEP: Then the last step has a final table -> ' +
            f'table.rows[{idx}]: [ {v1}, {v2}, {v3} ], {type(v1)}, {type(v2)}, {type(v3)}')
