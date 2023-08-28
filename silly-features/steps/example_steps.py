# -*- coding: utf-8 -*-
# -- FILE: features/steps/example_steps.py

import os
import json
import pandas as pd
import matplotlib.pyplot as plt

from t8s.log_config import LogConfig
from behave import given, when, then, model, step # type: ignore
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

@given('an attribute created in context')
def step_impl_1(context):
    context.meu_atributo = 'valor'
    def minha_funcao_1(x):
        return x**2

    context.minha_funcao_1 = minha_funcao_1
    print(f'\ntype(context.minha_funcao_1) = {type(context.minha_funcao_1)}\n')
    context.minha_funcao_2 = lambda x: x + 1
    print(f'\ntype(context.minha_funcao_2) = {type(context.minha_funcao_2)}\n')

@then('I can list the context attributes in another step and check the created attribute')
def step_impl_2(context):
    print(vars(context.scenario))
    # json_str = json.dumps(vars(context.scenario), indent=2)
    # print(json_str)
    assert context.meu_atributo == 'valor'
    assert callable(context.minha_funcao_1), "A função minha_funcao_1 não foi criada corretamente"
    assert isinstance(context.minha_funcao_2, type(lambda x: x)), 'context.minha_funcao is not a function'
    assert callable(context.minha_funcao_2), "A função minha_funcao_2 não foi criada corretamente"

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
    workspace_dir = os.environ.get('T8S_WORKSPACE_DIR', '/Volumes/dev/t8s')
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


# --------------------------------------------------------------------------------

"""
Scenario: Plotting a time series using Pandas Dataframe
"""

@given(u'a dataset in the Parquet file representing a time series')
def read_machine_13(context):
    logger.info(u'STEP: Given a dataset in the Parquet file representing a time series')
    f = 'datasets/machine13_01.parquet'
    df = pd.read_parquet(f)
    context.df = df

@then(u'I plot 4 features selected for exploratory analysis')
def step_impl(context):
    logger.info(u'STEP: Then I plot 4 features selected for exploratory analysis')
    do_plot(context.df)

def do_plot(df: pd.DataFrame):

    def plot_synthetic_data():
        # Criando um DataFrame simples de exemplo para experimentar com
        # gráficos de duas escalas, sendo uma na esquerda (colunas: a, b) e outra
        # na direita (colunas: c, d)
        df = pd.DataFrame({
            't': pd.date_range('2022-01-01', periods=10, freq='D'),
            'a': [8, 6, 2, 3, 10, 5, 7, 8, 9, 4],
            'b': [20, 14, 18, 8, 4, 6, 16, 10, 12, 20],
            'c': [180, 60, 90, 120, 150, 30, 270, 210, 240, 300],
            'd': [140, 200, 80, 120, 160, 40, 320, 400, 280, 360]
        })

        # Configurando o índice do DataFrame como a coluna 't'
        df.set_index('t', inplace=True)

        # Criando o gráfico de linhas com duas escalas y
        fig, ax1 = plt.subplots()

        # Plotando as colunas 'a' e 'b' no eixo y esquerdo
        ax1.plot(df.index, df['a'], color='red')
        ax1.plot(df.index, df['b'], color='blue')
        ax1.set_ylabel('Escala esquerda')

        # Criando um segundo eixo y para as colunas 'c' e 'd'
        ax2 = ax1.twinx()

        # Plotando as colunas 'c' e 'd' no eixo y direito
        ax2.plot(df.index, df['c'], color='green')
        ax2.plot(df.index, df['d'], color='orange')
        ax2.set_ylabel('Escala direita')

        # Exibindo o gráfico
        plt.show()

    plot_synthetic_data()

    logger.info(f'df.columns = {df.columns}')
    logger.info(df.info())

    # Criando o gráfico de linhas
    df.plot(x='TIMESTAMP', y=['T6021', 'T6022', 'T6023', 'T6024'], kind='line')
    plt.show()

    # Criando o gráfico de linhas com duas escalas em y
    fig, ax1 = plt.subplots(figsize=(12, 4)) # 12 é largura e 4 é altura do canvas
    # Plotando as colunas 'a' e 'b' no eixo y esquerdo
    ax1.plot(df.index, df['T6021'], color='red')
    ax1.plot(df.index, df['T6023'], color='blue')
    ax1.set_ylabel('T6021 (vermelho) e T6023 (azul)')
    # Criando um segundo eixo y para as colunas 'T6022' e 'T6024'
    ax2 = ax1.twinx()
    # Plotando as colunas 'T6021' e 'T6023' no eixo y direito
    ax2.plot(df.index, df['T6022'], color='green')
    ax2.plot(df.index, df['T6024'], color='orange')
    ax2.set_ylabel('T6022 (verde) e T6024 (laranja)')
    plt.show()
