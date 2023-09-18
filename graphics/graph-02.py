from __future__ import annotations

import altair as alt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from t8s.log_config import LogConfig
from t8s.ts import TimeSerie

logger = LogConfig().get_logger()

st.set_page_config(layout="wide")

"""
### Visualização gráfica de séries temporais
"""

f = 'datasets/mach13.parquet'
df = pd.read_parquet(f)
df['TEMPO'] = pd.to_datetime(df['TEMPO'])
# Criando uma TimeSerie multivariada a partir de um DataFrame
ts = TimeSerie(df, format='wide', features_qty=len(df.columns))
subserie = ts.df[175000:-33000]

# Extraindo uma TimeSerie univariada a partir da TimeSerie multivariada
ts_list = ts.split()
ts1 = ts_list[0]
# Adicionando a coluna com as correções dos valores ausentes. Esta coluna é nomeada
# adicionando o sufixo '_nan' ao nome da coluna original.
ts1 = ts1.add_nan_mask(inplace=False, plot=False, method='interpolate')
chart_data_1 = ts1.df[165000:]
print(chart_data_1)
"""
#### Imputação de valores ausentes

O gráfico abaixo mostra uma série temporal com valores ausentes. Os valores ausentes
foram imputados usando o método 'interpolate' com função polinomial de ordem 3, ou
seja, funçãao cubica.

Valores em Azul são valores os valores originais. Valores em Vermelho são valores imputados.
"""
st._arrow_line_chart(
    chart_data_1,
    x='TEMPO',
    y=[
        'T6421',
        'T6421_nan',
    ],  # Esta coluna é nomeada adicionando o sufixo '_nan' ao nome da coluna original.
    color=['#5555FF', '#FF0000'],  # NaNs em vermelho. O atributo color é opcional
)

"""
##### Imputação de valores ausentes (Zoom)
"""
st._arrow_line_chart(
    chart_data_1[10000:-33000],  # Seleciona um conjunto de dados para exibir
    x='TEMPO',
    y=['T6421', 'T6421_nan'],
    color=['#5555FF', '#FF0000'],  # NaNs em vermelho. O atributo color é opcional
    use_container_width=True,
)

"""
#### Analise de uma máquina rotativa

O gráfico abaixo mostra os dados originais de duas features de um dataset com dados de dois sensores.
"""
st._arrow_line_chart(subserie, x='TEMPO', y=['T6421', 'T6422'])


"""
#### Histograma sobreposto de duas features de um dataset
"""
chart = (
    alt.Chart(subserie)
    .transform_fold(['T6421', 'T6422'], as_=['Sensor', 'Measurement'])
    .mark_bar(opacity=0.3, binSpacing=0)
    .encode(
        alt.X('Measurement:Q').bin(maxbins=100),
        alt.Y('count()').stack(None),
        alt.Color('Sensor:N'),
    )
)

st._arrow_altair_chart(chart, use_container_width=True)

chart_data_2 = ts.df[175000:-33000]

"""
##### Visualização gráfica usando a biblioteca Matplotlib
"""
ax: plt.Axes
fig: plt.Figure
fig, ax = plt.subplots(figsize=(12, 5))  # type: ignore
ax.plot(chart_data_2['TEMPO'], chart_data_2[['T6421', 'T6422']])
ax.set_title('Subsérie de uma máquina rotativa')
ax.set_ylim(ymin=80, ymax=110)
st.pyplot(fig)

"""
##### Visualização gráfica usando a biblioteca Matplotlib
"""
chart_data_3 = ts1.df[175000:-33000]

fig, ax = plt.subplots(figsize=(12, 5))  # type: ignore
ax.plot(chart_data_3['TEMPO'], chart_data_3[['T6421', 'T6421_nan']])
ax.set_title('Subsérie de uma máquina rotativa')
ax.set_ylim(ymin=80, ymax=110)
st.pyplot(fig)
