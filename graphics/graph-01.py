from __future__ import annotations
import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
from sklearn.base import TransformerMixin   # type: ignore
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler # type: ignore
from t8s.ts import TimeSerie
from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

st.set_page_config(layout="wide")

"""
### Visualização gráfica de séries temporais
"""
def create_df_with_nans_and_mask() -> pd.DataFrame:
    # Criando um DataFrame de exemplo
    values_a = [8.0, 5.0, 4.0, 3.0, 6.0, 18.0, np.nan, np.nan, np.nan, 14.0, 4.0, 4.0, 8.0, 20.0,
                7.0, 10.0, np.nan, np.nan, np.nan, np.nan, np.nan, 16.0, 8.0, 12.0, np.nan, 9.0,
                10.0, 4.0, 6.0, 5.0, np.nan, 6.0, 17.0, 4.0, np.nan, np.nan, 9.0, 4.0, 5.0, 7.0]
    p = len(values_a)
    df = pd.DataFrame({
        't': pd.date_range('2023-07-26', periods=p, freq='D'),
        'a': values_a
    })
    ts = TimeSerie(df, format='wide', features_qty=len(df.columns))
    ts1 = ts.add_nan_mask(inplace=False, plot=False, method='interpolate')
    return ts1.df

chart_data = create_df_with_nans_and_mask()

"""
#### Imputação de valores ausentes

O gráfico abaixo mostra uma série temporal com valores ausentes. Os valores ausentes
foram imputados usando o método 'interpolate' com função polinomial de ordem 3, ou
seja, função cubica.

Valores em Azul são valores os valores originais. Valores em Vermelho são valores imputados.
"""
st._arrow_line_chart(
    chart_data,
    x = 't',
    y = ['a', 'a_nan'],
    color = ['#5555FF', '#FF0000']  # NaNs em vermelho. O atributo color é opcional
)

"""
#### Analise de uma máquina rotativa

O gráfico abaixo mostra duas features de um dataset com dados de dois sensores instalados
numa máquina rotativa. A feature 'T6021' mede a temperatura do motor num dado ponto. A
feature 'T6023' mede a temperatura do motor em outro ponto. Observa-se uma anomalia na
medida no dia 2023-08-04. A anomalia é um outlier, ou seja, um valor muito distante dos
demais valores da série temporal. Este aquecimento abrupto pode ser um indicativo de
problemas sistema acoplado ao motor que pode ter causado uma sobrecarga no motor e por
conseguinte, aumentando a sua temperatura em dois pontos específicos.

Uma analise criteriosa dos dados pode ajudar a identificar a causa raiz do problema.
"""
f = 'datasets/machine13_01.parquet'
other_data = pd.read_parquet(f)
st._arrow_line_chart(
    other_data,
    x = 'TIMESTAMP',
    y = ['T6021', 'T6023']
)

chart = alt.Chart(other_data).transform_fold(
    ['T6021', 'T6023'],
    as_=['Sensor', 'Measurement']
).mark_bar(
    opacity=0.3,
    binSpacing=0
).encode(
    alt.X('Measurement:Q').bin(maxbins=100),
    alt.Y('count()').stack(None),
    alt.Color('Sensor:N')
)

st._arrow_altair_chart(chart, use_container_width=True)
