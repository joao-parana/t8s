from __future__ import annotations
from datetime import datetime, timezone

import streamlit as st
from streamlit.delta_generator import DeltaGenerator

import altair as alt # type: ignore
import pandas as pd
import numpy as np
from sklearn.base import TransformerMixin   # type: ignore
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler # type: ignore
from t8s.ts import TimeSerie
from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

# Cria uma instância da classe SessionStateProxy
session_state = st.session_state

# Inicializa as variáveis my_count e my_total
if 'counter1' not in session_state: session_state.counter1 = 0
if 'total1' not in session_state: session_state.total1 = 0
if 'counter2' not in session_state: session_state.counter2 = 0
if 'total2' not in session_state: session_state.total2 = 0
if 'counter3' not in session_state: session_state.counter3 = 0
if 'total3' not in session_state: session_state.total3 = 0

plot_cached = 1
st.set_page_config(layout="wide")

# Just add it after st.sidebar:
option_selected: str = ""
# option_selected = st.sidebar.radio('Escolha dentre os 3 exemplos abaixo:',[
option_selected = st.radio('Escolha dentre os 3 exemplos abaixo:',[
    '1 - Imputação de valores ausentes',
    '2 - Analise de uma máquina rotativa',
    '3 - Outro exemplo de gráfico',
    '4 - Exibindo uma série Temporal',
]) or "0 - None"
print(f'option_selected = {option_selected}, type(option_selected) = {type(option_selected)}')

"""
### Visualização gráfica de séries temporais
"""
@st.cache_data(show_spinner="Fetching data from Dataframe...")
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

@st.cache_data(show_spinner="Fetching data from Parquet file...")
def get_dataframe_from_parquet(f) -> pd.DataFrame:
    print(f'Read data and put into cache')
    data = pd.read_parquet(f)
    print(f'data.shape = {data.shape}')
    print('Cache dataframe_from_parquet miss at first run')
    return data

chart_data = create_df_with_nans_and_mask()
other_data = get_dataframe_from_parquet('datasets/machine13_01.parquet')

match option_selected.split(' - ')[0]:
    case "1":
        """
        #### Imputação de valores ausentes

        O gráfico abaixo mostra uma série temporal com valores ausentes. Os valores ausentes
        foram imputados usando o método 'interpolate' com função polinomial de ordem 3, ou
        seja, função cubica.

        Valores em Azul são valores os valores originais. Valores em Vermelho são valores imputados.
        """
        start = datetime.now()
        @st.cache_resource()
        def build_chart_1(cached:int):
            print('Cache chart_1 miss at first run')
            chart_1 = st.line_chart(
                chart_data,
                x = 't',
                y = ['a', 'a_nan'],
                # NaNs em vermelho. O atributo color é opcional
                color = ['#5555FF', '#FF0000'] # type: ignore
            )
            assert isinstance(chart_1, DeltaGenerator)

        build_chart_1(plot_cached)
        elapsed = datetime.now() - start
        print(f'counter1 = {session_state.counter1}, elapsed chart_1: {elapsed}')
        # Incrementa o contador e o totalizador
        session_state.counter1 += 1
        session_state.total1 += elapsed.total_seconds()
        # Forma ta a saida para a página WEB
        std = round(1000 * session_state.total1 / session_state.counter1)
        st.markdown(f"""
        Tempo médio de {std} milissegundos para construir o gráfico após {session_state.counter1} execuções
        """)
    case "2":
        st.markdown("""
        #### Analise de uma máquina rotativa

        O gráfico abaixo mostra duas features de um dataset com dados de dois sensores instalados
        numa máquina rotativa. A feature 'T6021' mede a temperatura do motor num dado ponto. A
        feature 'T6023' mede a temperatura do motor em outro ponto. Observa-se uma anomalia na
        medida no dia 2023-08-04. A anomalia é um outlier, ou seja, um valor muito distante dos
        demais valores da série temporal. Este aquecimento abrupto pode ser um indicativo de
        problemas sistema acoplado ao motor que pode ter causado uma sobrecarga no motor e por
        conseguinte, aumentando a sua temperatura em dois pontos específicos.

        Uma analise criteriosa dos dados pode ajudar a identificar a causa raiz do problema.
        """)
        start = datetime.now()
        @st.cache_resource()
        def build_chart_2(cached:int):
            print('Cache chart_2 miss at first run')
            st._arrow_line_chart(
                other_data,
                x = 'TIMESTAMP',
                y = ['T6021', 'T6023']
            )

        build_chart_2(plot_cached)
        elapsed = datetime.now() - start
        print(f'counter2 = {session_state.counter2}, elapsed chart_2: {elapsed}')
        # Incrementa o contador e o totalizador
        session_state.counter2 += 1
        session_state.total2 += elapsed.total_seconds()
        # Forma ta a saida para a página WEB
        std = round(1000 * session_state.total2 / session_state.counter2)
        st.markdown(f"""
        Tempo médio de {std} milissegundos para construir o gráfico após {session_state.counter2} execuções
        """)
    case "3":
        st.markdown("""
        #### Exemplo de histograma multivariado
        """)
        start = datetime.now()
        @st.cache_resource()
        def build_chart_3(cached:int):
            print('Cache chart_3 miss at first run')
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

        build_chart_3(plot_cached)
        elapsed = datetime.now() - start
        print(f'counter3 = {session_state.counter3}, elapsed chart_3: {elapsed}')
        # Incrementa o contador e o totalizador
        session_state.counter3 += 1
        session_state.total3 += elapsed.total_seconds()
        # Forma ta a saida para a página WEB
        std = round(1000 * session_state.total3 / session_state.counter3)
        st.markdown(f"""
        Tempo médio de {std} milissegundos para construir o gráfico após {session_state.counter3} execuções
        """)
    case "4":
        st.markdown("""
        #### Exibindo os dados de uma série Temporal

        Valores máximos de cada coluna aparecem hachurados em amarelo
        """)
        col1, col2 = st.columns(2)
        df = other_data.style.highlight_max(axis=0)
        with col1:
            st.markdown('###### VIsualize os dados da série temporal')
            st.dataframe(df)
        with col2:
            st.markdown('###### Outras informações')
            x = st.slider('x')  # <- this is a widget
            st.write(x, 'squared is', x * x)
            st.text_input("Entre duas datas separadas por virgula", key="interval")
            # st.write(f'st.session_state.interval = {st.session_state.interval.split(",")}')
            interval = st.session_state.interval.split(",")
            print(f'interval = {interval}')
            data1 = data2 = None
            if len(interval) > 0:
                start = interval[0]
                if not (start == '' or start == None):
                    data1 = datetime.fromisoformat(start).replace(
                        tzinfo=timezone.utc).replace(microsecond=0)
                    if False:
                        data1.replace(hour=0, minute=0, second=0)
            if len(interval) > 1:
                end = interval[1]
                data2 = datetime.fromisoformat(end).replace(
                    tzinfo=timezone.utc).replace(microsecond=0)
                if False:
                    data2.replace(hour=0, minute=0, second=0)
                st.write(f'start = {data1}, end = {data2}')
                st.write(f'tipos de dado: {type(data1)}, {type(data2)}')
