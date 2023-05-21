import yaml
from pathlib import Path
from datetime import datetime
from t8s import TimeSerie

def test_to_parquet():
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    data = {
        'timestamp': [datetime(2022, 1, 1, 0, 0, 0), datetime(2022, 1, 1, 1, 0, 0), datetime(2022, 1, 1, 2, 0, 0)],
        'temperatura': [25.0, 26.0, 27.0],
        'velocidade': [2000, 1100, 1200]
    }

    ts = TimeSerie(data, format='wide', features_qty=len(data))
    df = ts.df
    cols_str = [name for name in sorted(df.columns)]
    print('cols_str :', ', '.join(cols_str))
    print(f'Dataframe com {len(df.columns)} colunas: {df.columns}')
    # Imprime a série temporal
    print(ts)
    # Imprime a série temporal em parquet
    print(f'Converte a série temporal (formato {ts.format}) em arquivo parquet {path}')
    ts.to_parquet(Path(path_str))
