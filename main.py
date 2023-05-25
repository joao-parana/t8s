from t8s.log_config import LogConfig
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import t8s
from t8s.ts import TimeSerie
from t8s.ts_builder import TSBuilder, ReadParquetFile, ReadCsvFile
from t8s.log_config import LogConfig
from t8s.ts_writer import TSWriter, WriteParquetFile, WriteCsvFile
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

if __name__ == "__main__":
    # print('globals:', globals())
    # The client code.
    LogConfig().initialize_logger(INFO)
    logger = LogConfig().getLogger()
    ts = TSBuilder.empty()
    # initialize_logger(INFO)
    logger.info('t8s package version:' + t8s.__version__)
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade
    data = {
        'timestamp': [
            datetime(2022, 1, 1, 0, 0, 0), 
            datetime(2022, 1, 1, 1, 0, 0),
            datetime(2022, 1, 1, 2, 0, 0), 
            datetime(2022, 1, 1, 3, 0, 0)],
        'temperatura': np.array([25.0, 26.0, 27.0, 23.2], dtype=np.float32),
        'velocidade': [2000, 1100, 1200, 4000]
    }
    # Convertendo os tipos de dado para temperatura e velocidade para 
    # np.float32 e np.int32 respectivamente, pois o padrão é np.float64 e np.int64
    data['temperatura'] = np.array(data['temperatura'], dtype=np.float32)
    data['velocidade'] = np.array(data['velocidade'], dtype=np.int32)
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e
    # velocidade para o proposito de teste
    ts = TimeSerie(data, format='wide', features_qty = 3)
    cols_str = [name for name in sorted(ts.df.columns)]
    cols_str = ', '.join(cols_str)
    logger.info(f'Dataframe com {len(ts.df.columns)} colunas: {cols_str}')
    # Faz o display da série temporal no terminal
    print(ts)
    # Grava a série temporal em parquet
    print(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path}')
    context = TSWriter(WriteParquetFile())
    print("Client: Strategy was seted to write Parquet file.")
    context.write(Path(path_str), ts)
    print(f'\nArquivo {str(path)} gerado à partir da TimeSerie fornecida')
    # --------------------------------------------------------------------------------
    # Limpando objeto ts para garantir que será lido corretamente.
    ts: TimeSerie = TSBuilder.empty()
    # Lê a série temporal gravada no arquivo parquet
    print(f'\nLendo path {str(path)} e gerando TimeSerie')
    
    # The client code picks a concrete strategy and passes it to the context.
    # The client should be aware of the differences between strategies in order
    # to make the right choice.
    assert isinstance(path, Path), "path must be a Path object"
    if  (str(path)).endswith('.parquet'):
        context = TSBuilder(ReadParquetFile())
        print("Client: ReadStrategy is set to read Parquet file.")
        ts = context.build_from_file(Path(path_str))
    else:
        assert str(path).endswith('.csv'), "If path is not a Parquet file the path must be a CSV file"
        print("Client: ReadStrategy is set to read CSV file.")
        context = TSBuilder(ReadCsvFile())
        ts = context.build_from_file(Path(path_str))

    assert int(ts.features) == 3
    assert ts.format == 'wide'
    assert len(ts.df) == 4
    print('Tipos das colunas no Dataframe da Série Temporal:')
    for col in ts.df.columns:
        print(col, '\t', type(ts.df[col][0]))

    # pd._libs.tslibs.timestamps.Timestamp é privado e devo usar pd.Timestamp
    assert type(ts.df['timestamp'][0]) == pd.Timestamp
    assert type(ts.df['temperatura'][0]) == np.float32
    assert type(ts.df['velocidade'][0]) == np.int32

    univariate_list = ts.split()
    for idx, ts_uni in enumerate(univariate_list):
        print(f'TimeSerie univariada {idx}:')
        print(ts_uni)
        print('---------------------------------------------------')
        print(f'univariate {idx-1}', '\n ', ts_uni)

    print('---------------------------------------------------')
    