from t8s.log_config import LogConfig
from pathlib import Path
from datetime import datetime
import t8s
from t8s import TimeSerie, TSBuilder, ReadParquetFile, ReadCsvFile

from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL

if __name__ == "__main__":
    # print('globals:', globals())
    # The client code.
    LogConfig().initialize_logger(INFO)
    logger = LogConfig().getLogger()

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
        'temperatura': [25.0, 26.0, 27.0, 23.2],
        'velocidade': [2000.1, 1100.3, 1200.5, 4000.4]
    }

    ts = TimeSerie(data, format='wide', features_qty=len(data))
    df = ts.df
    cols_str = [name for name in sorted(df.columns)]
    cols_str = ', '.join(cols_str)
    logger.info(f'Dataframe com {len(df.columns)} colunas: {cols_str}')
    # Imprime a série temporal
    print(ts)
    # Imprime a série temporal em parquet
    print(f'Converte a série temporal (formato {ts.format}) em arquivo parquet {path}')
    ts.to_parquet(Path(path_str))
    # Lê a série temporal gravada no arquivo parquet
    print('\nLendo path: ', path)
    # ts = TimeSerie.build_from_file(path) 
    
    # --------------------------------------------------------------------------------
    # The client code picks a concrete strategy and passes it to the context.
    # The client should be aware of the differences between strategies in order
    # to make the right choice.

    ts = TimeSerie(format = 'wide', features_qty = 0)
    assert isinstance(path, Path), "path must be a Path object"
    if  (str(path)).endswith('.parquet'):
        context = TSBuilder(ReadParquetFile())
        print("Client: Strategy is set to read Parquet file.")
        ts = context.build_from_file(Path(path_str))
    else:
        assert str(path).endswith('.csv'), "If path is not a Parquet file the path must be a CSV file"
        print("Client: Strategy is set to read CSV file.")
        context = TSBuilder(ReadCsvFile())
        ts = context.build_from_file(Path(path_str))

    assert int(ts.features) == 3
    assert ts.format == 'wide'
    assert ts.df.__len__() == 4