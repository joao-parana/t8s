import types
import yaml
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from t8s import get_sample_df
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile
import pyarrow.parquet as pq         # type: ignore


from t8s.log_config import LogConfig
from logging import INFO, DEBUG, WARNING, ERROR, CRITICAL
logger = LogConfig().getLogger()

def check_schema(ts, parquet_file: Path, types: list[type]):
    """
    Verifica o schema do arquivo parquet gerado e os tipos dos dados de cada coluna
    contra os tipos esperados encontrados no Dataframe que é parte da TimeSerie.
    """
    print(f'Verificando os tipos dos dados gravados no arquivo parquet {parquet_file} contra os tipos esperados:', types)
    # Lê os metadados do arquivo Parquet
    metadata: pq.FileMetaData = pq.read_metadata(parquet_file)
    logger.debug('\nParquet file metadata:\n' + str(metadata.to_dict()) + '\n' + str(metadata.metadata))
    assert isinstance(metadata, pq.FileMetaData), "metadata must be a pq.FileMetaData object"
    dict_meta: dict = metadata.to_dict()
    logger.debug('\n-------------------------------')
    logger.debug('created_by: ' + str(dict_meta['created_by']))
    # Imprime o valor do metadado 'format'
    format = metadata.metadata[b'format'].decode()
    features = metadata.metadata[b'features'].decode()
    logger.info('format: ' + format + ' type(format) ' + str(type(format)))
    logger.info('features: ' + str(features) + ' type(features): ' + str(type(features)))
    assert isinstance(format, str), "format metadada must be a string"
    assert isinstance(features, str), "features metadada must be a string"
    features_qty = int(features)
    assert features_qty > 1, "features_qty must be greater than one"
    # print('format', dict_meta['format'])
    # print('features', dict_meta['features'])
    # Imprime o esquema do arquivo Parquet
    logger.debug('\ntype(metadata.schema): ' + str(type(metadata.schema)) + '\t' + str(metadata.schema))
    # Imprime as colunas do arquivo Parquet
    # print('metadata.column_names', metadata.column_names)
    # Imprime as estatísticas do arquivo Parquet
    physical_types = []
    if metadata.num_row_groups > 0:
        for idx in range(metadata.num_columns):
            stats = metadata.row_group(0).column(idx).statistics
            logger.debug(stats)
            logger.debug('-----------------------------')
            logger.info('physical_type = ' + stats.physical_type)
            physical_types.append(stats.physical_type)
    # Exemplo: INT64, FLOAT, INT32 -> int64(datetime64[ns]), float32, int32
    print('Parquet physical_types:', ', '.join(physical_types))
    print(ts.df.info())
    print('TimeSerie types expected:', ', '.join([str(t) for t in types]))
    pass

def test_to_parquet():
    TimeSerie.empty()
    # Cria uma série temporal multivariada com três atributos: timestamp, temperatura e velocidade

    number_of_records = 4
    time_interval = 1 # hour
    df, last_ts = get_sample_df(number_of_records, datetime(2022, 1, 1, 0, 0, 0), time_interval)
    columns_types = [type(df[col][0]) for col in df.columns]
    print('columns_types =', columns_types)
    # data = df.to_dict()
    ts = TimeSerie(df, format='wide', features_qty=len(df.columns))
    cols_str = [name for name in sorted(df.columns)]
    print('cols_str :', ', '.join(cols_str))
    print(f'Dataframe com {len(df.columns)} colunas: {df.columns}')
    # Imprime a série temporal
    print(ts)
    # Grava a série temporal em parquet
    path_str: str = 'data/parquet/ts_01.parquet'
    path = Path(path_str)
    print(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path}')
    context = TSWriter(WriteParquetFile())
    print("Client: Strategy was seted to write Parquet file.")
    context.write(Path(path_str), ts)
    print(f'\nArquivo {str(path)} gerado à partir da TimeSerie fornecida')
    check_schema(ts, path, [datetime, np.float32, np.int32])
    logger.info('FIM')

if __name__ == "__main__":
    LogConfig().initialize_logger(DEBUG, log_file = 'tests/to_parquet.log')
    test_to_parquet()
