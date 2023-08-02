import os
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from pandas import Series
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile
from t8s.log_config import LogConfig
from t8s.ts_builder import TSBuilder, ReadParquetFile # , ReadCsvFile

# to consider inf and -inf to be “NA” in computations, you can set:
pd.options.mode.use_inf_as_na = True

logger = LogConfig().getLogger()

class Util:
    @staticmethod
    def to_parquet(ts: TimeSerie, path_ts: Path):
        # def write_ts_to_parquet_file(ts, parquet_path, filename: str):
        logger.debug(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path_ts}')
        ctx = TSWriter(WriteParquetFile())
        logger.debug("Client: Strategy was seted to write Parquet file.")
        ctx.write(Path(path_ts), ts)
        logger.debug(f'\nArquivo {str(path_ts)} gerado à partir da TimeSerie fornecida')

    @staticmethod
    def list_all_files(path: Path) -> list:
        assert path.exists() and path.is_dir()
        result = []
        for root, dirs, files in os.walk(path):
            for file in files:
                # logger.debug(os.path.join(root, file))
                result.append(os.path.join(root, file))
            for dir in dirs:
                # logger.debug(os.path.join(root, dir))
                result.append(os.path.join(root, dir))
        for item in result:
            logger.debug(item)

        return result

    @staticmethod
    def build_time_serie_from_parquet(workspace_dir: Path, filename: str) -> TimeSerie:
        DATA_PATH = os.path.join(workspace_dir, 'data')
        PARQUET_DATA_PATH = os.path.join(DATA_PATH, 'parquet')
        parquet_input_file = os.path.join(PARQUET_DATA_PATH, filename)
        logger.debug(f'parquet_input_file: {parquet_input_file}, {type(parquet_input_file)}')
        path = Path(parquet_input_file)
        logger.debug(f'{path},  {type(path)}')
        ts = TimeSerie.empty()
        assert isinstance(path, Path), "path must be a Path object"
        if (str(path)).endswith('.parquet'):
            context = TSBuilder(ReadParquetFile())
            logger.debug("Client: ReadStrategy is set to read Parquet file.")
            ts = context.build_from_file(path)
        return ts

    # Função para obter a quantidade de valores nulos e não nulos consecutivos
    #
    # O resultado é um Dataframe com duas colunas, a primeira com os valores True e False, indicando
    # se o valor é nulo ou não, e a segunda com a quantidade de valores consecutivos iguais.
    # Valor: True se o valor é nulo, False se não é nulo
    @staticmethod
    def get_null_and_notnull_consecutive_counts(df_to_process: pd.DataFrame, col_name:str) -> pd.DataFrame:
        df_ret = df_to_process[col_name].isna()
        # print('df_ret:\n',  df_ret)
        # Obtendo os grupos consecutivos de valores iguais
        groups = df_ret.ne(df_ret.shift()).cumsum()
        inicio = np.where(groups.ne(groups.shift()))
        # Contando a quantidade de elementos em cada grupo
        counts = groups.value_counts()
        # Obtendo os valores dos grupos
        group_values = df_ret.groupby(groups).first()
        # Juntando as informações em um único DataFrame
        result = pd.DataFrame({'Value': group_values, 'Counts': counts,'Start': inicio[0]})
        result.reset_index(drop=True, inplace=True)
        return result

    @staticmethod
    def get_numeric_column_names(df) -> list:
        ret:list = []
        for idx, c in enumerate(df.columns):
            if (df[c].dtype == float or df[c].dtype == int or
            df[c].dtype == np.float64 or df[c].dtype == np.int64 or
            df[c].dtype == np.float32 or df[c].dtype == np.int32):
                ret.append(c)
        ret.sort()
        return ret

    @staticmethod
    def identify_start_and_end_of_nan_block(df, col) -> pd.DataFrame | None:
        logger.debug(f'\ncol: {col}, \ndf:\n{df[col]} \ntype: {type(df[col])}')
        # O schema da saida é: {'Value': [bool array], 'Counts': [int array], 'Start': [int array]}
        logger.debug(f'{df[col].head(1)}, {df[col].head(1)}, {type(df[col].head(1))}')
        x: Series = df[col]
        # if df[col].head(1).isnull().values.any():
        if np.isnan(df[col].iloc[0]):
            logger.error('OperationNotSupported: Primeiro elemento da série é NaN')
            return None
        else:
            logger.debug('Primeiro elemento da série não é NaN')
            null_and_notnull_counts = Util.get_null_and_notnull_consecutive_counts(df, col)
            return null_and_notnull_counts

    @staticmethod
    def identify_all_start_and_end_of_nan_block(df: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], int]:
        last_idx = 0
        ret_all_s_e_nan_block = {}
        cols = Util.get_numeric_column_names(df)
        for idx, col in enumerate(cols):
            key = f'{idx}-{col}'
            l = Util.identify_start_and_end_of_nan_block(df, col)
            ret_all_s_e_nan_block.update({key: l})
            last_idx = idx

        return (ret_all_s_e_nan_block, last_idx)
