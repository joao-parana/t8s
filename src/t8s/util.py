import os
from pathlib import Path
from datetime import datetime
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile
from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

class Util:
    @staticmethod
    def to_parquet(ts: TimeSerie, path_ts: Path):
        # def write_ts_to_parquet_file(ts, parquet_path, filename: str):
        logger.debug(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path_ts}')
        context = TSWriter(WriteParquetFile())
        logger.debug("Client: Strategy was seted to write Parquet file.")
        context.write(Path(path_ts), ts)
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