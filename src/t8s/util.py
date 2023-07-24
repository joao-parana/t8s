from pathlib import Path
from datetime import datetime
from t8s.ts import TimeSerie
from t8s.ts_writer import TSWriter, WriteParquetFile

class Util:
    @staticmethod
    def to_parquet(ts: TimeSerie, path_ts: Path):
        # def write_ts_to_parquet_file(ts, parquet_path, filename: str):
        print(f'Grava a série temporal (formato {ts.format}) em um arquivo parquet {path_ts}')
        context = TSWriter(WriteParquetFile())
        print("Client: Strategy was seted to write Parquet file.")
        context.write(Path(path_ts), ts)
        print(f'\nArquivo {str(path_ts)} gerado à partir da TimeSerie fornecida')
