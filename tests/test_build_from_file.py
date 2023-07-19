from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from t8s.ts import TimeSerie
from t8s.ts_builder import TSBuilder
from t8s.ts_builder import ReadParquetFile

def test_build_from_file():
    path_str: str = 'data/parquet/ts_01.parquet'
    path = Path(path_str)
    print('path: ', path)
    context = TSBuilder(ReadParquetFile())
    print("Client: Strategy is set to read Parquet file.")
    ts: TimeSerie = context.build_from_file(Path(path_str))
    assert int(ts.features) == 3
    assert ts.format == 'wide'
    assert ts.df.__len__() == 4
    # pd._libs.tslibs.timestamps.Timestamp é privado e devo usar pd.Timestamp
    print('type(timestamp)', type(ts.df['timestamp'][0]),
          'type(temperatura)', type(ts.df['temperatura'][0]),
          'type(velocidade)', type(ts.df['velocidade'][0]))
    assert type(ts.df['timestamp'][0]) == pd.Timestamp
    assert type(ts.df['temperatura'][0]) == np.float32
    assert type(ts.df['velocidade'][0]) == np.int32

if __name__ == "__main__":
    test_build_from_file()
