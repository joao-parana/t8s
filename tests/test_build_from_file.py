import yaml
from pathlib import Path
from datetime import datetime
from t8s import TimeSerie

def test_build_from_file():
    path_str: str = 'ts_01.parquet'
    path = Path(path_str)
    print('path: ', path)
    ts = TimeSerie.build_from_file(path) 
    assert int(ts.features) == 3
    assert ts.format == 'wide'
    assert ts.df.__len__() == 3
 