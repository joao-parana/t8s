# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Protocol
import pandas as pd

class TimeSerie(Protocol):
    df: pd.DataFrame
    format: str
    features: str

    def copy(self) -> TimeSerie:
        ...

    def to_wide(self) -> None:
        ...

