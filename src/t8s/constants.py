"""Constants for use with tests. Don't move this file elsewhere"""
from __future__ import annotations

import pathlib

CONSTANTS_DIR = pathlib.Path(__file__).parent
if not str(CONSTANTS_DIR).endswith('t8s/src/t8s'):
    raise ValueError("This file must be in a directory named 'src/t8s'")
# caminho absoluto para o diretório do projeto
PROJECT_ROOT = CONSTANTS_DIR.parent.parent
# caminho absoluto para o diretório data
TEST_DATA = PROJECT_ROOT / "data"
TEST_ROOT = PROJECT_ROOT / "tests"
