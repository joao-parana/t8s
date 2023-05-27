# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import datetime
import yaml
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

import re
from enum import Enum

null_path: Path = Path('/NullPathObject')

class IO:
    # A linguagem Python não um meio de definir um método privado, mas o uso de dois underscore
    # no inicio do nome do método é uma convenção que indica que o método é privado e que estamos
    # usando a técnica name mangling para impedir a visibilidade padrão. Portanto o método 
    # __check_csv_schema não deve ser chamado diretamente de fora da classe.
    @staticmethod
    def __check_csv_schema(csv_file_list: list[Path]):
        print('\nAtenção: Links simbólicos serão resolvidos, caso existam.')
        for f in csv_file_list:
            print('', f.resolve())
            
        csv_file_schema = ''
        print('csv_file_list:', csv_file_list)
        first_lines = []
        for f in csv_file_list:
            with open(f) as f:
                first_line = f.readline()
                first_lines.append(first_line)

        # print('first_lines:', first_lines)
        for i, line in enumerate(first_lines):
            if line != first_lines[0]:
                msg = f"Schema do arquivo {csv_file_list[i]} não é igual " + \
                    "ao do arquivo {csv_file_list[0]} que é {first_lines[0]}"
                raise ValueError(msg)
        
        if len(first_lines) > 0:
            csv_file_schema = first_lines[0]
            logger.debug('schema for CSV file: %s', csv_file_schema)

    # Quando directory, que é um Path, é informado ordenamos a lista de nomes
    # de arquivos CSV por convenção. O método retorna um Boolean que indica se
    # o schema de todos os arquivos CSV é o mesmo.
    @staticmethod
    def check_schema_in_csv_files(directory: Path = null_path, csv_files: list[Path] = []) -> bool:
        result = False
        csv_file_list = []
        if directory == null_path and len(csv_files) == 0:
            msg = 'Você deve fornecer um diretório ou uma lista de arquivos'
            raise ValueError('Parâmetros inválidos')
        elif directory != null_path and len(csv_files) > 0:
            msg = 'Você deve escolher entre  fornecer um diretório ou ' + \
                'uma lista de arquivos, mas não os dois !'
            raise ValueError('Parâmetros ambíguos. ' + msg)
        elif len(csv_files) > 0:
            csv_file_list = csv_files
        else:
            # Quando directory, que é um Path, é informado ordenamos a 
            # lista de nomes de arquivos CSV por convenção
            csv_file_list = list(directory.glob('*.csv'))
            csv_file_list.sort()

        if len(csv_file_list) == 0:
            result = False
            raise ValueError(f"Não existem aquivos CSV no diretório")

        IO.__check_csv_schema(csv_file_list)
        result = True
        
        return result
