# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import datetime
import csv
import os
import yaml
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

import re
from enum import Enum
from t8s import get_numeric_regex

null_path: Path = Path('/NullPathObject')


class IO:
    # A linguagem Python não tem um meio de definir um método privado, mas o uso de dois underscore
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
                msg = (
                    f"Schema do arquivo {csv_file_list[i]} não é igual "
                    + "ao do arquivo {csv_file_list[0]} que é {first_lines[0]}"
                )
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
            msg = 'Você deve escolher entre  fornecer um diretório ou ' + 'uma lista de arquivos, mas não os dois !'
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

    # Quando estamos lendo de arquivos CSV podem aparecer valores que não tem uma
    # representação valida para NaN e portanto atrapalha o processo de inferência
    # de tipo do Pandas. Assim tokens como  'Configure', 'Bad' e 'NotNamber',
    # por exemplo, devem ser substituidos por NaN (np.na do numpy) para que possamos
    # fazer a conversão para float. O método check_convertion_to_float() faz isso.
    @staticmethod
    def __check_convertion_to_float(col_name:str, cols: pd.Series) -> tuple[str, list[str]]:
        assert isinstance(cols, pd.Series)
        not_float_values = []
        # Definindo a expressão regular para identificar números de ponto flutuante
        # padrao = r'[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?'
        padrao = get_numeric_regex()
        # Use o método items() para iterar sobre os itens da série
        for idx, valor in cols.items():
            print(idx, valor)
            if (not re.match(padrao, str(valor))) and (not pd.isna(valor)):
                if isinstance(valor, str):
                    if valor not in ['NaN', '-NaN', '-nan', 'nan', '<NA>', 'N/A',
                                     'NA', 'NULL', 'None', 'n/a', 'null']:
                        # print(idx, '\t', valor, 'não é uma representação valida para NaN')
                        not_float_values.append(valor)
                else:
                    # print(idx, '\t', valor, 'não e um número valido e nem uma representação valida para NaN')
                    not_float_values.append(valor)
        if len(not_float_values) > 0:
            logger.debug('not_float_values: ' + str(not_float_values) + ' for column: ' + str(col_name))

        return (col_name, not_float_values)

    @staticmethod
    def check_convertion_to_float(numeric_columns: list[str], df: pd.DataFrame) -> dict[str, list[str]]:
        all_problems = {}
        for col in numeric_columns:
            print('Verificando a conversão para tipo Number da coluna', col)
            col_name, not_float_values = IO.__check_convertion_to_float(col, df[col])
            if len(not_float_values) > 0:
                all_problems.update({col_name: not_float_values})
        return all_problems

    # path é um Path identificando um nome de arquivo CSV e a extensão deve
    # ser obrigatoriamente '.csv' (em minusculo). Isto é uma convenção.
    # Além disso o arquivo deve existir e possuir na primira linha a lista
    # de nomes das colunas / campos (schema). A quintidade de elementos na
    # lista 'column_types' deve ser igual a quntidade de colunas no arquivo,
    # ou seja, deve ser igual ao tamanho da lista de nomes especificada na
    # primeira coluna do arquivo CSV.
    @staticmethod
    def read_csv_file(path: Path, column_types: list[type],
            nan_values:list[str] = [], parse_dates: list[int] = [1],
            date_format:str = 'AAAA-MM-DD HH:MM:SS') -> pd.DataFrame:
        for value in column_types:
            assert isinstance(value, type), f'Erro: {value} não é um tipo válido'

        # Define o conversor para cada tipo de coluna
        # Crio um dicionário com os conversores e tipos de colunas
        converters_tmp = {
            f'{i}': column_types[i] for i in range(len(column_types))
        }

        # print('converters:', converters_tmp, type(converters_tmp))
        result = pd.read_csv(filepath_or_buffer = path, sep=',', delimiter=None,
            header='infer', dtype=None, engine='c', converters=converters_tmp,
            na_values=nan_values, keep_default_na=True, na_filter=True, verbose=True,
            skip_blank_lines=True, parse_dates=parse_dates, date_format=date_format,
            dayfirst=False, cache_dates=False, compression='infer', thousands=None,
            decimal='.', quotechar='"', # Not Supported: lineterminator=os.linesep
            quoting=csv.QUOTE_MINIMAL, doublequote=True, comment=None, encoding='utf-8',
            encoding_errors='strict', on_bad_lines='error', delim_whitespace=False,
            low_memory=True, memory_map=False, dtype_backend='numpy_nullable')
        converters =  {}
        if len(converters_tmp) > 0:
            # TODO: Verificar como converter a primeira coluna que é Timestamp e é index.
            for idx, col in enumerate(result.columns[1:]):
                converters[col] = converters_tmp[str(idx)]
            print('converters:', converters, type(converters))

        numeric_columns = list(result.columns[1:])
        all_problems_on_data = IO.check_convertion_to_float(numeric_columns, result)
        for key, value in converters.items():
            assert isinstance(value, type), f'Erro: {value} não é um tipo válido para coluna {key}'
            # Mapeia os tokens inválidos para NaN
            if (key in all_problems_on_data.keys()):
                result[key] = result[key].replace(to_replace=all_problems_on_data[key], value=np.nan)
            print('coluna:', key, '\ttipo:', value)
            if '<NA>' in result[key].values:
                result[key] = result[key].replace(to_replace='<NA>', value='NaN')
            # Faz a conversão de tipo para cada coluna
            result[key] = result[key].astype(value)
            print('coluna:', key, '\ttipo:', value)

        return result

    @staticmethod
    def dataframe_to_csv_file(df: pd.DataFrame, path: Path) -> None:
        # Se ocorrer erro devemos lançar Exception
        #
        # O Pandas salva por padrão datetime como neste exemplo: "2022-11-29 10:40:55".
        # Este é um formato de data e hora padrão ISO 8601 que é um padrão internacional
        # para representação de datas e horas, e é amplamente utilizado em sistemas de
        # computação e comunicação. O formato ISO 8601 para datas e horas é definido
        # como "AAAA-MM-DDTHH:MM:SS", onde "AAAA" representa o ano com quatro dígitos,
        # "MM" representa o mês com dois dígitos, "DD" representa o dia com dois dígitos,
        # "T" é o separador entre a data e a hora, "HH" representa a hora com dois dígitos,
        # "MM" representa os minutos com dois dígitos e "SS" representa os segundos com
        # dois dígitos. No formato "2022-11-29 10:40:55", a data e a hora são separadas
        # por um espaço em vez do caractere "T", mas ainda segue o padrão ISO 8601
        # para representação de datas e horas.
        # Veja: https://www.w3.org/TR/NOTE-datetime
        df.to_csv(path, sep=',', na_rep='NaN', float_format=None,
            header=True, index=False, index_label=None, mode='w',
            encoding='utf-8', compression=None, quoting=csv.QUOTE_MINIMAL,
            quotechar='"', lineterminator=os.linesep, chunksize=None,
            date_format=None, doublequote=True, escapechar=None,
            decimal='.', errors='strict', storage_options=None)


if __name__ == "__main__":
    # Lê o arquivo CSV e gera um Dataframe com os tipos de dados corretos informados
    # na lista column_types que deve corresponder na mesma ordem com as colunas.
    path = Path('ts_01.csv')
    # TODO: Ver como resolver a conversão para pd.Timestamp, pois o Pandas não está fazendo isso automaticamente
    column_types: list[type] = [np.float32, np.float32]
    df_whith_nans = IO.read_csv_file(path, column_types)
    print('df_whith_nans:\n', df_whith_nans)
    print('Tipos das colunas no Dataframe da Série Temporal:')
    for col in df_whith_nans.columns:
        for value in df_whith_nans[col]:
            print(col, '\t', value, type(value))
        print('-----------------------------------------------')
