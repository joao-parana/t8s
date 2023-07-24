# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import date, datetime
import gzip
import csv
import os
import gc
import yaml
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

import numpy as np
import pandas as pd
import pyarrow as pa         # type: ignore
import pyarrow.parquet as pq # type: ignore

from t8s.log_config import LogConfig

logger = LogConfig().getLogger()

import re
from enum import Enum
from t8s import get_numeric_regex

NULL_PATH: Path = Path('/NullPathObject')


class IO:
    all_columns_with_errors: dict[str, str] = {}
    all_tokens_with_errors: dict[str, str] = {}
    @staticmethod
    def add_column_name_and_invalid_token(column: str, token: str) -> None:
        # Atualize o dicionário xpto
        IO.all_columns_with_errors[column] = ''
        IO.all_tokens_with_errors[token] = ''

    # A linguagem Python não tem um meio de definir um método privado, mas o uso de dois underscore
    # no inicio do nome do método é uma convenção que indica que o método é privado e que estamos
    # usando a técnica name mangling para impedir a visibilidade padrão. Portanto o método
    # __check_csv_schema não deve ser chamado diretamente de fora da classe.
    @staticmethod
    def __check_csv_schema(csv_file_list: list[Path]):
        start_at = datetime.now()
        print('\nAtenção: Links simbólicos serão resolvidos, caso existam.')
        for f in csv_file_list:
            print('', f.resolve())

        csv_file_schema = ''
        # print('csv_file_list:', csv_file_list)
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

        # Podemos usar o Pandas para verificar os valores de cada coluna para encontrar possíveis
        # valores inválidos que não podem ser convertidos para o tipo da coluna. Por exemplo,
        # valores como  'Configure', 'Bad' e 'NotNumber', por exemplo, devem ser substituidos
        # por 'NaN' (np.na do numpy) para que possamos fazer a conversão para float.
        # Observe que o Pandas já tenta fazer uma inferencia de tipo para cada coluna, mas
        # quando não consegue determinar o tipo de uma coluna com precisão e sem ambiguidades
        # ele assume o tipo object. Assim, se o tipo de uma coluna for object, devemos usar uma
        # heuristica para determinar o tipo correto. Por exemplo, se a coluna contém apenas números
        # inteiros, podemos converter para int32 ou int64, dependendo do tamanho do número. Se a
        # coluna contém apenas números de ponto flutuante, podemos converter para float32 ou float64,
        # dependendo do tamanho do número. Se a coluna contém apenas strings, podemos converter para
        # string. Se a coluna contém apenas datas, podemos converter para datetime64[ns]. Se a coluna
        # contém apenas valores booleanos, podemos converter para bool. Se a coluna contém apenas NaN,
        # podemos converter para np.nan. Se a coluna contém apenas valores inválidos, podemos converter
        # para np.nan. Se a coluna contém apenas valores inválidos e NaN, podemos converter para np.nan.
        types: list[type] = []
        type_map: dict[str, type] = {}
        first_types = []
        first_map = {}
        for idx, csv_file in enumerate(csv_file_list):
            types, type_map = IO.check_data_types_in_csv_file(str(csv_file))
            if idx == 0:
                first_types = types
                first_map = type_map
            else:
                if types != first_types:
                    msg = (
                        f"Os tipos de dados do arquivo {csv_file} não são iguais "
                        + "aos tipos de dados do arquivo {csv_file_list[0]}"
                    )
                    raise ValueError(msg)

        if len(IO.all_columns_with_errors.keys()) > 0:
            print('Erros de tokens inválidos encontrados em campos numéricos dos arquivos CSV')
            for error in IO.all_columns_with_errors.keys():
                print(f'{error}')
        print(f'Este método __check_csv_schema() demorou: {datetime.now()-start_at}')
        return (types, type_map)

    @staticmethod
    def get_column_names_and_types_from_csv_file(csv_file: str) -> tuple[list[str], list[type], dict[str, type]]:
        # Define os tipos de dados das colunas assumindo a primeira como pd.Timestamp e a ultima como target
        # e todas as outras como features numéricas. Outros caso de uso não são tratados no momento.
        # Verifica se é arquivo CSV no formato texto, sem compactação e com a extensão .csv
        if not os.path.isfile(csv_file):
            raise ValueError(f"O arquivo {csv_file} não existe")

        column_names = []
        first_line = ''

        # Trata gzip compressed data
        if csv_file.endswith('.csv.gz'):
            # Abra o arquivo .txt.gz e leia a primeira linha como uma string
            with gzip.open(csv_file, 'rt') as f:
                first_line = f.readline()
            column_names = first_line.split(',')
        # Trata CSV não comprimido
        if csv_file.endswith('.csv'):
            with open(csv_file) as f:
                first_line = f.readline()
            column_names = first_line.split(',')
        column_type_dict: dict[str, type] = {}
        column_type_list: list[type] = []
        last_index = len(column_names) - 1
        for idx, col_name in enumerate(column_names):
            if idx == 0:
                column_type_list.append(datetime)
                column_type_dict[col_name] = pd.Timestamp
            elif idx == last_index:
                column_type_list.append(str)
                column_type_dict[col_name] = str
            else:
                column_type_list.append(np.float32)
                column_type_dict[col_name] = np.float32
            logger.debug(f'{idx}\t"{col_name}": type: {column_type_list[idx]}')

        return column_names, column_type_list, column_type_dict

    @staticmethod
    def check_data_types_in_csv_file(csv_file: str) -> tuple[list[type], dict[str, type]]:
        # TODO: No caso em que a primeira linha do CSV não tenha a definição dos nomes das colunas
        #       podemos usar o parâmetro header=None para que o Pandas crie nomes para as colunas.
        # -----------------------------------------------------------------------------------------------
        column_names, column_type_list, column_type_dict = IO.get_column_names_and_types_from_csv_file(csv_file)

        # Lê o arquivo CSV
        nan_values_tokens = ['Bad', 'Missing', 'Configure', 'nan', 'NaN']
        parse_dates: list[int] = [1] # Assumindo que a coluna 1 é a coluna de timestamp
        df = pd.read_csv(Path(csv_file)) # , parse_dates=parse_dates, na_values=nan_values_tokens

        # Valida os tipos de dados
        def check_number_regex(x: Any) -> bool:
            regex = get_numeric_regex()
            if not re.match(regex, str(x)):
                # print(f'check_number_regex() -> parms= col_name: {col_name}, token: {x}')
                IO.add_column_name_and_invalid_token(col_name, str(x))
                return False
            return True

        for i, col_type in enumerate(column_type_list):
            col_name = column_names[i]
            if col_name != '' and (col_type == np.float32 or col_type == np.float64):
                numeric_values = df[col_name].apply(check_number_regex).all()
                # print(df[col_name])
                if not numeric_values:
                    logger.error(f"A coluna {col_name} não contém apenas valores do tipo {col_type}")

        # Delete the used DataFrame
        del df

        # Perform garbage collection
        gc.collect()
        return (column_type_list, column_type_dict)

    # Quando directory, que é um Path, é informado ordenamos a lista de nomes
    # de arquivos CSV por convenção. O método retorna um Boolean que indica se
    # o schema de todos os arquivos CSV é o mesmo.
    @staticmethod
    def check_schema_in_csv_files(directory: Path = NULL_PATH, csv_files: list[Path] = []) -> bool:
        result = False
        csv_file_list = []
        if directory == NULL_PATH and len(csv_files) == 0:
            msg = 'Você deve fornecer um diretório ou uma lista de arquivos'
            raise ValueError('Parâmetros inválidos')
        elif directory != NULL_PATH and len(csv_files) > 0:
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
            # print(idx, valor)
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
    # de nomes das colunas / campos (schema). A quantidade de elementos na
    # lista 'column_types' deve ser igual a quntidade de colunas no arquivo,
    # ou seja, deve ser igual ao tamanho da lista de nomes especificada na
    # primeira coluna do arquivo CSV. Este método verifica os valores dos
    # campos numéricos contra as regras para Not A Number e retorna um
    # Dataframe com os tipos de dados numéricos corretos para cada coluna.
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
        result: pd.DataFrame = pd.read_csv(filepath_or_buffer = path, sep=',', delimiter=None,
            header='infer', dtype=None, engine='c', converters=converters_tmp,
            na_values=nan_values, keep_default_na=True, na_filter=True, verbose=True,
            skip_blank_lines=True, parse_dates=parse_dates, date_format=date_format,
            dayfirst=False, cache_dates=False, compression='infer', thousands=None,
            decimal='.', quotechar='"', # Not Supported: lineterminator=os.linesep
            quoting=csv.QUOTE_MINIMAL, doublequote=True, comment=None, encoding='utf-8',
            encoding_errors='strict', on_bad_lines='error', delim_whitespace=False,
            low_memory=True, memory_map=False, dtype_backend='numpy_nullable')
        print(result.info())
        # Criando um dicionário com os conversores de tipos para as colunas
        converters =  {}
        if len(converters_tmp) > 0:
            # TODO: Verificar como converter a primeira coluna que é Timestamp e é index.
            for idx, col in enumerate(result.columns):
                converters[col] = converters_tmp[str(idx)]
            print('converters:', converters, type(converters))

        numeric_columns = list(result.columns[1:])
        print('path =', path)
        all_problems_on_data = IO.check_convertion_to_float(numeric_columns, result)
        idx: int = 0
        for key, value in converters.items():
            assert isinstance(value, type), f'Erro: {value} não é um tipo válido para coluna {key}'
            # Mapeia os tokens inválidos para NaN
            if (key in all_problems_on_data.keys()):
                result[key] = result[key].replace(to_replace=all_problems_on_data[key], value=np.nan)
            print('coluna:', key, '\ttipo:', value)
            if '<NA>' in result[key].values:
                result[key] = result[key].replace(to_replace='<NA>', value='NaN')
            # Faz a conversão de tipo para cada coluna
            column_series = result[key]
            print(column_series, type(column_series), type(column_series[0]))
            if value == pd.Timestamp or value == datetime:
                result[key] = pd.to_datetime(result[key], format='%Y-%m-%d %H:%M:%S')
            else:
                result[key] = result[key].astype(value)

        print(result.info())
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
        print('csv_file_path =', path)
        df.to_csv(path, sep=',', na_rep='NaN', float_format=None,
            header=True, index=False, index_label=None, mode='w',
            encoding='utf-8', compression='infer', quoting=csv.QUOTE_MINIMAL,
            quotechar='"', lineterminator=os.linesep, chunksize=None,
            date_format=None, doublequote=True, escapechar=None,
            decimal='.', errors='strict', storage_options=None)

def __validate_type_convertion_test(path: Path):
    print('\n\n__validate_type_convertion(). Path:', path, '\n')
    column_types: list[type] = [pd.Timestamp, np.float32, np.float32]
    nan_values:list[str] = ['Bad', 'Configure']
    parse_dates: list[int] = [1]
    date_format:str = 'AAAA-MM-DD HH:MM:SS'
    df_whith_nans = IO.read_csv_file(path, column_types, nan_values, parse_dates, date_format)
    print('df_whith_nans:\n', df_whith_nans)
    print('Tipos das colunas no Dataframe da Série Temporal:')
    for col in df_whith_nans.columns:
        for value in df_whith_nans[col]:
            print(col, '\t', value, type(value))
        print('-----------------------------------------------')

if __name__ == "__main__":
    # Lê o arquivo CSV e gera um Dataframe com os tipos de dados corretos informados
    # na lista column_types que deve corresponder na mesma ordem com as colunas.
    __validate_type_convertion_test(Path('data/csv/ts_01.csv'))
    __validate_type_convertion_test(Path('data/csv/ts_02.csv'))
