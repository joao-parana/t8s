Feature: Create a time series set using Dataframe, CSV and Parquet

Value Statement:
  As a data analyst
  I want the hability to create a timeseries set using Dataframe, CSV and Parquet
  So that I can start studying the data right away and propose solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a bunch of CSV and Parquet files to analyze

  Scenario: First, I create 2 time series with sample data using Dataframes and save it at T8S_WORKSPACE/data directory
    Given a start timestamp, a number of records and a time interval
    When I create 2 time series using Dataframes with sample data
    Then I have a time series with the `correct` number of rows and columns, schema and time interval
    And I have a CSV file in T8S_WORKSPACE/data/csv correctelly formated
    And I have a Parquet file in T8S_WORKSPACE/data/parquet correctelly formated with metadata annotations
    And I have a text representation for the first time serie like this below
    """
    TimeSerie(format=wide, features=3, df=
                timestamp  temperatura  velocidade
    0 2022-01-01 00:00:00    25.000000      3000.0
    1 2022-01-01 01:00:00    26.000000      1100.0
    2 2022-01-01 02:00:00    27.000000      1200.0
    3 2022-01-01 03:00:00    23.200001      4000.0) +
    types: [<class 'pandas._libs.tslibs.timestamps.Timestamp'>, <class 'numpy.float32'>, <class 'numpy.float32'>]
    """
    # Constraint: The first  Dataframe doesn't have nulls or invalid values, but the second does

  Scenario: Second, I create 1 time series at T8S_WORKSPACE/data directory using a literal sample data
    Given a the table below as input
      | datetime            | float       | float       |
      | timestamp           | temperatura | velocidade  |
      | 2022-01-01 00:00:00 | 3.0         | 30.0        |
      | 2022-01-01 01:00:00 | 4.1         | 1124        |
      | 2022-01-01 02:00:00 | 5.2         | 3276        |
    When converted to a data frame using 1 row as column names and 1 column as index
    And printed using data_frame_to_table function
    Then I build a time series with the `correct` number of rows and columns