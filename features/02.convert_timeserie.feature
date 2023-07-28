Feature: Convert a time series from wide format to long format and vice versa

Value Statement:
    As a data analyst
    I want the ability to convert time series format ['long', 'wide'] for use in different applications
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a Parquet file to analyze

  Scenario:
    Given I create a time series using the selected parquet file at T8S_WORKSPACE/data/parquet directory
    When I convert the time series from the original wide format to long format
    Then I have a time series with 3 columns and the `correct` number of rows
    And I have a text representation for the time serie like this below
    """
    TimeSerie(format=long, features=3, df=
                timestamp           ds        value
    0 2022-01-01 00:00:00  temperatura    25.000000
    4 2022-01-01 00:00:00   velocidade  3000.000000
    1 2022-01-01 01:00:00  temperatura    26.000000
    5 2022-01-01 01:00:00   velocidade  1100.000000
    2 2022-01-01 02:00:00  temperatura    27.000000
    6 2022-01-01 02:00:00   velocidade  1200.000000
    3 2022-01-01 03:00:00  temperatura    23.200001
    7 2022-01-01 03:00:00   velocidade  4000.000000) +
    types: [<class 'pandas._libs.tslibs.timestamps.Timestamp'>, <class 'str'>, <class 'numpy.float32'>]
    """
    And can I save this long format time series to a parquet file in the T8S_WORKSPACE_DIR/data/parquet directory

    # Constraint: The Dataframe doesn't have invalid values
