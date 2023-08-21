Feature: Create a time series set using Dataframe, CSV and Parquet

Value Statement:
  As a data analyst
  I want the ability to create a timeseries set using Dataframe, CSV and Parquet files, or SQL queries
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

  Scenario: Third, I create a time series using a datafusion query and save as a parquet file
    Given a Datafusion SQL query
    When I convert a Datafusion Table to a Pandas Dataframe using the Datafusion API and the query mentioned above
    And I create a time series
    Then I have a time series with the `correct` number of rows and columns, schema and time interval to be checked
    # https://github.com/apache/arrow-datafusion-python/blob/main/examples/sql-on-pandas.py

  Scenario: Fourth, I need to display the descriptive statistics of a time series
    Given a time series
    When I call the get_statistics function
    Then I have a descriptive statistics object for the time series

  Scenario: Fifth, I need to find the standard deviation of a time series to check for outliers using a naive method
    Given a time series
    When I call the get_min_max_variation_factors function in the Util class
    Then I have a dictionary object with minimum and maximum multiplication factor for each feature
    And I can use this information to check for outliers using a naive method

  Scenario: Sixth, I need to select only a subset of features in a time series persisted as a parquet file
    Given a time series
    When I pass select_features as a list of feature names to the TimeSerie constructor
    Then I have a time series with only a subset of features as defined in the list
    And I can read from the file system only the resources I need, improving performance when reading data.
    # It is done using parquet module from pyarrow package
