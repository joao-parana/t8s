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
    # Constraint: The first  Dataframe doesn't have nulls or invalid values, but the second does
