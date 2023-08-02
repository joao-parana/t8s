Feature: Identify NaN values in multivariate and univariate Timeseries on wide format
  Value Statement:
    As a data analyst
    I want the ability to identify NaN values in multivariate and univariate Timeseries on wide format
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a wide format time series persisted to a Parquet file

  Scenario: Identify NaN values in multivariate Timeseries on wide format
    Given that I create a multivariate Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory
    When I check the multivariate Timeseries for NaN values
    Then I build a dictionary of NaN values blocks to use elsewhere
    And I check the result of NaNs blocks.
    # Constraint: The Timeseries has no invalid values

  Scenario: Identify NaN values in univariate Timeseries on wide format
    Given that I create a univariate Timeseries set using the selected parquet file in the T8S_WORKSPACE/data/parquet directory
    When I check the univariate Timeseries for NaN values
    Then I build a dictionary list of NaN values blocks to use elsewhere
    And I check the result of NaNs blocks of univariate Timeseries.
    # Constraint: The Timeseries has no invalid values
