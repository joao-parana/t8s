Feature: Identify NaN values in multivariate and univariate Timeseries on wide format
  Value Statement:
    As a data analyst
    I want the ability to identify NaN values in multivariate and univariate Timeseries on wide format
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a TimeSerie with a bunch of NaNs blocks saved as a parquet file in T8S_WORKSPACE_DIR

  Scenario: Identify NaN values in univariate Timeseries on wide format
    Given that I read a multivariate Timeseries and convert to univariate timeseries list
    When I check the first univariate Timeseries from list for NaN values
    Then I build a dataframe describing blocks of NaN values to use elsewhere
    And I check the result of NaNs blocks of univariate Timeseries.
    # Constraint: The Timeseries has no invalid values
