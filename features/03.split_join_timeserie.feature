Feature: Convert a multivariate Timeseries to list of univariate Timeseries and vice versa

Value Statement:
    As a data analyst
    I want the ability to convert between Timeseries types ['univariate', 'multivariate'] for use in different situations
    So I can start analyzing the data right away and come up with solutions for the business.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a long format time series persisted to a Parquet file

  Scenario: Conversion of Timeseries types ['univariate', 'multivariate'] for use in different situations
    Given that I create a Timeseries using the selected parquet file in the T8S_WORKSPACE/data/parquet directory
    When I convert Timeseries from long format to wide format and check the convertion
    Then I can convert the Timeseries from multivariate to a list of univariate Timeseries
    And I convert the list of univariate Timeseries into a single multivariate Timeseries
    And I check the result.
    # Constraint: The Timeseries has no invalid values
