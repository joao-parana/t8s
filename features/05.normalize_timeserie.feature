Feature: Normalize data for multivariate and univariate Timeseries on wide format
  Value Statement:
    As a data analyst
    I want the ability to normalize values in multivariate and univariate Timeseries on wide format
    So I can make the data available for machine learning training to solve business problems.

  Background:
    Given that I have a T8S_WORKSPACE_DIR and a wide format time series available as Parquet file

  Scenario: Normalize multivariate time series data using normalization methods such as MinMaxScaler RobustScaler
    Given that I create a multivariate Timeseries using the selected parquet file
    When I normalize the multivariate time series data using the chosen methods below
    """
    StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler, QuantileTransformer, PowerTransformer
    """
    Then I check the result of normalization running the inverse operation (denormalize) for some values
    # Constraint: The Timeseries has no invalid values

  Scenario: Normalize some features in multivariate time series data
    Given that I create a multivariate Timeseries using the selected parquet file
    When I normalize only some of the features in the multivariate time series data using the methods below
    """
    StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler, QuantileTransformer, PowerTransformer
    """
    Then I check the result of normalization running the inverse operation (denormalize) for some values
    # Constraint: The Timeseries has no invalid values

