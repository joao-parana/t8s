Feature: Outlier Detection in multivariate and univariate Timeseries on wide format
  Value Statement:
    As a data analyst
    I want the ability to identify outliers in multivariate and univariate Timeseries on wide format
    So I can start analyzing the data right away and come up with solutions for the business.

  Scenario: Detecting outliers in univariate Timeseries on wide format

    Given a time series dataset
      | Timestamp       | Value |
      | 2023-01-01      | 10    |
      | 2023-01-02      | 15    |
      | 2023-01-03      | 12    |
      | 2023-01-04      | 14    |
      | 2023-01-05      | 120   |
      | 2023-01-06      | 13    |
      | 2023-01-07      | 16    |
      | 2023-01-08      | 18    |
      | 2023-01-09      | 14    |
      | 2023-01-10      | 17    |

    When the Z-Score outlier detection algorithm is applied
    Then outliers should be identified using Z-Score
      | Timestamp       | Value | Outlier Detection Method |
      | 2023-01-05      | 120   | Z-Score                 |

    And non-outliers should not be flagged as outliers
      | Timestamp       | Value | Outlier Detection Method |
      | 2023-01-01      | 10    | None                    |
      | 2023-01-02      | 15    | None                    |
      | 2023-01-03      | 12    | None                    |
      | 2023-01-04      | 14    | None                    |
      | 2023-01-06      | 13    | None                    |
      | 2023-01-07      | 16    | None                    |
      | 2023-01-08      | 18    | None                    |
      | 2023-01-09      | 14    | None                    |
      | 2023-01-10      | 17    | None                    |

    When the IQR-based outlier detection algorithm is applied
    Then outliers should be identified using IQR
      | Timestamp       | Value | Outlier Detection Method |
      | 2023-01-05      | 120   | IQR                     |

    And non-outliers should not be flagged as outliers
      | Timestamp       | Value | Outlier Detection Method |
      | 2023-01-01      | 10    | None                    |
      | 2023-01-02      | 15    | None                    |
      | 2023-01-03      | 12    | None                    |
      | 2023-01-04      | 14    | None                    |
      | 2023-01-06      | 13    | None                    |
      | 2023-01-07      | 16    | None                    |
      | 2023-01-08      | 18    | None                    |
      | 2023-01-09      | 14    | None                    |
      | 2023-01-10      | 17    | None                    |
