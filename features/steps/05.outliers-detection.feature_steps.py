# Import necessary libraries
from behave import given, when, then
import pandas as pd
import numpy as np

from t8s.util import Util


# Define the time series dataset
time_series_data = [
    ("2023-01-01", 10),
    ("2023-01-02", 15),
    ("2023-01-03", 12),
    ("2023-01-04", 14),
    ("2023-01-05", 120),
    ("2023-01-06", 13),
    ("2023-01-07", 16),
    ("2023-01-08", 18),
    ("2023-01-09", 14),
    ("2023-01-10", 17)
]

# Create a pandas.DataFrame from the time series dataset
df = pd.DataFrame(time_series_data, columns=["timestamp", "tag"])


@given('a time series dataset')
def step_given_time_series(context):
    context.time_series = df

@when('the Z-Score outlier detection algorithm is applied')
def step_when_zscore_detection(context):
    df = context.time_series
    outliers_mask = Util.detect_outliers(df, 'tag', 'zscore')
    context.outliers = context.time_series[outliers_mask]['timestamp'].tolist()

@then('outliers should be identified using Z-Score')
def step_then_zscore_outliers(context):
    expected_outliers = ['2023-01-05']
    assert context.outliers == expected_outliers

@when('the IQR-based outlier detection algorithm is applied')
def step_when_iqr_detection(context):
    df = context.time_series
    outliers_mask = Util.detect_outliers(df, 'tag', 'iqr')
    context.outliers = context.time_series[outliers_mask]['timestamp'].tolist()

@then('outliers should be identified using IQR')
def step_then_iqr_outliers(context):
    expected_outliers = ['2023-01-05']
    assert context.outliers == expected_outliers

@then('non-outliers should not be flagged as outliers')
def step_then_non_outliers(context):
    expected_non_outliers = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10']
    detected_non_outliers = [x for x in context.time_series['timestamp'].tolist() if x not in context.outliers]
    assert detected_non_outliers == expected_non_outliers
