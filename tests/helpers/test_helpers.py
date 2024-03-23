import os
import pandas as pd
import pytest
from datetime import datetime
from helpers.gen_samples import (
    generate_fake_data,
)


def test_file_creation(tmp_path):
    """
    Test if the CSV file is created at the specified location.
    """
    filename = tmp_path / "test_sales_data.csv"
    generate_fake_data(str(filename), 100, 10)
    assert os.path.isfile(filename)


def test_row_count(tmp_path):
    """
    Test if the generated CSV file contains the correct number of samples.
    """
    filename = tmp_path / "test_sales_data.csv"
    number_of_samples = 100
    generate_fake_data(str(filename), number_of_samples, 10)

    df = pd.read_csv(filename)
    assert len(df) == number_of_samples


def test_null_percentage(tmp_path):
    """
    Test that the null percentages of date and category fields are within expected ranges.
    """
    filename = tmp_path / "test_sales_data.csv"
    number_of_samples = 1000  # increased sample size for better statistical accuracy
    null_percentage = 10
    generate_fake_data(str(filename), number_of_samples, null_percentage)

    df = pd.read_csv(filename, na_values="NULL")

    for column in ["sale_date", "product_category"]:
        null_count = df[column].isna().sum()

        # Calculate actual percentage
        actual_null_percentage = (null_count / number_of_samples) * 100

        # Assert that actual percentage is close to the specified null percentage
        assert actual_null_percentage == pytest.approx(
            null_percentage, abs=2
        )  # abs determines the absolute tolerance


def test_column_existence(tmp_path):
    """
    Test that all required columns exist in the generated CSV file.
    """
    filename = tmp_path / "test_sales_data.csv"
    generate_fake_data(str(filename), 100, 10)

    df = pd.read_csv(filename)
    expected_columns = {"store_name", "sale_date", "sales_amount", "product_category"}
    assert set(df.columns) == expected_columns


def test_sales_amount_range(tmp_path):
    """
    Test that sales_amount values are within the expected range (10.0 to 2000.0).
    """
    filename = tmp_path / "test_sales_data.csv"
    generate_fake_data(str(filename), 100, 10)

    df = pd.read_csv(filename)
    assert (df["sales_amount"] >= 10.0).all() and (df["sales_amount"] <= 2000.0).all()


def test_date_range(tmp_path):
    """
    Test that sale_date values, if present, are within the expected date range.
    """
    filename = tmp_path / "test_sales_data.csv"
    generate_fake_data(str(filename), 100, 10)

    df = pd.read_csv(filename, parse_dates=["sale_date"], na_values="NULL")
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 1, 1)

    assert (df["sale_date"].dropna() >= start_date).all() and (
        df["sale_date"].dropna() <= end_date
    ).all()
