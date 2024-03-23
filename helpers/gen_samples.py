import pandas as pd
import random
import os
from datetime import datetime, timedelta


def generate_fake_data(filename: str, number_of_samples: int, null_percentage: int):
    """
    Generate a CSV file with simulated sales data.

    This function creates a DataFrame with random sales data including dates,
    categories, and amounts. A specified percentage of the date and category
    fields can be randomly set to null.

    Parameters:
    filename (str): The name of the file to save the data to.
    number_of_samples (int): The number of samples/rows to generate.
    null_percentage (float): The percentage of the data that should be null.

    Returns:
    None: This function does not return a value; it writes data to a CSV file.
    """

    store_names = ["Store_A", "Store_B", "Store_C", "Store_D", "Store_E"]
    product_categories = [
        "Electronics",
        "Clothing",
        "Groceries",
        "Toys",
        "Furniture",
        None,
    ]  # Includes None for nulls

    # Adjusted null percentages
    date_null_percentage = null_percentage
    category_null_percentage = null_percentage

    # Helper function to generate a random date
    def random_date(start, end):
        return start + timedelta(
            seconds=random.randint(0, int((end - start).total_seconds()))
        )

    # Generate sample data
    df = pd.DataFrame(
        {
            "store_name": [
                random.choice(store_names) for _ in range(number_of_samples)
            ],
            "sale_date": [
                (
                    random_date(datetime(2020, 1, 1), datetime(2023, 1, 1))
                    if random.random() > date_null_percentage / 100.0
                    else None
                )
                for _ in range(number_of_samples)
            ],
            "sales_amount": [
                round(random.uniform(10.0, 2000.0), 2) for _ in range(number_of_samples)
            ],
            "product_category": [
                (
                    random.choice(product_categories[:-1])
                    if random.random() > category_null_percentage / 100.0
                    else None
                )
                for _ in range(number_of_samples)
            ],
        }
    )

    df.to_csv(filename, index=False, na_rep="NULL")
    print(f"Sales data has been successfully created and saved to {filename}")


base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_file = os.path.join(base_dir, "data", "sales_data.csv")

generate_fake_data(data_file, 10000, 10)
