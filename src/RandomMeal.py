import requests
import pandas as pd


def get_random_meal():
    """
    Retrieves a random meal from TheMealDB API and returns the data as a pandas DataFrame.
    """
    url = "https://www.themealdb.com/api/json/v1/1/random.php"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    print(data)

    # Convert the 'meals' list from the API response into a DataFrame
    df = pd.DataFrame(data.get("meals", []))

    # Rename columns: remove the "str" prefix if it exists
    df.rename(columns=lambda x: x[3:] if x.startswith("str") else x, inplace=True)
    return df


if __name__ == '__main__':
    meal_df = get_random_meal()
    print(meal_df)