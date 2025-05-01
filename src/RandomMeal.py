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

    # Convert the 'meals' list from the API response into a DataFrame
    df = pd.DataFrame(data.get("meals", []))

    # Rename columns: remove the "str" prefix if it exists
    df.rename(columns=lambda x: x[3:] if x.startswith("str") else x, inplace=True)
    return df


def get_unique_meals(n=1000):
    """
    Retrieves random meals from the API n times, combines them into a DataFrame,
    removes duplicates, and returns the unique meal data.
    """
    all_meals = []

    # Loop n times to collect the data
    for i in range(n):
        try:
            meal_df = get_random_meal()
            all_meals.append(meal_df)
        except Exception as e:
            print(f"Error on iteration {i}: {e}")

    # Combine all collected DataFrames into one DataFrame
    combined_df = pd.concat(all_meals, ignore_index=True)

    # Remove duplicate rows and return the cleaned DataFrame
    unique_meals_df = combined_df.drop_duplicates()
    return unique_meals_df


if __name__ == '__main__':
    meals_df = get_unique_meals()
    print(meals_df)