"""
@file: NutrionTable.py
@date: 23/02/2025
@author: roshin
"""

# all imports
import string
import time
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# specific pandas df
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', None)
pd.set_option('display.precision', 2)

# define url 
url_nutritionTable = "https://www.nutritiontable.com/nutritions/"

# scraping function for main nutrition Values
class NutritionScraper:
    def __init__(self, url_base: str):
        """
        Initializes the NutritionScraper with the base URL.
        
        :param url_base: The base URL of the nutrition table website.
        """
        self.url_base = url_base
        self.headers: Optional[list] = None
        self.all_dfs: list[pd.DataFrame] = []  # List to store DataFrames for each letter

    def extract_data_for_letter(self, letter: str) -> Optional[pd.DataFrame]:
        """
        Fetches the page for a given letter, extracts headers if necessary, 
        and collects the product data for that letter.

        :param letter: A single letter representing the section of the website to scrape.
        :return: A DataFrame containing the nutrition data for the given letter, or None if an error occurs.
        """
        try:
            # Fetch the page content
            response = self.fetch_page(letter)
            if not response:
                print(f"Skipping letter {letter} due to fetch error.")
                return None

            soup = BeautifulSoup(response.content, "html.parser")

            # Extract headers if not already set
            if not self.headers:
                header_elements = soup.find_all(class_="cBlue")
                if not header_elements:
                    raise ValueError(f"No headers found for letter {letter}")

                self.headers = ['Product Name'] + [element.get_text().strip() for element in header_elements]
                # Ensure headers match the required 14 columns
                self.headers = ['Product Name', 'kcal', 'kJoule', 'water', 'protein', 'carbohydrat', 'sugars',
                                'fat', 'saturated_fat', 'monounsat', 'polyunsat', 'cholesterol',
                                'dietary_fiber', 'emotional_value', 'health_value']

            # Extract product names using list comprehension
            product_names = [element.get_text().strip() for element in soup.find_all(class_="prodNameLink")]
            if not product_names:
                print(f"Warning: No product names found for letter {letter}")
                return None

            # Create an empty DataFrame for this letter's data
            df = pd.DataFrame([[None] * (len(self.headers) - 1)] * len(product_names), columns=self.headers[1:])
            df.insert(0, 'Product Name', product_names)

            # Fill the DataFrame with nutritional values
            base_id = lambda idx: f"ctl00_cphMain_ltvNutrition_ctrl{idx}_"
            fields = [('kcal', 'lblKcal'), ('kJoule', 'lblKjoule'), ('water', 'lblWater'),
                      ('protein', 'lblEiwit'), ('carbohydrat', 'lblKoolh'), ('sugars', 'lblSuikers'),
                      ('fat', 'lblVet'), ('saturated_fat', 'lblVerz'), ('monounsat', 'lblEov'),
                      ('polyunsat', 'lblMov'), ('cholesterol', 'lblChol'), ('dietary_fiber', 'lblVoedv'),
                      ('emotional_value', 'lblFeeling'), ('health_value', 'lblHealty')]

            # Populate the DataFrame by iterating over the products
            for idx in range(len(df)):
                get_value = lambda label: self.extract_value(soup, base_id(idx), label)
                for field, label in fields:
                    df.at[idx, field] = get_value(label)

            return df

        except Exception as e:
            print(f"Error processing letter {letter}: {e}")
            return None

    def fetch_page(self, letter: str) -> Optional[requests.Response]:
        """
        Fetches the page content for a given letter of the alphabet.

        :param letter: A single letter representing the section of the website to scrape.
        :return: The Response object containing the page content, or None if there was an error.
        """
        try:
            response = requests.get(self.url_base + letter + '/')
            response.raise_for_status()  # Raise an exception for bad status codes
            return response
        except requests.RequestException as e:
            print(f"Error fetching {self.url_base + letter}/: {e}")
            return None

    @staticmethod
    def extract_value(soup: BeautifulSoup, base: str, label: str) -> Optional[str]:
        """
        Extracts the value from a specific HTML element identified by a base ID and label.

        :param soup: The BeautifulSoup object representing the parsed HTML of the page.
        :param base: The base ID used to identify the specific element.
        :param label: The specific label that forms part of the element's ID.
        :return: The text value of the element, or None if not found or empty.
        """
        try:
            element = soup.find("span", id=base + label)
            if element:
                value = element.get_text().strip()
                if value:  # Ensure the value is not empty
                    return value
            return None
        except Exception as e:
            print(f"Error extracting value for {label}: {e}")
            return None

    # main Function
    def main(self) -> pd.DataFrame:
        """
        Extracts nutrition tables for all letters (A-Z) and combines them into a single DataFrame.
    
        :return: A DataFrame containing all nutrition data, or an empty DataFrame if no data was found.
        """
        try:
            if self.headers is None:
                pass

            # Loop through each letter and collect DataFrames
            for letter in string.ascii_uppercase:
                df = self.extract_data_for_letter(letter)
                if df is not None and not df.empty:
                    self.all_dfs.append(df)

            # Validate that DataFrames were collected
            if not self.all_dfs:
                print("No dataframes collected.")
                return pd.DataFrame()

            # Combine all DataFrames
            combined_df = pd.concat(self.all_dfs, ignore_index=True)

            # Print important statistics
            total_items = len(combined_df)
            print(f"\nTotal count of food items: {total_items}\n")
            print(f"Total of rows: {combined_df.shape[0]}")
            print(f"\nTotal of columns: {combined_df.shape[1]}\n")

            return combined_df

        except Exception as e:
            print(f"Error during data extraction: {e}")
            return pd.DataFrame()


# scraping function for main Vitamin & Mineral values
class Scraper:
    """
    Base class to initialize the Selenium WebDriver and provide functions for scraping.
    """

    def __init__(self, url):
        """
        Initialize the Scraper with the target URL and sets up the WebDriver.

        Args:
        url (str): The URL to start scraping from.
        """
        self.url = url
        self.driver = self._init_driver()

    @staticmethod
    def _init_driver():
        """
        Initializes the Chrome WebDriver with the necessary options.

        Returns:
        WebDriver: The initialized Chrome WebDriver instance.
        """
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        return webdriver.Chrome(options=chrome_options)

    def open_page(self, url):
        """
        Opens a given URL in the WebDriver.

        Args:
        url (str): The URL to open.
        """
        self.driver.get(url)
        print(f"URL Page: {url}")
        time.sleep(2)  # wait for the page to load

    def close(self):
        """Closes the WebDriver."""
        self.driver.quit()


# extract table nutrition (ATTENTION - Take around 40s to run this CODE)
NutritionScraper = NutritionScraper(url_nutritionTable)
df_NutritionTable = NutritionScraper.main()