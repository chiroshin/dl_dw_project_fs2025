import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from datetime import datetime

s3 = boto3.client('s3')  # connect to S3

def scrape_commodity_table(category_keyword):
    url = "https://tradingeconomics.com/commodities"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")
    result = []

    tables = soup.find_all("table", class_="table-hover")
    for table in tables:
        header = table.find("thead")
        if header and category_keyword in header.get_text():
            rows = table.find("tbody").find_all("tr", {"data-symbol": True})
            for row in rows:
                name = "Unknown"
                price = "N/A"
                currency = "N/A"
                unit = "N/A"

                # Get name
                name_tag = row.find("b")
                if name_tag:
                    name = name_tag.get_text(strip=True)

                # Get price
                price_tag = row.find("td", {"id": "p"})
                if price_tag:
                    price = price_tag.get_text(strip=True)

                # Get currency and unit
                unit_div = row.find("td", class_="datatable-item-first").find("div")
                if unit_div:
                    unit_text = unit_div.get_text(strip=True)
                    if "/" in unit_text:
                        currency, unit = unit_text.split("/", 1)
                        currency = currency.strip()
                        unit = unit.strip()

                result.append({
                    "category": category_keyword,
                    "name": name,
                    "price": price,
                    "currency": currency,
                    "unit": unit
                })
            break
    return result


def lambda_handler(event, context):
    agriculture = scrape_commodity_table("Agricultural")
    livestock = scrape_commodity_table("Livestock")
    all_data = agriculture + livestock

    # Create DataFrame and convert to CSV
    df = pd.DataFrame(all_data)
    csv_data = df.to_csv(index=False)

    # Generate filename with timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"commodity_data_{timestamp}.csv"

    # Upload to S3
    s3.put_object(
        Bucket="seraina-commodity-data",  # <- use your real bucket name
        Key=filename,
        Body=csv_data
    )

    return {
        "statusCode": 200,
        "body": json.dumps(f"File {filename} uploaded to S3!")
    }
