import boto3
import pandas as pd
import pymysql
import os
from io import StringIO
from datetime import datetime

# Environment variables
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = 'lakecrusher'
DB_PORT = int(os.environ.get('DB_PORT', 3306))

COMMODITY_BUCKET = 'seraina-commodity-prod'
EXCHANGE_BUCKET = 'exchange-rate-bucket-lakecrusher'
EXCHANGE_PREFIX = 'exchange_rates/'

s3 = boto3.client('s3')

def get_latest_csv(bucket, prefix=''):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    csv_files = sorted(
        [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')],
        reverse=True
    )
    if not csv_files:
        raise Exception(f"No CSV files found in {bucket}/{prefix}")
    latest_key = csv_files[0]
    obj = s3.get_object(Bucket=bucket, Key=latest_key)
    return pd.read_csv(obj['Body']), latest_key

def clean_currency(value):
    value = str(value).upper()
    if 'USD' in value:
        return 'USD'
    return value.strip()

def lambda_handler(event, context):
    try:
        # Load CSVs from S3
        commodities_df, commodities_key = get_latest_csv(COMMODITY_BUCKET)
        exchange_df, exchange_key = get_latest_csv(EXCHANGE_BUCKET, EXCHANGE_PREFIX)

        # Rename column for schema compatibility
        commodities_df.rename(columns={'name': 'trading_goods_name'}, inplace=True)

        # Normalize currency names
        commodities_df['currency'] = commodities_df['currency'].apply(clean_currency)
        exchange_rates = dict(zip(exchange_df['Currency'], exchange_df['ExchangeRate']))

        # Convert prices to USD
        converted_usd = []
        for _, row in commodities_df.iterrows():
            currency = row['currency']
            price = row['price']
            try:
                if currency == 'USD':
                    usd_price = float(price)
                elif currency in exchange_rates:
                    rate = float(exchange_rates[currency])
                    usd_price = float(price) / rate if rate != 0 else None
                else:
                    usd_price = None
            except:
                usd_price = None
            converted_usd.append(usd_price)

        commodities_df['converted_usd_price'] = converted_usd

        # Add YYYYMMDD timestamp column
        timestamp_str = datetime.utcnow().strftime('%Y%m%d')
        commodities_df['timestamp'] = timestamp_str

        # Insert into RDS
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            port=DB_PORT,
            connect_timeout=10
        )

        with conn.cursor() as cursor:
            insert_stmt = """
                INSERT INTO commodity_prices_usd
                (category, trading_goods_name, price, currency, unit, converted_usd_price, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            for _, row in commodities_df.iterrows():
                cursor.execute(insert_stmt, (
                    row['category'],
                    row['trading_goods_name'],
                    row['price'],
                    row['currency'],
                    row['unit'],
                    row['converted_usd_price'],
                    row['timestamp']
                ))

        conn.commit()
        conn.close()

        return {
            'statusCode': 200,
            'body': f"Inserted {len(commodities_df)} rows into updated table using exchange file '{exchange_key}' and data file '{commodities_key}'"
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }