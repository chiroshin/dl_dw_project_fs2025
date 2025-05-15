import pymysql
import os

def lambda_handler(event, context):
    try:
        # Connect to RDS
        connection = pymysql.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASS'],
            database='lakecrusher',
            port=int(os.environ.get('DB_PORT', 3306)),
            connect_timeout=5
        )

        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS commodity_prices_usd;")
            cursor.execute("""
                CREATE TABLE commodity_prices_usd (
                    category VARCHAR(100),
                    trading_goods_name VARCHAR(100) PRIMARY KEY,
                    price FLOAT,
                    currency VARCHAR(10),
                    unit VARCHAR(50),
                    converted_usd_price FLOAT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

        connection.commit()
        connection.close()

        return {
            "statusCode": 200,
            "body": "Table 'commodity_prices_usd' created successfully with new primary key."
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }
