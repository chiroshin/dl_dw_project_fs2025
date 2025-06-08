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
            # Drop child table first due to foreign key dependency
            cursor.execute("DROP TABLE IF EXISTS nutrition_data;")
            cursor.execute("DROP TABLE IF EXISTS commodity_prices_usd;")

            # Re-create commodity_prices_usd
            cursor.execute("""
                CREATE TABLE commodity_prices_usd ( 
                    category VARCHAR(100),
                    trading_goods_name VARCHAR(100),
                    price FLOAT,
                    currency VARCHAR(10),
                    unit VARCHAR(50),
                    converted_usd_price FLOAT,
                    timestamp CHAR(8),
                    PRIMARY KEY (trading_goods_name, timestamp)
                );
            """)

            # Drop and create nutrition_data
            cursor.execute("DROP TABLE IF EXISTS nutrition_data;")
            cursor.execute("""
                CREATE TABLE nutrition_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    trading_goods_name VARCHAR(100),
                    product_name VARCHAR(100),
                    kcal INT,
                    kJoule INT,
                    water DECIMAL(5,1),
                    protein DECIMAL(5,1),
                    carbohydrate DECIMAL(5,1),
                    sugars DECIMAL(5,1),
                    fat DECIMAL(5,1),
                    saturated_fat DECIMAL(5,1),
                    monounsat DECIMAL(5,1),
                    polyunsat DECIMAL(5,1),
                    cholesterol DECIMAL(5,1),
                    dietary_fiber DECIMAL(5,1),
                    emotional_value DECIMAL(5,1),
                    health_value DECIMAL(5,1),
                    timestamp CHAR(8),
                    CONSTRAINT fk_trading_goods
                        FOREIGN KEY (trading_goods_name, timestamp)
                        REFERENCES commodity_prices_usd(trading_goods_name, timestamp)
                );
            """)

            # Insert trading_goods_name entries
            valid_goods = [
                "Soybeans", "Wheat", "Lumber", "Palm Oil", "Cheese", "Milk", "Rubber",
                "Orange Juice", "Coffee", "Cotton", "Rice", "Canola", "Oat", "Wool",
                "Sugar", "Cocoa", "Tea", "Sunflower Oil", "Rapeseed", "Barley", "Butter",
                "Potatoes", "Corn", "Feeder Cattle", "Live Cattle", "Lean Hogs", "Beef",
                "Poultry", "Eggs US", "Eggs CH", "Salmon"
            ]

            for good in valid_goods:
                cursor.execute(
                    "INSERT IGNORE INTO commodity_prices_usd (trading_goods_name) VALUES (%s);",
                    (good,)
                )

        connection.commit()
        connection.close()

        return {
            "statusCode": 200,
            "body": "Tables created and trading goods inserted successfully."
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }
