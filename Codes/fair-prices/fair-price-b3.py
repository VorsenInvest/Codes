import pymysql
from dotenv import load_dotenv
import os
import math
import requests

# Load environment variables
load_dotenv()

# Database credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Connect to the database
try:
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Connected to the database successfully")

    with connection.cursor() as cursor:
        cursor.execute("SET SQL_SAFE_UPDATES = 0;")
        cursor.execute("DELETE FROM fair_price_b3;")
        cursor.execute("ALTER TABLE fair_price_b3 AUTO_INCREMENT = 1;")
        cursor.execute("SET SQL_SAFE_UPDATES = 1;")
        # Select the desired columns from weighted_b3
        cursor.execute("""
            SELECT `key`,
                sum_marketCap AS marketCap_b3, 
                weighted_mean_priceEarnings AS priceEarnings_b3,
                weighted_mean_forwardPE AS forwardPE_b3,
                weighted_mean_trailingEps AS trailingEps_b3,
                weighted_mean_forwardEps AS forwardEps_b3,
                weighted_mean_bookValue AS bookValue_b3,
                weighted_mean_priceToBook AS priceToBook_b3
            FROM weighted_b3;
        """)
        results = cursor.fetchall()

        # Fetching regularMarketPrice from the ^BVSP endpoint
        url = "https://brapi.dev/api/quote/^BVSP"
        params = {
            'range': '5d',
            'interval': '1d',
            'fundamental': 'true',
            'dividends': 'true',
            'modules': 'balanceSheetHistory',
            'token': 'fUNoicPSZTzstUdqw1uVeB',
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            regular_market_price = data['results'][0]['regularMarketPrice']
        else:
            print(f"Request failed with status code {response.status_code}")
            regular_market_price = None  # Set regular_market_price to None if request fails

        # Insert the selected data into fair_price_b3
        for row in results:
            # Calculate fairPricePEtEpst_b3
            fairPricePEtEpst_b3 = math.sqrt(row['priceEarnings_b3'] * row['trailingEps_b3'] * row['bookValue_b3'] * row['priceToBook_b3'])
            fairPricePEtEpsf_b3 = math.sqrt(row['priceEarnings_b3'] * row['forwardEps_b3'] * row['bookValue_b3'] * row['priceToBook_b3'])
            fairPricePEfEpst_b3 = math.sqrt(row['forwardPE_b3'] * row['trailingEps_b3'] * row['bookValue_b3'] * row['priceToBook_b3'])
            fairPricePEfEpsf_b3 = math.sqrt(row['forwardPE_b3'] * row['forwardEps_b3'] * row['bookValue_b3'] * row['priceToBook_b3'])

            # Calculate index-adjusted fair prices
            fairIndexPEtEpst_b3 = (fairPricePEtEpst_b3 / fairPricePEtEpst_b3) * regular_market_price
            fairIndexPEtEpsf_b3 = (fairPricePEtEpsf_b3 / fairPricePEtEpst_b3) * regular_market_price
            fairIndexPEfEpst_b3 = (fairPricePEfEpst_b3 / fairPricePEtEpst_b3) * regular_market_price
            fairPIndexPEfEpsf_b3 = (fairPricePEfEpsf_b3 / fairPricePEtEpst_b3) * regular_market_price
            
            data_tuple = (
                row['key'],
                row['marketCap_b3'],
                row['priceEarnings_b3'],
                row['forwardPE_b3'],
                row['trailingEps_b3'],
                row['forwardEps_b3'],
                row['bookValue_b3'],
                row['priceToBook_b3'],
                fairPricePEtEpst_b3,
                fairPricePEtEpsf_b3,
                fairPricePEfEpst_b3,
                fairPricePEfEpsf_b3,
                regular_market_price,  # Add regularMarketPrice to the data tuple
                fairIndexPEtEpst_b3,
                fairIndexPEtEpsf_b3,
                fairIndexPEfEpst_b3,
                fairPIndexPEfEpsf_b3
            )
            cursor.execute("""
                INSERT INTO fair_price_b3 (`key`, marketCap_b3, priceEarnings_b3, forwardPE_b3, trailingEps_b3, forwardEps_b3, bookValue_b3, priceToBook_b3, 
                           fairPricePEtEpst_b3, fairPricePEtEpsf_b3, fairPricePEfEpst_b3, fairPricePEfEpsf_b3, index_b3, 
                           fairIndexPEtEpst_b3, fairIndexPEtEpsf_b3, fairIndexPEfEpst_b3, fairPIndexPEfEpsf_b3)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                marketCap_b3 = VALUES(marketCap_b3),
                priceEarnings_b3 = VALUES(priceEarnings_b3),
                forwardPE_b3 = VALUES(forwardPE_b3),
                trailingEps_b3 = VALUES(trailingEps_b3),
                forwardEps_b3 = VALUES(forwardEps_b3),
                bookValue_b3 = VALUES(bookValue_b3),
                priceToBook_b3 = VALUES(priceToBook_b3),
                fairPricePEtEpst_b3 = VALUES(fairPricePEtEpst_b3),
                fairPricePEtEpsf_b3 = VALUES(fairPricePEtEpsf_b3),
                fairPricePEfEpst_b3 = VALUES(fairPricePEfEpst_b3),
                fairPricePEfEpsf_b3 = VALUES(fairPricePEfEpsf_b3),
                index_b3 = VALUES(index_b3),
                fairIndexPEtEpst_b3 = VALUES(fairIndexPEtEpst_b3),
                fairIndexPEtEpsf_b3 = VALUES(fairIndexPEtEpsf_b3),
                fairIndexPEfEpst_b3 = VALUES(fairIndexPEfEpst_b3),
                fairPIndexPEfEpsf_b3 = VALUES(fairPIndexPEfEpsf_b3);
            """, data_tuple)

        connection.commit()
        print("Data successfully processed and inserted/updated into 'fair_price_b3'.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if connection:
        connection.close()
    print("Database connection closed.")
