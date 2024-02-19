import pymysql
from dotenv import load_dotenv
import os
import math

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
        cursor.execute("DELETE FROM fair_price_economicSectors;")
        
        cursor.execute("ALTER TABLE fair_price_economicSectors AUTO_INCREMENT = 1;")

        # Fetch values from fair_price_b3
        cursor.execute("""
            SELECT fairPricePEtEpst_b3, fairPricePEtEpsf_b3, fairPricePEfEpst_b3, fairPricePEfEpsf_b3
            FROM fair_price_b3
            WHERE id = 1; # Adjust if necessary
        """)
        b3_values = cursor.fetchone()
        # Select the desired columns from weighted_economicSectors
        cursor.execute("""
            SELECT `key`,
                sum_marketCap AS marketCap, 
                weighted_mean_priceEarnings AS priceEarnings,
                weighted_mean_forwardPE AS forwardPE,
                weighted_mean_trailingEps AS trailingEps,
                weighted_mean_forwardEps AS forwardEps,
                weighted_mean_bookValue AS bookValue,
                weighted_mean_priceToBook AS priceToBook
            FROM weighted_economicSectors;
        """)
        results = cursor.fetchall()

        # Insert the selected data into fair_price_economicSectors
        for row in results:
            # Check for non-negative and non-null values before calculation
            def calc_fair_price(pe, eps, bv, pb):
                if all(v is not None and v >= 0 for v in [pe, eps, bv, pb]):
                    return math.sqrt(pe * eps * bv * pb)
                else:
                    return None

            fairPricePEtEpst = calc_fair_price(row['priceEarnings'], row['trailingEps'], row['bookValue'], row['priceToBook'])
            fairPricePEtEpsf = calc_fair_price(row['priceEarnings'], row['forwardEps'], row['bookValue'], row['priceToBook'])
            fairPricePEfEpst = calc_fair_price(row['forwardPE'], row['trailingEps'], row['bookValue'], row['priceToBook'])
            fairPricePEfEpsf = calc_fair_price(row['forwardPE'], row['forwardEps'], row['bookValue'], row['priceToBook'])

            data_tuple = (
                row['key'],
                row['marketCap'],
                row['priceEarnings'],
                row['forwardPE'],
                row['trailingEps'],
                row['forwardEps'],
                row['bookValue'],
                row['priceToBook'],
                fairPricePEtEpst,
                fairPricePEtEpsf,
                fairPricePEfEpst,
                fairPricePEfEpsf,
                b3_values['fairPricePEtEpst_b3'],
                b3_values['fairPricePEtEpsf_b3'],
                b3_values['fairPricePEfEpst_b3'],
                b3_values['fairPricePEfEpsf_b3']
            )
            cursor.execute("""
                INSERT INTO fair_price_economicSectors (`key`, marketCap, priceEarnings, forwardPE, trailingEps, forwardEps, bookValue, priceToBook, 
                fairPricePEtEpst, fairPricePEtEpsf, fairPricePEfEpst, fairPricePEfEpsf, fairPricePEtEpst_b3, fairPricePEtEpsf_b3, fairPricePEfEpst_b3, fairPricePEfEpsf_b3)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                marketCap = VALUES(marketCap),
                priceEarnings = VALUES(priceEarnings),
                forwardPE = VALUES(forwardPE),
                trailingEps = VALUES(trailingEps),
                forwardEps = VALUES(forwardEps),
                bookValue = VALUES(bookValue),
                priceToBook = VALUES(priceToBook),
                fairPricePEtEpst = VALUES(fairPricePEtEpst),
                fairPricePEtEpsf = VALUES(fairPricePEtEpsf),
                fairPricePEfEpst = VALUES(fairPricePEfEpst),
                fairPricePEfEpsf = VALUES(fairPricePEfEpsf),
                fairPricePEtEpst_b3 = VALUES(fairPricePEtEpst_b3),
                fairPricePEtEpsf_b3 = VALUES(fairPricePEtEpsf_b3),
                fairPricePEfEpst_b3 = VALUES(fairPricePEfEpst_b3),
                fairPricePEfEpsf_b3 = VALUES(fairPricePEfEpsf_b3);
            """, data_tuple)
            cursor.execute("""
            UPDATE fair_price_economicSectors
            SET 
                diffPricePEtEpst_b3 = fairPricePEtEpst - fairPricePEtEpst_b3,
                diffPricePEtEpsf_b3 = fairPricePEtEpsf - fairPricePEtEpsf_b3,
                diffPricePEfEpst_b3 = fairPricePEfEpst - fairPricePEfEpst_b3,
                diffPricePEfEpsf_b3 = fairPricePEfEpsf - fairPricePEfEpsf_b3,
                diffPercPricePEtEpst_b3 = (fairPricePEtEpst - fairPricePEtEpst_b3)/fairPricePEtEpst_b3,
                diffPercPricePEtEpsf_b3 = (fairPricePEtEpsf - fairPricePEtEpsf_b3)/fairPricePEtEpst_b3,
                diffPercPricePEfEpst_b3 = (fairPricePEfEpst - fairPricePEfEpst_b3)/fairPricePEtEpst_b3,
                diffPercPricePEfEpsf_b3 = (fairPricePEfEpsf - fairPricePEfEpsf_b3)/fairPricePEtEpst_b3;
            """)

            
        cursor.execute("SET SQL_SAFE_UPDATES = 1;")

        connection.commit()
        print("Data successfully processed and inserted/updated into 'fair_price_economicSectors'.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if connection:
        connection.close()
    print("Database connection closed.")
