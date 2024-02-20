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

# Function to calculate geometric mean
def geometric_mean(values):
    # Filter out None values and convert Decimal to float
    valid_values = [float(v) for v in values if v is not None]
    if not valid_values:
        return None  # Or handle the empty list case appropriately
    product = math.prod(valid_values)
    return product ** (1.0 / len(valid_values))


# Function to calculate fair price
def calc_fair_price(pe, eps, bv, pb):
    if all(v is not None and v >= 0 for v in [pe, eps, bv, pb]):
        return math.sqrt(pe * eps * bv * pb)
    else:
        return None
    
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
        cursor.execute("DELETE FROM fair_price_stocks;")
        
        cursor.execute("ALTER TABLE fair_price_stocks AUTO_INCREMENT = 1;")

        # Fetch values from fair_price_b3
        cursor.execute("""
            SELECT marketCap_b3, fairPricePEtEpst_b3, fairPricePEtEpsf_b3, fairPricePEfEpst_b3, fairPricePEfEpsf_b3
            FROM fair_price_b3
            WHERE id = 1; # Adjust if necessary
        """)
        b3_values = cursor.fetchone()
        # Select the desired columns from stock_indicators
        cursor.execute("""
            SELECT `key`,
                marketCap AS marketCap, 
                priceEarnings AS priceEarnings,
                forwardPE AS forwardPE,
                trailingEps AS trailingEps,
                forwardEps AS forwardEps,
                bookValue AS bookValue,
                priceToBook AS priceToBook
            FROM stock_indicators;
        """)
        results = cursor.fetchall()

        # Insert the selected data into fair_price_stocks
        for row in results:

            fairPricePEtEpst_initial = calc_fair_price(row['priceEarnings'], row['trailingEps'], row['bookValue'], row['priceToBook'])
            fairPricePEtEpsf_initial = calc_fair_price(row['priceEarnings'], row['forwardEps'], row['bookValue'], row['priceToBook'])
            fairPricePEfEpst_initial = calc_fair_price(row['forwardPE'], row['trailingEps'], row['bookValue'], row['priceToBook'])
            fairPricePEfEpsf_initial = calc_fair_price(row['forwardPE'], row['forwardEps'], row['bookValue'], row['priceToBook'])

            data_tuple = (
                row['key'],
                row['marketCap'],
                row['priceEarnings'],
                row['forwardPE'],
                row['trailingEps'],
                row['forwardEps'],
                row['bookValue'],
                row['priceToBook'],
                b3_values['marketCap_b3'],
                b3_values['fairPricePEtEpst_b3'],
                b3_values['fairPricePEtEpsf_b3'],
                b3_values['fairPricePEfEpst_b3'],
                b3_values['fairPricePEfEpsf_b3']
            )
            cursor.execute("""
                INSERT INTO fair_price_stocks (`key`, marketCap, priceEarnings, forwardPE, trailingEps, forwardEps, bookValue, priceToBook,
                            marketCap_b3, fairPricePEtEpst_b3, fairPricePEtEpsf_b3, fairPricePEfEpst_b3, fairPricePEfEpsf_b3)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                marketCap = VALUES(marketCap),
                priceEarnings = VALUES(priceEarnings),
                forwardPE = VALUES(forwardPE),
                trailingEps = VALUES(trailingEps),
                forwardEps = VALUES(forwardEps),
                bookValue = VALUES(bookValue),
                priceToBook = VALUES(priceToBook),
                marketCap_b3 = VALUES(marketCap_b3),
                fairPricePEtEpst_b3 = VALUES(fairPricePEtEpst_b3),
                fairPricePEtEpsf_b3 = VALUES(fairPricePEtEpsf_b3),
                fairPricePEfEpst_b3 = VALUES(fairPricePEfEpst_b3),
                fairPricePEfEpsf_b3 = VALUES(fairPricePEfEpsf_b3);
            """, data_tuple)

            # Retrieve the economicSector for the current 'key'
            cursor.execute("""
                SELECT economicSector FROM stock_indicators WHERE `key` = %s;
            """, (row['key'],))
            economicSector_row = cursor.fetchone()
            if economicSector_row:
                economicSector = economicSector_row['economicSector']
                
                # Retrieve fair price values from fair_price_economicSectors based on economicSector
                cursor.execute("""
                    SELECT marketCap, fairPricePEtEpst, fairPricePEtEpsf, fairPricePEfEpst, fairPricePEfEpsf
                    FROM fair_price_economicSectors
                    WHERE `key` = %s;
                """, (economicSector,))
                es_values = cursor.fetchone()
                if es_values:
                    # Update fair_price_stocks with economic sector fair prices
                    cursor.execute("""
                        UPDATE fair_price_stocks
                        SET 
                            marketCap_es = %s,
                            fairPricePEtEpst_es = %s,
                            fairPricePEtEpsf_es = %s,
                            fairPricePEfEpst_es = %s,
                            fairPricePEfEpsf_es = %s
                        WHERE `key` = %s;
                    """, (
                        es_values['marketCap'],
                        es_values['fairPricePEtEpst'],
                        es_values['fairPricePEtEpsf'],
                        es_values['fairPricePEfEpst'],
                        es_values['fairPricePEfEpsf'],
                        row['key']
                    ))
            # Retrieve the subsector for the current 'key'
            cursor.execute("""
                SELECT subsector FROM stock_indicators WHERE `key` = %s;
            """, (row['key'],))
            subsector_row = cursor.fetchone()
            if subsector_row:
                subsector = subsector_row['subsector']
                
                # Retrieve fair price values from fair_price_economicSectors based on economicSector
                cursor.execute("""
                    SELECT marketCap, fairPricePEtEpst, fairPricePEtEpsf, fairPricePEfEpst, fairPricePEfEpsf
                    FROM fair_price_subsectors
                    WHERE `key` = %s;
                """, (subsector,))
                ss_values = cursor.fetchone()
                if ss_values:
                    # Update fair_price_stocks with subsector fair prices
                    cursor.execute("""
                        UPDATE fair_price_stocks
                        SET 
                            marketCap_ss = %s,
                            fairPricePEtEpst_ss = %s,
                            fairPricePEtEpsf_ss = %s,
                            fairPricePEfEpst_ss = %s,
                            fairPricePEfEpsf_ss = %s
                        WHERE `key` = %s;
                    """, (
                        ss_values['marketCap'],
                        ss_values['fairPricePEtEpst'],
                        ss_values['fairPricePEtEpsf'],
                        ss_values['fairPricePEfEpst'],
                        ss_values['fairPricePEfEpsf'],
                        row['key']
                    ))
            # Retrieve the subsector for the current 'key'
            cursor.execute("""
                SELECT segment FROM stock_indicators WHERE `key` = %s;
            """, (row['key'],))
            segment_row = cursor.fetchone()
            if segment_row:
                segment = segment_row['segment']
                
                # Retrieve fair price values from fair_price_segment based on segment
                cursor.execute("""
                    SELECT marketCap, fairPricePEtEpst, fairPricePEtEpsf, fairPricePEfEpst, fairPricePEfEpsf
                    FROM fair_price_segments
                    WHERE `key` = %s;
                """, (segment,))
                s_values = cursor.fetchone()
                if s_values:
                    # Update fair_price_stocks with segment fair prices
                    cursor.execute("""
                        UPDATE fair_price_stocks
                        SET 
                            marketCap_s = %s,
                            fairPricePEtEpst_s = %s,
                            fairPricePEtEpsf_s = %s,
                            fairPricePEfEpst_s = %s,
                            fairPricePEfEpsf_s = %s
                        WHERE `key` = %s;
                    """, (
                        s_values['marketCap'],
                        s_values['fairPricePEtEpst'],
                        s_values['fairPricePEtEpsf'],
                        s_values['fairPricePEfEpst'],
                        s_values['fairPricePEfEpsf'],
                        row['key']
                    ))

            fairPricePEtEpst = geometric_mean([
                fairPricePEtEpst_initial, 
                b3_values['fairPricePEtEpst_b3'], 
                es_values['fairPricePEtEpst'], 
                ss_values['fairPricePEtEpst'], 
                s_values['fairPricePEtEpst']
            ])

            fairPricePEtEpsf = geometric_mean([
                fairPricePEtEpsf_initial, 
                b3_values['fairPricePEtEpsf_b3'], 
                es_values['fairPricePEtEpsf'], 
                ss_values['fairPricePEtEpsf'], 
                s_values['fairPricePEtEpsf']
            ])

            fairPricePEfEpst = geometric_mean([
                fairPricePEfEpst_initial, 
                b3_values['fairPricePEfEpst_b3'], 
                es_values['fairPricePEfEpst'], 
                ss_values['fairPricePEfEpst'], 
                s_values['fairPricePEfEpst']
            ])

            fairPricePEfEpsf = geometric_mean([
                fairPricePEfEpsf_initial, 
                b3_values['fairPricePEfEpsf_b3'], 
                es_values['fairPricePEfEpsf'], 
                ss_values['fairPricePEfEpsf'], 
                s_values['fairPricePEfEpsf']
            ])

            data_tuple = (
                row['key'],
                fairPricePEtEpst,
                fairPricePEtEpsf,
                fairPricePEfEpst,
                fairPricePEfEpsf
            )
            cursor.execute("""
                INSERT INTO fair_price_stocks (`key`, fairPricePEtEpst, fairPricePEtEpsf, fairPricePEfEpst, fairPricePEfEpsf)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                fairPricePEtEpst = VALUES(fairPricePEtEpst),
                fairPricePEtEpsf = VALUES(fairPricePEtEpsf),
                fairPricePEfEpst = VALUES(fairPricePEfEpst),
                fairPricePEfEpsf = VALUES(fairPricePEfEpsf);
            """, data_tuple)

            cursor.execute("""
            UPDATE fair_price_stocks
            SET 
                diffPricePEtEpst_b3 = fairPricePEtEpst - fairPricePEtEpst_b3,
                diffPricePEtEpsf_b3 = fairPricePEtEpsf - fairPricePEtEpsf_b3,
                diffPricePEfEpst_b3 = fairPricePEfEpst - fairPricePEfEpst_b3,
                diffPricePEfEpsf_b3 = fairPricePEfEpsf - fairPricePEfEpsf_b3,
                diffPercPricePEtEpst_b3 = (fairPricePEtEpst - fairPricePEtEpst_b3)/fairPricePEtEpst,
                diffPercPricePEtEpsf_b3 = (fairPricePEtEpsf - fairPricePEtEpsf_b3)/fairPricePEtEpst,
                diffPercPricePEfEpst_b3 = (fairPricePEfEpst - fairPricePEfEpst_b3)/fairPricePEtEpst,
                diffPercPricePEfEpsf_b3 = (fairPricePEfEpsf - fairPricePEfEpsf_b3)/fairPricePEtEpst,
                           
                diffPricePEtEpst_es = fairPricePEtEpst - fairPricePEtEpst_es,
                diffPricePEtEpsf_es = fairPricePEtEpsf - fairPricePEtEpsf_es,
                diffPricePEfEpst_es = fairPricePEfEpst - fairPricePEfEpst_es,
                diffPricePEfEpsf_es = fairPricePEfEpsf - fairPricePEfEpsf_es,
                diffPercPricePEtEpst_es = (fairPricePEtEpst - fairPricePEtEpst_es) / fairPricePEtEpst,
                diffPercPricePEtEpsf_es = (fairPricePEtEpsf - fairPricePEtEpsf_es) / fairPricePEtEpsf,
                diffPercPricePEfEpst_es = (fairPricePEfEpst - fairPricePEfEpst_es) / fairPricePEfEpst,
                diffPercPricePEfEpsf_es = (fairPricePEfEpsf - fairPricePEfEpsf_es) / fairPricePEfEpsf,
                
                diffPricePEtEpst_ss = fairPricePEtEpst - fairPricePEtEpst_ss,
                diffPricePEtEpsf_ss = fairPricePEtEpsf - fairPricePEtEpsf_ss,
                diffPricePEfEpst_ss = fairPricePEfEpst - fairPricePEfEpst_ss,
                diffPricePEfEpsf_ss = fairPricePEfEpsf - fairPricePEfEpsf_ss,
                diffPercPricePEtEpst_ss = (fairPricePEtEpst - fairPricePEtEpst_ss) / fairPricePEtEpst,
                diffPercPricePEtEpsf_ss = (fairPricePEtEpsf - fairPricePEtEpsf_ss) / fairPricePEtEpsf,
                diffPercPricePEfEpst_ss = (fairPricePEfEpst - fairPricePEfEpst_ss) / fairPricePEfEpst,
                diffPercPricePEfEpsf_ss = (fairPricePEfEpsf - fairPricePEfEpsf_ss) / fairPricePEfEpsf,
        
                diffPricePEtEpst_s = fairPricePEtEpst - fairPricePEtEpst_s,
                diffPricePEtEpsf_s = fairPricePEtEpsf - fairPricePEtEpsf_s,
                diffPricePEfEpst_s = fairPricePEfEpst - fairPricePEfEpst_s,
                diffPricePEfEpsf_s = fairPricePEfEpsf - fairPricePEfEpsf_s,
                diffPercPricePEtEpst_s = (fairPricePEtEpst - fairPricePEtEpst_s) / fairPricePEtEpst,
                diffPercPricePEtEpsf_s = (fairPricePEtEpsf - fairPricePEtEpsf_s) / fairPricePEtEpsf,
                diffPercPricePEfEpst_s = (fairPricePEfEpst - fairPricePEfEpst_s) / fairPricePEfEpst,
                diffPercPricePEfEpsf_s = (fairPricePEfEpsf - fairPricePEfEpsf_s) / fairPricePEfEpsf;


            """)



        cursor.execute("SET SQL_SAFE_UPDATES = 1;")

        connection.commit()
        print("Data successfully processed and inserted/updated into 'fair_price_stocks'.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if connection:
        connection.close()
    print("Database connection closed.")
