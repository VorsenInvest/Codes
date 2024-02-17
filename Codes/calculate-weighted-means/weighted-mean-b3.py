import pymysql
from dotenv import load_dotenv
import os

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
        cursor.execute("DELETE FROM weighted_b3;")
        cursor.execute("ALTER TABLE weighted_b3 AUTO_INCREMENT = 1;")
        cursor.execute("SET SQL_SAFE_UPDATES = 1;")
        
        # List of columns for which weighted means need to be calculated
        weighted_columns = [
            "regularMarketChange", "regularMarketChangePercent", "regularMarketPrice", "regularMarketDayHigh",
            "regularMarketDayLow", "regularMarketOpen", "fiftyTwoWeekLow", "fiftyTwoWeekHigh",
            "priceEarnings", "earningsPerShare", "forwardPE", "heldPercentInsiders", "heldPercentInstitutions", "beta",
            "bookValue", "priceToBook", "earningsQuarterlyGrowth", "trailingEps", "forwardEps",
            "pegRatio", "enterpriseToRevenue", "enterpriseToEbitda", "_52WeekChange", "lastDividendValue",
            "targetHighPrice", "targetLowPrice", "targetMeanPrice", "targetMedianPrice", "recommendationMean",
            "totalCashPerShare", "quickRatio", "currentRatio", "debtToEquity", "revenuePerShare", "returnOnAssets", 
            "returnOnEquity", "earningsGrowth", "revenueGrowth", "grossMargins", "ebitdaMargins", "operatingMargins", 
            "profitMargins"
        ]

        # Columns for which sums need to be calculated
        sum_columns = [
            "marketCap", "regularMarketVolume", "averageDailyVolume3Month", "averageDailyVolume10Day", "enterpriseValue",
            "floatShares", "sharesOutstanding", "impliedSharesOutstanding", "totalCash", "ebitda", "totalDebt", 
            "totalRevenue", "grossProfits", "freeCashflow", "operatingCashflow","netIncomeToCommon"
        ]

        # Construct the SQL query string for weighted means and sums, with casting to DECIMAL
        select_clauses = [
            f"SUM(CAST(marketCap AS DECIMAL(65, 2)) * CAST({col} AS DECIMAL(65, 2))) / SUM(CAST(marketCap AS DECIMAL(65, 2))) AS weighted_mean_{col}" 
            for col in weighted_columns
        ] + [
            f"SUM({col}) AS sum_{col}" for col in sum_columns
        ]

        query = """
        SELECT 
            """ + ", ".join(select_clauses) + """
        FROM 
            stock_indicators;
        """

        cursor.execute(query)
        results = cursor.fetchone()  # Since we're not grouping, fetch one row of results

        # Find the most common 'regularMarketTime'
        cursor.execute("""
            SELECT regularMarketTime
            FROM stock_indicators
            GROUP BY regularMarketTime
            ORDER BY COUNT(*) DESC
            LIMIT 1;
        """)
        most_common_time_result = cursor.fetchone()
        most_common_time = most_common_time_result['regularMarketTime'] if most_common_time_result else 'Unknown'

        # Prepare the insert query including 'regularMarketTime' and setting 'Bovespa' as the key
        insert_columns = ["`key`"] + [f"weighted_mean_{col}" for col in weighted_columns] + [f"sum_{col}" for col in sum_columns] + ['weighted_mean_regularMarketDayRange', 'weighted_mean_fiftyTwoWeekRange', 'regularMarketTime']
        placeholders = ["%s"] * len(insert_columns)
        update_clauses = [f"{col} = VALUES({col})" for col in insert_columns[1:]]  # Exclude `key` for update
        update_clauses += [f"regularMarketTime = VALUES(regularMarketTime)"]

        insert_query = f"""
        INSERT INTO weighted_b3 (`key`, {", ".join(insert_columns[1:])})
        VALUES ('Bovespa', {", ".join(placeholders[1:])})
        ON DUPLICATE KEY UPDATE
        {", ".join(update_clauses)};
        """


        # Execute the insertion/update
        weighted_mean_regularMarketDayRange = "{:.2f} - {:.2f}".format(
            results.get('weighted_mean_regularMarketDayLow', 0), 
            results.get('weighted_mean_regularMarketDayHigh', 0)
        )
        weighted_mean_fiftyTwoWeekRange = "{:.2f} - {:.2f}".format(
            results.get('weighted_mean_fiftyTwoWeekLow', 0),
            results.get('weighted_mean_fiftyTwoWeekHigh', 0)
        )
            
        # Insert data including the most common 'regularMarketTime'
        data_tuple = (
            [results.get(f"weighted_mean_{col}") for col in weighted_columns] + 
            [results.get(f"sum_{col}") for col in sum_columns] + 
            [weighted_mean_regularMarketDayRange, weighted_mean_fiftyTwoWeekRange, most_common_time]
        )
                    
        cursor.execute(insert_query, data_tuple)
            
        connection.commit()
        print("Data successfully processed and inserted/updated into 'weighted_b3'.")


except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if connection:
        connection.close()
    print("Database connection closed.")
