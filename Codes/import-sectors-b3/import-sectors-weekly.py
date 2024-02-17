import requests
import zipfile
import io
import pandas as pd
import pymysql
import logging
from dotenv import load_dotenv
import os
import psycopg2

# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection details
host = DB_HOST
user = DB_USER
password = DB_PASSWORD
db_name = DB_NAME

# Function to check for non-blank, four-character words
def is_four_characters(word):
    return isinstance(word, str) and word.strip() and len(word.strip()) == 4

# Set pandas options to display all rows (use with caution for very large data sets)
pd.set_option('display.max_rows', None)

# Send a GET request to the URL
url = 'https://www.b3.com.br/data/files/57/E6/AA/A1/68C7781064456178AC094EA8/ClassifSetorial.zip'
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Use io.BytesIO to treat the downloaded data as a file-like object for zipfile
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the Excel file
        excel_names = [name for name in thezip.namelist() if name.endswith('.xlsx') or name.endswith('.xls')]

        if excel_names:
            # Assuming there's only one Excel file in the zip, use its name
            excel_file_name = excel_names[0]

            # Read the Excel file directly into pandas without using the first row as headers
            with thezip.open(excel_file_name) as excel_file:
                df = pd.read_excel(excel_file, header=None)

            # Assign column names based on their position
            df.columns = ['economicSector', 'subsector', 'segment', 'ticker','E']

            # Corrected logic for 'economicSector', following the pattern of 'subsector' but using the correct comparison
            condition_economic_sector = df['economicSector'].notnull() & df['subsector'].notnull()
            df['EconomicSector_Update'] = df.apply(lambda row: row['economicSector'] if condition_economic_sector[row.name] else None, axis=1)
            df['EconomicSector_Update'] = df['EconomicSector_Update'].ffill()
            # Assuming 'ticker' was a mistake and meant to refer to 'D' based on previous context
            df['economicSector'] = df.apply(lambda row: row['EconomicSector_Update'] if is_four_characters(row['ticker']) else row['economicSector'], axis=1)

            # Logic for 'subsector' remains as it is, which you've confirmed to be correct:
            condition_subsector = df['subsector'].notnull() & df['segment'].notnull()
            df['Subsector_Update'] = df.apply(lambda row: row['subsector'] if condition_subsector[row.name] else None, axis=1)
            df['Subsector_Update'] = df['Subsector_Update'].ffill()
            df['subsector'] = df.apply(lambda row: row['Subsector_Update'] if is_four_characters(row['ticker']) else row['subsector'], axis=1)


            # Drop the temporary update columns
            df.drop(columns=['EconomicSector_Update', 'Subsector_Update'], inplace=True)

            # Forward fill the 'Segment' column based on 'C' and 'D'
            df['segment'] = df.apply(lambda row: row['segment'] if not is_four_characters(row['ticker']) else None, axis=1)
            df['segment'] = df['segment'].ffill()

            # Filter the DataFrame to only include rows where 'D' has a 4-character word
            df_filtered = df[df['ticker'].apply(is_four_characters)]

            # Reorder the DataFrame columns
            df_final = df_filtered[['ticker', 'economicSector', 'subsector', 'segment']]

            # Print the final DataFrame
            print(df_final)  # This will print all rows of the final DataFrame
        else:
            print("No Excel file found in the ZIP archive.")
else:
    print("Failed to download the file")
    
# Initialize connection to None
connection = None

try:
    connection = pymysql.connect(host=host, user=user, password=password, db=db_name, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    logger.info("Connected to the database successfully")

    with connection.cursor() as cursor:
        cursor.execute("SET SQL_SAFE_UPDATES = 0;")
        cursor.execute("DELETE FROM stock_sectors;")
        # Reset auto-increment for MySQL/MariaDB
        cursor.execute("ALTER TABLE stock_sectors AUTO_INCREMENT = 1;")
        cursor.execute("SET SQL_SAFE_UPDATES = 1;")
        # Prepare the SQL query to insert data into 'stock_sectors'
        sql_query = """
        INSERT INTO stock_sectors (ticker, economicSector, subsector, segment) VALUES (%s, %s, %s, %s)
        """
        
        # Iterate through DataFrame rows and insert data
        for index, row in df_final.iterrows():
            cursor.execute(sql_query, (row['ticker'], row['economicSector'], row['subsector'], row['segment']))

        connection.commit()
        logger.info("Data inserted successfully")

except Exception as e:
    logger.error(f"Error while connecting to or interacting with the database: {e}")
finally:
    if connection:
        connection.close()
        logger.info("Database connection closed")