from flask import Flask
import json
import requests
import logging
import pymysql
import os

app = Flask(__name__)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to fetch and update data
def fetch_and_update_data():
    
    # API URL
    url = "https://brapi.dev/api/quote/list?token=fUNoicPSZTzstUdqw1uVeB"

    # Send a GET request to the API
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
    else:
        logger.error("Failed to retrieve data: %s", response.status_code)
        # Depending on your use case, you might want to exit or handle this differently
        raise Exception("Failed to retrieve data from the API")

    # Access the 'stocks' part of the data
    stocks_data = data['stocks']

    # Filter to keep only components where 'type' is 'stock' and not ending with 'F'
    filtered_stocks = [item for item in stocks_data if item.get('type') == 'stock' and not item.get('stock', '').endswith('F')]

    # Initialize empty lists
    symbol_list, price_earnings_list, trailing_eps_list, forward_eps_list, price_to_book_list, book_value_list = [], [], [], [], [], []

    # Iterate over unique tickers
    for stock in filtered_stocks:
        ticker = stock.get('stock')
        url = f"https://brapi.dev/api/quote/{ticker}?modules=defaultKeyStatistics,summaryProfile&token=fUNoicPSZTzstUdqw1uVeB"
        response = requests.get(url)
        
        if response.status_code == 200:
            results = response.json()
            
            # Extract and append the data to respective lists
            symbol_list.append(results.get('results')[0].get('symbol'))
            price_earnings_list.append(results.get('results')[0].get('priceEarnings'))
            trailing_eps_list.append(results.get('results')[0].get('defaultKeyStatistics', {}).get('trailingEps'))
            forward_eps_list.append(results.get('results')[0].get('defaultKeyStatistics', {}).get('forwardEps'))
            price_to_book_list.append(results.get('results')[0].get('defaultKeyStatistics', {}).get('priceToBook'))
            book_value_list.append(results.get('results')[0].get('defaultKeyStatistics', {}).get('bookValue'))
        else:
            logger.error("Failed to retrieve data for ticker: %s, Status code: %s", ticker, response.status_code)
            # Handle or log the failure as per your requirement

    # Database connection details
    host = 'brapi-api-db.cx0ko2c0yzso.us-east-2.rds.amazonaws.com'
    user = 'admin'
    password = 'G78u75s61T91!'
    db_name = 'brapi_API_DB'

    # Initialize connection to None
    connection = None

    try:
        connection = pymysql.connect(host=host, user=user, password=password, db=db_name, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        logger.info("Connected to the database successfully")

        with connection.cursor() as cursor:
            cursor.execute("SET SQL_SAFE_UPDATES = 0;")
            cursor.execute("DELETE FROM brapi_API_data_daily;")
            cursor.execute("SET SQL_SAFE_UPDATES = 1;")

            for i in range(len(symbol_list)):
                sql_query = "INSERT INTO brapi_API_data_daily (`Ação`, `P/L`, `LPA trailing`, `LPA forward`, `P/VPA`, `VPA`) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_query, (symbol_list[i], price_earnings_list[i], trailing_eps_list[i], forward_eps_list[i], price_to_book_list[i], book_value_list[i]))
            
            connection.commit()
            logger.info("Data inserted successfully")

    except Exception as e:
        logger.error("Error connecting to or interacting with the database: %s", e)
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed")

# Create a route to trigger the function
@app.route('/update_stock_data', methods=['GET'])
def update_stock_data():
    try:
        fetch_and_update_data()
        return {"status": "success", "message": "Data updated successfully"}, 200
    except Exception as e:
        logger.error("Error occurred: %s", e)
        return {"status": "error", "message": str(e)}, 500

if __name__ == '__main__':
    app.run(debug=True)