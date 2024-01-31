import json
import requests
import logging
import pymysql
import os


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
print(price_earnings_list)