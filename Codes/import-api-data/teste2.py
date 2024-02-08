import requests
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# API URL and parameters for fetching balance sheet history
url = "https://brapi.dev/api/quote/PETR4"
params = {
    'modules': 'incomeStatementHistory',
    'token': 'fUNoicPSZTzstUdqw1uVeB',
}

# Send a GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(data)  # Add this line to print the entire response data

    # Access the 'balanceSheetHistory' part of the data
    if 'balanceSheetHistory' in data and 'balanceSheetStatements' in data['balanceSheetHistory']:
        balance_sheet_data = data['balanceSheetHistory']['balanceSheetStatements']
        for statement in balance_sheet_data:
            # Assuming you want to print the endDate for each balance sheet statement
            print("Balance Sheet Statement End Date:", statement['endDate'])
    else:
        logger.info("Balance sheet data not found.")
else:
    logger.error(f"Request failed with status code {response.status_code}")

# Insert additional code here for further processing or storage of fetched data
