import requests

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
    # Extract regularMarketPrice
    regular_market_price = data['results'][0]['regularMarketPrice']
    print("Regular Market Price for ^BVSP:", regular_market_price)
else:
    print(f"Request failed with status code {response.status_code}")
