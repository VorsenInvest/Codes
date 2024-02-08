import requests


url = "https://brapi.dev/api/quote/ITUB4"
params = {
    'range': '1mo',
    'interval': '1mo',
    'fundamental': 'true',
    'dividends': 'true',
    'modules': 'balanceSheetHistory',
    'token': 'fUNoicPSZTzstUdqw1uVeB',
}
 
response = requests.get(url, params=params)
 
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Request failed with status code {response.status_code}")
 