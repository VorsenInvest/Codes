import requests
url = "https://brapi.dev/api/quote/VALE3?modules=financialData&token=fUNoicPSZTzstUdqw1uVeB"
response = requests.get(url)
results = response.json()
ev_list_q4, ev_list_q3, ev_list_q2, ev_list_q1, ebit_last_12_list= [] , [],[],[],[]
ebit_last_12_list.append(results.get('results')[0].get('financialData', {}).get('profitMargins'))
print(ebit_last_12_list)
