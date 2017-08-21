import requests
get_response = requests.get(url='https://poloniex.com/public?command=returnTicker')
print(len(list(get_response)))