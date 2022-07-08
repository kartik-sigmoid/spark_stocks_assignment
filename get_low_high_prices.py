import requests
from list_of_stocks import list_of_stocks
import pandas as pd


def format_time(date):
    return date[:10]


def get_low_high():
    data = list_of_stocks()

    for i in data['stocks']:
        url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

        querystring = {"ticker_symbol": i, "years": "1", "format": "json"}

        headers = {
            "X-RapidAPI-Key": "a40aecd837msh1116406c3fc9763p1d7e64jsn8be3a321683f",
            "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        data2 = response.json()

        df = pd.DataFrame(data2['historical prices'])
        df['Date'] = df['Date'].apply(format_time)
        df['Company'] = i
        df.head()
        file_name = 'stock_data/' + str(i) + '.csv'
        df.to_csv(file_name)


get_low_high()
