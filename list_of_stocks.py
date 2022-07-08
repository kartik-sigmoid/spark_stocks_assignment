import requests


def list_of_stocks():
    url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

    headers = {
        "X-RapidAPI-Key": "a40aecd837msh1116406c3fc9763p1d7e64jsn8be3a321683f",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)

    data = response.json()
    # pprint(data['stocks'][:100])
    return data


list_of_stocks()
