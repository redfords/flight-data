import requests
import json
import pandas as pd

def extract_data(url):
    response = requests.get(url)
    response_data = response.json()['data']
    data = pd.DataFrame(response_data)

    return data

access_key = "510c0e5f6d29bf5e1701266de1280e06"

# airports
url = "http://api.aviationstack.com/v1/airports?limit=100&access_key=" + access_key
airport = extract_data(url)
airport.to_csv('airport.csv')

# airlines
url = "http://api.aviationstack.com/v1/airlines?limit=100&access_key=" + access_key
airline = extract_data(url)
airline.to_csv('airline.csv')

# cities
url = "http://api.aviationstack.com/v1/cities?limit=100&access_key=" + access_key
city = extract_data(url)
city.to_csv('city.csv')

# countries
url = "http://api.aviationstack.com/v1/countries?limit=100&access_key=" + access_key
country = extract_data(url)
country.to_csv('country.csv')