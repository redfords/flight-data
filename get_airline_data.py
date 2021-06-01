import requests
import json
import pandas as pd
import numpy as np

access_key = "?limit=100&access_key=bf3848f598ed4c4dfd554b391fc444b1"

url = "http://api.aviationstack.com/v1/"

# get .json from request
def extract_data(url):
    response = requests.get(url)
    response_data = response.json()['data']
    data = pd.DataFrame(response_data)
    data.drop(['iata_prefix_accounting', 'country_name'], axis = 1, inplace = True)

    return data

# convert .json into .csv
def convert_csv(url_name, file_name):
    data = extract_data(url + url_name + access_key)

    # replace nan values
    to_replace_nan = {
        'fleet_average_age': 0,
        'callsign': '-',
        'hub_code': '-',
        'date_founded': '1901',
        'fleet_size': 0,
        'status': '-',
        'type': '-'
    }

    data.fillna(value = to_replace_nan, inplace = True)

    data.to_csv('files/' + file_name + '.csv')

convert_csv('airlines', 'airline')