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

    return data

def convert_datetime(data, column):
    data[column] = ((data[column].astype(str)).str.replace('T', ' ')).str[:-6]

    # replace invalid datetime
    data[column] = (data[column].astype(str)).str.replace('-0001', '0001 ')

    data[column] = pd.to_datetime(data[column])

# convert .json into .csv
def convert_csv(url_name, file_name):
    data = extract_data(url + url_name + access_key)
    data = data.iloc[:, 0:10]
    data.drop(['airline_iata_code', 'airline_icao_code'], axis = 1, inplace = True)
    
    # convert RFC3339 (ISO8601) to datetime
    to_convert_datetime = [
        'delivery_date',
        'first_flight_date'
    ]

    for column in to_convert_datetime:
        convert_datetime(data, column)

    # replace nan values
    to_replace_nan = {
        'iata_type': '-',
        'iata_code_short': '-',
        'construction_number': 'Unknown',
        'delivery_date': '1000-01-01 00:00:00',
        'engines_count': 0,
        'engines_type': '-',
        'first_flight_date': '1000-01-01 00:00:00',
        'icao_code_hex': '-'
    }

    data.fillna(value = to_replace_nan, inplace = True)

    data.to_csv('files/' + file_name + '.csv')

convert_csv('airplanes', 'airplane')