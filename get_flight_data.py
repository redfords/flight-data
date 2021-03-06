import requests
import json
import pandas as pd
import numpy as np
import collections.abc

# create new key for nested dictionary
def flatten(d, parent_key = '', sep = '_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten(v, new_key, sep = sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def convert_json(list_of_dict):
    items = []
    for dict in list_of_dict:
        items.append(flatten(dict))

    return items

def convert_datetime(data, column):
    data[column] = ((data[column].astype(str)).str.replace('T', ' ')).str[:-6]
    data[column] = pd.to_datetime(data[column])

# extract real time flights data
access_key = "bf3848f598ed4c4dfd554b391fc444b1"

url = "http://api.aviationstack.com/v1/flights?limit=100&access_key=" + access_key
response = requests.get(url)
data = response.json()['data']
data = convert_json(data)

# create new data frame
flight_data = pd.DataFrame(data)
flight_data = flight_data.iloc[:, 0:33]

# drop dimension columns
to_drop = [
    'departure_airport',
    'departure_timezone',
    'departure_iata',
    'arrival_airport',
    'arrival_timezone',
    'arrival_iata',
    'arrival_baggage',
    'airline_name',
    'airline_iata'
]

for column in to_drop:
    flight_data.drop([column], axis = 1, inplace = True)

# convert RFC3339 (ISO8601) to datetime
to_convert_datetime = [
    'departure_scheduled',
    'departure_estimated',
    'departure_actual',
    'departure_estimated_runway',
    'departure_actual_runway',
    'arrival_scheduled',
    'arrival_estimated',
    'arrival_actual',
    'arrival_estimated_runway',
    'arrival_actual_runway'
]

for column in to_convert_datetime:
    convert_datetime(flight_data, column)

# replace nan values
to_replace_nan = {
    'departure_airport': '-',
    'departure_timezone': '-',
    'departure_terminal': '-',
    'departure_gate': '-',
    'departure_delay': 0,
    'departure_actual': '1000-01-01 00:00:00',
    'departure_estimated_runway': '1000-01-01 00:00:00',
    'departure_actual_runway': '1000-01-01 00:00:00',
    'arrival_airport': '-',
    'arrival_timezone': '-',
    'arrival_terminal': '-',
    'arrival_gate': '-',
    'arrival_baggage': '-',
    'arrival_delay': 0,
    'arrival_actual': '1000-01-01 00:00:00',
    'arrival_estimated_runway': '1000-01-01 00:00:00',
    'arrival_actual_runway': '1000-01-01 00:00:00'
}

flight_data.fillna(value = to_replace_nan, inplace = True)

# load data into csv
flight_data.to_csv('files/flights.csv')