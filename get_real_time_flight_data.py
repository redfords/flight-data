import requests
import json
import pandas as pd
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

# extract real time flights data
access_key = "510c0e5f6d29bf5e1701266de1280e06"

url = "http://api.aviationstack.com/v1/flights?limit=100&access_key=" + access_key
response = requests.get(url)
data = response.json()['data']
data = convert_json(data)

# create new data frame
real_time_flights = pd.DataFrame(data)
real_time_flights = real_time_flights.iloc[:, 0:33]

# convert RFC3339 (ISO8601) to datetime
real_time_flights['departure_scheduled'] = pd.to_datetime(
    real_time_flights['departure_scheduled']).dt.tz_convert(None)

real_time_flights['departure_estimated'] = pd.to_datetime(
    real_time_flights['departure_estimated']).dt.tz_convert(None)

real_time_flights['departure_actual'] = pd.to_datetime(
    real_time_flights['departure_actual']).dt.tz_convert(None)

real_time_flights['departure_estimated_runway'] = pd.to_datetime(
    real_time_flights['departure_estimated_runway']).dt.tz_convert(None)

real_time_flights['departure_actual_runway'] = pd.to_datetime(
    real_time_flights['departure_actual_runway']).dt.tz_convert(None)

real_time_flights['arrival_scheduled'] = pd.to_datetime(
    real_time_flights['arrival_scheduled']).dt.tz_convert(None)

real_time_flights['arrival_estimated'] = pd.to_datetime(
    real_time_flights['arrival_estimated']).dt.tz_convert(None)

real_time_flights['arrival_actual'] = pd.to_datetime(
    real_time_flights['arrival_actual']).dt.tz_convert(None)

real_time_flights['arrival_estimated_runway'] = pd.to_datetime(
    real_time_flights['arrival_estimated_runway']).dt.tz_convert(None)

real_time_flights['arrival_actual_runway'] = pd.to_datetime(
    real_time_flights['arrival_actual_runway']).dt.tz_convert(None)

# load data into csv
real_time_flights.to_csv('flight.csv')