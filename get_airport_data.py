import requests
import json
import pandas as pd
import numpy as np

access_key = "?limit=100&access_key=510c0e5f6d29bf5e1701266de1280e06"

url = "http://api.aviationstack.com/v1/"

# get .json from request
def extract_data(url):
    response = requests.get(url)
    response_data = response.json()['data']
    data = pd.DataFrame(response_data)

    return data

# convert .json into .csv
def convert_csv(url_name, file_name):
    data = extract_data(url + url_name + access_key)
    data.to_csv('files/' + file_name + '.csv')

# list of files to extract
file_name = {
    'airports': 'airport',
    'airlines': 'airline',
    'cities': 'city',
    'countries': 'country'
}

for key, value in file_name.items():
    convert_csv(key, value)