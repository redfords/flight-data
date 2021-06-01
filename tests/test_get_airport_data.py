import pytest
import requests
import get_airport_data

access_key = "access_key=bf3848f598ed4c4dfd554b391fc444b1"

url = "http://api.aviationstack.com/v1/airports?limit=100&" + access_key

def test_get_airport_data_status_code():
    response = requests.get(url)
    assert response.status_code == 200

def test_get_airport_data_content_type():
    response = requests.get(url)
    assert response.headers["Content-Type"] == "application/json"

def test_get_airport_data_check_attributes():
    response = requests.get(url)
    attributes = list(response.json()['data'][0].keys())

    keys = [
        'gmt',
        'iata_code',
        'city_iata_code',
        'icao_code',
        'country_iso2',
        'geoname_id',
        'latitude',
        'longitude',
        'airport_name',
        'country_name',
        'phone_number',
        'timezone'
    ]

    assert attributes == keys