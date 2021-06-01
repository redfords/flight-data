import pytest
import requests
import get_flight_data

access_key = "bf3848f598ed4c4dfd554b391fc444b1"

url = "http://api.aviationstack.com/v1/flights?limit=100&access_key=" + access_key

def test_get_flight_data_status_code():
    response = requests.get(url)
    assert response.status_code == 200

def test_get_flight_data_content_type():
    response = requests.get(url)
    assert response.headers["Content-Type"] == "application/json"

def test_get_flight_data_check_attributes():
    response = requests.get(url)
    attributes = list(response.json()['data'][0].keys())

    keys = [
        'flight_date',
        'flight_status',
        'departure',
        'arrival',
        'airline',
        'flight',
        'aircraft',
        'live']

    assert attributes == keys

def test_get_flight_data_flatten():
    flight_data = {
        'flight_no': 1234,
        'departure': {'airport': 'airport name', 'icao': 'ABC'},
        'status': 'active'
    }
    
    flight_data_flatten = {
        'flight_no': 1234,
        'departure_airport': 'airport name',
        'departure_icao': 'ABC',
        'status': 'active'
    }

    flight_data_test = get_flight_data.flatten(flight_data)
    assert flight_data_test == flight_data_flatten