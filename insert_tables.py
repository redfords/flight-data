import pandas as pd
from sqlalchemy import create_engine

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost:33060/{db}"
                       .format(user = "root", pw = "password", db = "test"))

# create data frame from csv
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)

    return dataframe

print("Uploading into database...")

# insert country data into MySQL
data = extract_from_csv('country.csv')
data = data.iloc[:, 1:]
data.to_sql('country', con = engine, if_exists = 'append', index = False, chunksize = 100)

# insert city data into MySQL
data = extract_from_csv('city.csv')
data = data.iloc[:, 1:]
data.to_sql('city', con = engine, if_exists = 'append', index = False, chunksize = 100)

# insert airport data into MySQL
data = extract_from_csv('airport.csv')
data = data.iloc[:, 1:]
data.to_sql('airport', con = engine, if_exists = 'append', index = False, chunksize = 100)

# insert airline data into MySQL
data = extract_from_csv('airline.csv')
data = data.iloc[:, 1:]
data.to_sql('airline', con = engine, if_exists = 'append', index = False, chunksize = 100)

# insert flight data into MySQL
data = extract_from_csv('flight.csv')
data = data.iloc[:, 1:]
data.to_sql('flight', con = engine, if_exists = 'append', index_label = 'flight_id', chunksize = 100)
