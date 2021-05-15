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

# insert CSV files into DB
def insert_into_db(table_name, index):
    data = extract_from_csv(table_name + '.csv')
    data = data.iloc[:, 1:]

    if index == False:
        data.to_sql(table_name, con = engine, if_exists = 'append', index = False)
    else:
        data.to_sql(table_name, con = engine, if_exists = 'append', index_label = table_name + '_id')

tables_to_insert = {
    'country': False,
    'city': False,
    'airport': False,
    'airline': False,
    'flight': True
}

for key, value in tables_to_insert.items():
    insert_into_db(key, value)
