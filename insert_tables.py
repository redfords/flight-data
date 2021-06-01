import pandas as pd
from sqlalchemy import create_engine

# connect to database
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(
        user = "root",
        pw = "password",
        host = "localhost:33060",
        db = "test"
    )
)

# create data frame from .csv
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)

    return dataframe

# insert .csv files into db
def insert_into_db(table_name):
    data = extract_from_csv('files/' + table_name + '.csv')
    data = data.iloc[:, 1:]
    data.to_sql(table_name, con = engine, if_exists = 'append', index = False)

tables_to_insert = ['country', 'city', 'airport', 'airline', 'airplane', 'flights']

for table in tables_to_insert:
    insert_into_db(table)