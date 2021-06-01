# Flight Data Analysis

Real-time flight data from Aviationstack API. Using .json and .csv files, a single source is loaded into the MySQL database.

## Modules

- Requests
- Pandas
- Numpy
- SQLAlchemy
- Pytest
- Apache Airflow

## Data Model

Dimension tables:
`country`
`city`
`airport`
`airline`
`airplane`
`date`

Fact tables:
`flights`

![Model](https://i.imgur.com/gK35KGi.jpg)

## ETL Flow

General overview:

- Flight data is extracted from Aviationstack API and stored in a .csv file.
- Airport, airline and airplane date is extracted and stored in .csv files.
- All files are transformed then loaded into the database.
- The data analysis queries are executed and saved into a single .xlsx file.

DAG graph view:

![Dag](https://i.imgur.com/2CLNNAE.jpg)
