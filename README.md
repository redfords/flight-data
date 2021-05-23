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

Fact tables:
`flight`
